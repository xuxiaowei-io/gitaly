package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
)

// RPCInfo contains the service name and proto file associated with a Gitaly RPC.
type RPCInfo struct {
	Service   string `json:"service"`
	ProtoFile string `json:"proto_file"`
}

// Query contains the details of a RPC request that will be benchmarked.
type Query struct {
	repo      string
	rpc       string
	info      RPCInfo
	queryFile string
}

// Client commands the coordinator to start/stop Gitaly and runs benchmarks.
type Client struct {
	ServerAddr string
	RootOutDir string
	QueryDir   string
	BenchCmd   func(Query, string) []string
	rw         *jsonRW
}

const (
	rpcInfoFile       = "rpc_info.json"
	benchDurationSecs = "30"
	maxBenchDur       = 10 * time.Minute
)

// RunClient executes the client role of gitaly-bench.
func RunClient(ctx *cli.Context) error {
	conn, err := openConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return newClient(ctx.String("server-addr"),
		ctx.String("out-dir"),
		ctx.String("query-dir"),
		conn,
		[]string{"/usr/local/bin/benchmark-gitaly"}).run()
}

func openConn(ctx *cli.Context) (net.Conn, error) {
	coordAddr := fmt.Sprintf("%v:%v", ctx.String("server-addr"), ctx.String("coord-port"))
	conn, err := net.Dial("tcp", coordAddr)
	if err != nil {
		return nil, fmt.Errorf("client: dial coordinator: %w", err)
	}
	return conn, nil
}

func newClient(serverAddr, rootOutDir, queryDir string, conn io.ReadWriter, benchCmd []string) *Client {
	return &Client{
		ServerAddr: serverAddr,
		RootOutDir: rootOutDir,
		QueryDir:   queryDir,
		rw:         newJSONRW(conn),
		BenchCmd: func(q Query, outDir string) []string {
			return []string{
				"/usr/local/bin/benchmark-gitaly",
				"-a", serverAddr,
				"-d", benchDurationSecs,
				"-o", outDir,
				"-p", q.info.ProtoFile,
				"-s", q.info.Service,
				"-r", q.rpc,
				"-g", q.repo,
			}
		},
	}
}

func (c *Client) run() error {
	ctx := context.Background()
	defer func() {
		// Coordinator exits unconditionally.
		_ = c.sendCmd(ctx, CoordCmd{
			Action: exitCoordAction,
		})
	}()

	rpcInfo, err := c.getRPCInfo()
	if err != nil {
		return fmt.Errorf("client: get RPC info: %w", err)
	}

	queries, err := c.findQueries(rpcInfo)
	if err != nil {
		return fmt.Errorf("client: find queries: %w", err)
	}

	return c.runQueries(ctx, queries)
}

func (c *Client) getRPCInfo() (map[string]RPCInfo, error) {
	infoPath := filepath.Join(c.QueryDir, rpcInfoFile)

	data, err := os.ReadFile(infoPath)
	if err != nil {
		return nil, fmt.Errorf("reading %v: %w", infoPath, err)
	}

	var info map[string]RPCInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("unmarshaling %v: %w", infoPath, err)
	}

	return info, nil
}

func (c *Client) findQueries(rpcInfo map[string]RPCInfo) ([]Query, error) {
	var queries []Query

	if err := filepath.WalkDir(c.QueryDir, func(path string, dirEnt fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}

		if dirEnt.IsDir() {
			return nil
		}

		if filepath.Base(path) == rpcInfoFile {
			return nil
		}

		// Ignore any unexpected files.
		if filepath.Ext(path) != ".json" {
			return nil
		}

		// Query files are stored as `<QUERY_DIR>/<RPC_NAME>/<REPO_NAME>.json`.
		rpc := filepath.Base(filepath.Dir(path))
		repo := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))

		info, ok := rpcInfo[rpc]
		if !ok {
			return fmt.Errorf("unknown RPC %q", rpc)
		}

		queries = append(queries, Query{
			repo:      repo,
			rpc:       rpc,
			info:      info,
			queryFile: filepath.Join(c.QueryDir, path),
		})

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking query directory: %w", err)
	}

	return queries, nil
}

func (c *Client) runQueries(ctx context.Context, queries []Query) error {
	for _, query := range queries {
		if err := c.runQuery(ctx, query); err != nil {
			return fmt.Errorf("client: run query: %w", err)
		}
	}

	return nil
}

func (c *Client) runQuery(ctx context.Context, query Query) error {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(maxBenchDur))
	defer cancel()

	outDir := filepath.Join(c.RootOutDir, query.rpc, query.repo)
	if err := c.sendCmd(ctx, CoordCmd{
		Action: startGitalyAction,
		Repo:   query.repo,
		RPC:    query.rpc,
		OutDir: outDir,
	}); err != nil {
		return fmt.Errorf("start gitaly: %w", err)
	}

	if err := c.runBench(ctx, query, outDir); err != nil {
		return fmt.Errorf("run bench: %w", err)
	}

	if err := c.sendCmd(ctx, CoordCmd{
		Action: stopGitalyAction,
		OutDir: outDir,
	}); err != nil {
		return fmt.Errorf("stop gitaly: %w", err)
	}

	return nil
}

func (c *Client) runBench(ctx context.Context, query Query, outDir string) error {
	if err := os.MkdirAll(outDir, perm.PrivateDir); err != nil {
		return fmt.Errorf("failed to create output directory %q: %w", outDir, err)
	}

	args := c.BenchCmd(query, outDir)
	cmd := exec.Command(args[0], args[1:]...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run ghz: %w", err)
	}

	return nil
}

func (c *Client) sendCmd(ctx context.Context, cmd CoordCmd) error {
	// This should be more than long enough for gitaly to stop/start.
	_, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Minute))
	defer cancel()

	if err := c.rw.encoder.Encode(&cmd); err != nil {
		return fmt.Errorf("send cmd: %w", err)
	}

	var resp CoordResp
	if err := c.rw.decoder.Decode(&resp); err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("cmd failed: %v", resp.Error)
	}

	return nil
}
