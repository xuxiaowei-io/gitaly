package praefect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/repocleaner"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

func newListUntrackedRepositoriesCommand() *cli.Command {
	return &cli.Command{
		Name:  "list-untracked-repositories",
		Usage: "list untracked repositories",
		Description: `List repositories located on physical storages but not tracked by Praefect.

By default, does not list untracked repositories that are newer than 6 hours.

Returns:

- Details of untracked repositories to stdout, including the repository's relative path, physical storage
  name, and virtual storage name.
- All errors and log messages to stderr. The output is produced as new data appears. The command doesn't wait to
  to complete processing before producing a result.

Examples:

- praefect --config praefect.config.toml list-untracked-repositories
- praefect --config praefect.config.toml list-untracked-repositories --older-than 1s`,
		HideHelpCommand: true,
		Action:          listUntrackedRepositoriesAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "delimiter",
				Value: "\n",
				Usage: "string used as a delimiter in output",
			},
			&cli.DurationFlag{
				Name:  "older-than",
				Value: 6 * time.Hour,
				Usage: "only include repositories created before this duration",
			},
		},
		Before: func(ctx *cli.Context) error {
			if ctx.Args().Present() {
				_ = cli.ShowSubcommandHelp(ctx)
				return cli.Exit(unexpectedPositionalArgsError{Command: ctx.Command.Name}, 1)
			}
			return nil
		},
	}
}

func listUntrackedRepositoriesAction(appCtx *cli.Context) error {
	logger := log.ConfigureCommand()

	conf, err := readConfig(appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	onlyIncludeOlderThan := appCtx.Duration("older-than")
	delimiter := appCtx.String("delimiter")

	ctx := correlation.ContextWithCorrelation(appCtx.Context, correlation.SafeRandomID())
	ctx = metadata.AppendToOutgoingContext(ctx, "client_name", appCtx.Command.Name)

	logger = logger.WithField("correlation_id", correlation.ExtractFromContext(ctx))
	logger.Debugf("starting %s command", appCtx.App.Name)

	logger.Debug("dialing to gitaly nodes...")
	nodeSet, err := dialGitalyStorages(ctx, conf, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial nodes: %w", err)
	}
	defer nodeSet.Close()
	logger.Debug("connected to gitaly nodes")

	logger.Debug("connecting to praefect database...")
	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, conf.DB)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()
	logger.Debug("connected to praefect database")

	walker := repocleaner.NewWalker(nodeSet.Connections(), 16, onlyIncludeOlderThan)
	reporter := reportUntrackedRepositories{
		ctx:         ctx,
		checker:     datastore.NewStorageCleanup(db),
		delimiter:   delimiter,
		out:         appCtx.App.Writer,
		printHeader: true,
	}
	for _, vs := range conf.VirtualStorages {
		for _, node := range vs.Nodes {
			logger.Debugf("check %q/%q storage repositories", vs.Name, node.Storage)
			if err := walker.ExecOnRepositories(ctx, vs.Name, node.Storage, reporter.Report); err != nil {
				return fmt.Errorf("exec on %q/%q: %w", vs.Name, node.Storage, err)
			}
		}
	}
	logger.Debug("completed")
	return nil
}

var errNoConnectionToGitalies = errors.New("no connection established to gitaly nodes")

func dialGitalyStorages(ctx context.Context, cfg config.Config, timeout time.Duration) (praefect.NodeSet, error) {
	nodeSet := praefect.NodeSet{}
	for _, vs := range cfg.VirtualStorages {
		for _, node := range vs.Nodes {
			conn, err := subCmdDial(ctx, node.Address, node.Token, timeout)
			if err != nil {
				return nil, fmt.Errorf("dial with %q gitaly at %q", node.Storage, node.Address)
			}
			if _, found := nodeSet[vs.Name]; !found {
				nodeSet[vs.Name] = map[string]praefect.Node{}
			}
			nodeSet[vs.Name][node.Storage] = praefect.Node{
				Storage:    node.Storage,
				Address:    node.Address,
				Token:      node.Token,
				Connection: conn,
			}
		}
	}
	if len(nodeSet.Connections()) == 0 {
		return nil, errNoConnectionToGitalies
	}
	return nodeSet, nil
}

type reportUntrackedRepositories struct {
	ctx         context.Context
	checker     *datastore.StorageCleanup
	out         io.Writer
	delimiter   string
	printHeader bool
}

// Report method accepts a list of repositories, checks if they exist in the praefect database
// and writes JSON serialized location of each untracked repository using the configured delimiter
// and writer.
func (r *reportUntrackedRepositories) Report(virtualStorage, storage string, replicaPaths []string) error {
	if len(replicaPaths) == 0 {
		return nil
	}

	missing, err := r.checker.DoesntExist(r.ctx, virtualStorage, storage, replicaPaths)
	if err != nil {
		return fmt.Errorf("existence check: %w", err)
	}

	if len(missing) > 0 && r.printHeader {
		if _, err := fmt.Fprintf(r.out, "The following repositories were found on disk, but missing from the tracking database:\n"); err != nil {
			return fmt.Errorf("write header to output: %w", err)
		}
		r.printHeader = false
	}

	for _, replicaPath := range missing {
		d, err := json.Marshal(map[string]string{
			"virtual_storage": virtualStorage,
			"storage":         storage,
			"relative_path":   replicaPath,
		})
		if err != nil {
			return fmt.Errorf("serialize: %w", err)
		}
		if _, err := r.out.Write(d); err != nil {
			return fmt.Errorf("write serialized data to output: %w", err)
		}
		if _, err := r.out.Write([]byte(r.delimiter)); err != nil {
			return fmt.Errorf("write serialized data to output: %w", err)
		}
	}

	return nil
}
