package bench

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
)

// Coordinator starts and stops Gitaly when directed by the client.
type Coordinator struct {
	Listener net.Listener
	StartCmd []string
	StopCmd  []string
	rw       *jsonRW
}

// RunCoordinator executes the coordinator role of gitaly-bench.
func RunCoordinator(ctx *cli.Context) error {
	l, err := net.Listen("tcp", ":"+ctx.String("coord-port"))
	if err != nil {
		return fmt.Errorf("coordinator: listen: %w", err)
	}
	defer l.Close()

	return newCoordinator(l).run()
}

func newCoordinator(listener net.Listener) *Coordinator {
	// We deliberately leave `rw` uninitialized until the connection is established.
	return &Coordinator{
		Listener: listener,
		StartCmd: []string{"/usr/bin/systemctl", "start", "gitaly"},
		StopCmd:  []string{"/usr/bin/systemctl", "stop", "gitaly"},
		rw:       nil,
	}
}

func (c *Coordinator) run() error {
	for {
		conn, err := c.Listener.Accept()
		if err != nil {
			return fmt.Errorf("coordinator: accept: %w", err)
		}
		defer conn.Close()

		c.rw = newJSONRW(conn)

		done, err := c.session()
		if err != nil {
			return fmt.Errorf("coordinator: session: %w", err)
		}
		if done {
			return nil
		}
	}
}

func (c *Coordinator) session() (bool, error) {
	for {
		cmd, err := c.readCmd()
		if err != nil {
			return false, err
		}

		switch cmd.Action {
		case startGitalyAction:
			if err := c.runBench(cmd); err != nil {
				c.respErr(err)
				return false, err
			}

			if err := c.respDone(); err != nil {
				return false, err
			}
		case exitCoordAction:
			if err := c.respDone(); err != nil {
				return false, err
			}
			return true, nil
		case stopGitalyAction:
			err := errors.New("received 'stop' command when Gitaly was not running")
			c.respErr(err)
			return false, err
		default:
			err := fmt.Errorf("unknown command: %q", cmd.Action)
			c.respErr(err)
			return false, err
		}
	}
}

func (c *Coordinator) runBench(cmd *CoordCmd) error {
	if err := c.startBench(cmd); err != nil {
		_ = c.stopGitaly()
		return fmt.Errorf("start benchmarking: %w", err)
	}

	if err := c.finishBench(); err != nil {
		return fmt.Errorf("finish benchmarking: %w", err)
	}

	return nil
}

func (c *Coordinator) startBench(cmd *CoordCmd) error {
	if err := os.MkdirAll(cmd.OutDir, perm.PrivateDir); err != nil {
		return fmt.Errorf("failed to create output directory %q: %w", cmd.OutDir, err)
	}

	if err := c.startGitaly(); err != nil {
		return err
	}

	// Let Gitaly warm up.
	time.Sleep(5 * time.Second)

	return c.respDone()
}

func (c *Coordinator) finishBench() error {
	cmd, err := c.readCmd()
	if err != nil {
		_ = c.stopGitaly()
		return err
	}

	if cmd.Action != stopGitalyAction {
		_ = c.stopGitaly()
		return errors.New("received command other than 'stop' while Gitaly was running")
	}

	return c.stopGitaly()
}

func (c *Coordinator) readCmd() (*CoordCmd, error) {
	var cmd CoordCmd
	if err := c.rw.decoder.Decode(&cmd); err != nil {
		return nil, fmt.Errorf("read command: %w", err)
	}
	return &cmd, nil
}

func (c *Coordinator) respDone() error {
	resp := CoordResp{}

	if err := c.rw.encoder.Encode(resp); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

func (c *Coordinator) respErr(err error) {
	resp := CoordResp{
		Error: err.Error(),
	}
	// We're already in a bad state and about to exit,
	// ignore additional errors.
	_ = c.rw.encoder.Encode(resp)
}

func (c *Coordinator) startGitaly() error {
	cmd := exec.Command(c.StartCmd[0], c.StartCmd[1:]...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("start gitaly: %w", err)
	}
	return nil
}

func (c *Coordinator) stopGitaly() error {
	cmd := exec.Command(c.StopCmd[0], c.StopCmd[1:]...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("stop gitaly: %w", err)
	}
	return nil
}
