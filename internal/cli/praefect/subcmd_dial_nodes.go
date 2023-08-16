package praefect

import (
	"context"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
)

func newDialNodesCommand() *cli.Command {
	return &cli.Command{
		Name:  "dial-nodes",
		Usage: "check connections",
		Description: `Check connections with Gitaly nodes.

Diagnoses connection problems with Gitaly or Praefect. Sources connection information from the
configuration file, and then dials and health checks the nodes.

Example: praefect --config praefect.config.toml dial-nodes`,
		HideHelpCommand: true,
		Action:          dialNodesAction,
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:  "timeout",
				Usage: "timeout for dialing Gitaly nodes",
				Value: 0,
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

func dialNodesAction(ctx *cli.Context) error {
	conf, err := readConfig(ctx.String(configFlagName))
	if err != nil {
		return err
	}

	timeout := ctx.Duration("timeout")
	if timeout == 0 {
		timeout = defaultDialTimeout
	}

	timeCtx, cancel := context.WithTimeout(ctx.Context, timeout)
	defer cancel()

	return nodes.PingAll(timeCtx, conf, nodes.NewTextPrinter(ctx.App.Writer), false)
}
