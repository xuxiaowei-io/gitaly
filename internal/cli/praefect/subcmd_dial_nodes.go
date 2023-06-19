package praefect

import (
	"context"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
)

func newDialNodesCommand() *cli.Command {
	return &cli.Command{
		Name:  "dial-nodes",
		Usage: "check connection with remote nodes",
		Description: "The subcommand \"dial-nodes\" helps diagnose connection problems to Gitaly or\n" +
			"Praefect. The subcommand works by sourcing the connection information from\n" +
			"the config file, and then dialing and health checking the remote nodes.",
		HideHelpCommand: true,
		Action:          dialNodesAction,
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:  "timeout",
				Usage: "timeout for dialing gitaly nodes",
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
	logger := log.Default()
	conf, err := getConfig(logger, ctx.String(configFlagName))
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
