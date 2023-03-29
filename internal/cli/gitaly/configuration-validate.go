package gitaly

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/cmd"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

const validationErrorCode = 2

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:  "configuration",
		Usage: "allows to run commands related to the configuration",
		Subcommands: []*cli.Command{
			{
				Name:   "validate",
				Usage:  "checks if provided on STDIN configuration is valid",
				Action: validateConfigurationAction,
			},
		},
	}
}

func validateConfigurationAction(ctx *cli.Context) error {
	logrus.SetLevel(logrus.ErrorLevel)

	cfg, err := config.Load(ctx.App.Reader)
	if err != nil {
		if cmd.WriteTomlReadError(err, ctx.App.Writer, ctx.App.ErrWriter) {
			return cli.Exit("", validationErrorCode)
		}

		return cli.Exit("", 1)
	}

	if !cmd.Validate(&cfg, ctx.App.Writer, ctx.App.ErrWriter) {
		return cli.Exit("", validationErrorCode)
	}

	return nil
}
