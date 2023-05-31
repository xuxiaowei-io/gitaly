package gitaly

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/cmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
)

const validationErrorCode = 2

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:            "configuration",
		Usage:           "run configuration-related commands",
		Description:     "Run commands related to Gitaly configuration.",
		HideHelpCommand: true,
		Subcommands: []*cli.Command{
			{
				Name:        "validate",
				Usage:       "validate configuration",
				Description: "check that configuration provided on stdin is valid.",
				Action:      validateConfigurationAction,
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
