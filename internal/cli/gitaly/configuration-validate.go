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
		Name:  "configuration",
		Usage: "Run configuration-related commands",
		Description: "Run commands related to Gitaly configuration.\n\n" +

			"Provides the following subcommand:\n\n" +

			"- validate",
		HideHelpCommand: true,
		Subcommands: []*cli.Command{
			{
				Name:  "validate",
				Usage: "Validate Gitaly configuration",
				Description: "Check that input provided on stdin is valid Gitaly configuration.\n" +
					"Use `validate` before starting Gitaly.\n\n" +

					"Prints all configuration problems to stdout in JSON format.\n" +
					"The output's structure includes:\n\n" +

					"- A key, which is the path to the configuration field where the\n" +
					"  problem is detected.\n" +
					"- A message, with an explanation of the problem.",
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
