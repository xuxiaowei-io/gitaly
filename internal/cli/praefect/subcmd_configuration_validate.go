package praefect

import (
	"io"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/cmd"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/config"
)

const validationErrorCode = 2

func newConfigurationValidateCommand() *cli.Command {
	return &cli.Command{
		Name:  "validate",
		Usage: "validates configuration",
		Description: "The command accepts configuration in toml format on STDIN. It applies " +
			"validation rules to the received configuration and returns all the found " +
			"validation errors in JSON format back on STDOUT.",
		HideHelpCommand: true,
		Action:          configurationValidateAction,
	}
}

func configurationValidateAction(ctx *cli.Context) error {
	if ctx.Args().Present() {
		_ = cli.ShowSubcommandHelp(ctx)
		return cli.Exit("invalid argument(s)", 1)
	}

	if code := validateConfiguration(ctx.App.Reader, ctx.App.Writer, ctx.App.ErrWriter); code != 0 {
		return cli.Exit("", code)
	}

	return nil
}

// validateConfiguration checks if provided configuration is valid.
func validateConfiguration(reader io.Reader, outWriter, errWriter io.Writer) int {
	cfg, err := config.FromReader(reader)
	if err != nil {
		if cmd.WriteTomlReadError(err, outWriter, errWriter) {
			return validationErrorCode
		}
		return 1
	}

	if !cmd.Validate(&cfg, outWriter, errWriter) {
		return validationErrorCode
	}

	return 0
}
