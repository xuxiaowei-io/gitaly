package praefect

import "github.com/urfave/cli/v2"

const configurationCmdName = "configuration"

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:            configurationCmdName,
		Usage:           "manages configuration",
		HideHelpCommand: true,
		Subcommands: []*cli.Command{
			newConfigurationValidateCommand(),
		},
	}
}
