package praefect

import "github.com/urfave/cli/v2"

const configurationCmdName = "configuration"

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:  configurationCmdName,
		Usage: "manages configuration",
		Subcommands: []*cli.Command{
			newConfigurationValidateCommand(),
		},
	}
}
