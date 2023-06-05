package praefect

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
)

const sqlPingCmdName = "sql-ping"

func newSQLPingCommand() *cli.Command {
	return &cli.Command{
		Name:            sqlPingCmdName,
		Usage:           "checks reachability of the database",
		Description:     "The subcommand checks if the database configured in the configuration file is reachable",
		HideHelpCommand: true,
		Action:          sqlPingAction,
		Before: func(appCtx *cli.Context) error {
			if appCtx.Args().Present() {
				_ = cli.ShowSubcommandHelp(appCtx)
				return unexpectedPositionalArgsError{Command: appCtx.Command.Name}
			}
			return nil
		},
	}
}

func sqlPingAction(appCtx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	subCmd := progname + " " + appCtx.Command.Name

	db, clean, err := openDB(conf.DB, appCtx.App.ErrWriter)
	if err != nil {
		return err
	}
	defer clean()

	if err := datastore.CheckPostgresVersion(db); err != nil {
		return fmt.Errorf("%s: fail: %w", subCmd, err)
	}

	fmt.Fprintf(appCtx.App.Writer, "%s: OK\n", subCmd)
	return nil
}
