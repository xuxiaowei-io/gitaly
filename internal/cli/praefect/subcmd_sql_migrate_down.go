package praefect

import (
	"fmt"
	"strconv"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
)

const sqlMigrateDownCmdName = "sql-migrate-down"

func newSQLMigrateDownCommand() *cli.Command {
	return &cli.Command{
		Name:  sqlMigrateDownCmdName,
		Usage: "apply revert SQL migrations",
		Description: "The sql-migrate-down subcommand applies revert migrations to the configured database.\n" +
			"It accepts a single argument - amount of migrations to revert.",
		HideHelpCommand: true,
		Action:          sqlMigrateDownAction,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "f",
				Usage: "apply down-migrations",
			},
		},
		Before: func(appCtx *cli.Context) error {
			if appCtx.Args().Len() == 0 {
				_ = cli.ShowSubcommandHelp(appCtx)
				return fmt.Errorf("%s requires a single positional argument", appCtx.Command.Name)
			}
			if appCtx.Args().Len() > 1 {
				_ = cli.ShowSubcommandHelp(appCtx)
				return fmt.Errorf("%s accepts only single positional argument", appCtx.Command.Name)
			}
			return nil
		},
	}
}

func sqlMigrateDownAction(appCtx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	maxMigrations, err := strconv.Atoi(appCtx.Args().First())
	if err != nil {
		return err
	}

	if maxMigrations < 1 {
		return fmt.Errorf("number of migrations to roll back must be 1 or more")
	}

	if appCtx.Bool("f") {
		n, err := datastore.MigrateDown(conf, maxMigrations)
		if err != nil {
			return err
		}

		fmt.Fprintf(appCtx.App.Writer, "OK (applied %d \"down\" migrations)\n", n)
		return nil
	}

	planned, err := datastore.MigrateDownPlan(conf, maxMigrations)
	if err != nil {
		return err
	}

	fmt.Fprintf(appCtx.App.Writer, "DRY RUN -- would roll back:\n\n")
	for _, id := range planned {
		fmt.Fprintf(appCtx.App.Writer, "- %s\n", id)
	}
	fmt.Fprintf(appCtx.App.Writer, "\nTo apply these migrations run with -f\n")

	return nil
}
