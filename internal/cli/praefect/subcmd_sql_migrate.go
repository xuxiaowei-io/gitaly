package praefect

import (
	"fmt"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/migrations"
)

const (
	sqlMigrateCmdName = "sql-migrate"
	timeFmt           = "2006-01-02T15:04:05"
)

func newSQLMigrateCommand() *cli.Command {
	return &cli.Command{
		Name:  sqlMigrateCmdName,
		Usage: "apply outstanding SQL migrations",
		Description: "The sql-migrate subcommand applies outstanding migrations to the configured database.\n" +
			"The subcommand doesn't fail if database has migrations unknown to the version of Praefect you're using.\n" +
			"To make the subcommand fail on unknown migrations, use the 'ignore-unknown' flag.",
		HideHelpCommand: true,
		Action:          sqlMigrateAction,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "ignore-unknown",
				Usage: "ignore unknown migrations",
				Value: true,
			},
			&cli.BoolFlag{
				Name:  "verbose",
				Usage: "show text of migration query",
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

func sqlMigrateAction(appCtx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	db, clean, err := openDB(conf.DB)
	if err != nil {
		return err
	}
	defer clean()

	ignoreUnknown := appCtx.Bool("ignore-unknown")
	migrationSet := migrate.MigrationSet{
		IgnoreUnknown: ignoreUnknown,
		TableName:     migrations.MigrationTableName,
	}

	planSource := &migrate.MemoryMigrationSource{
		Migrations: migrations.All(),
	}

	// Find all migrations that are currently down.
	planMigrations, _, _ := migrationSet.PlanMigration(db, "postgres", planSource, migrate.Up, 0)

	subCmd := progname + " " + appCtx.Command.Name
	if len(planMigrations) == 0 {
		fmt.Fprintf(appCtx.App.Writer, "%s: all migrations are up\n", subCmd)
		return nil
	}
	fmt.Fprintf(appCtx.App.Writer, "%s: migrations to apply: %d\n\n", subCmd, len(planMigrations))

	executed := 0
	for _, mig := range planMigrations {
		fmt.Fprintf(appCtx.App.Writer, "=  %s %v: migrating\n", time.Now().Format(timeFmt), mig.Id)
		start := time.Now()

		if appCtx.Bool("verbose") {
			fmt.Fprintf(appCtx.App.Writer, "\t%v\n", mig.Up)
		}

		n, err := glsql.MigrateSome(mig.Migration, db, ignoreUnknown)
		if err != nil {
			return fmt.Errorf("%s: fail: %w", time.Now().Format(timeFmt), err)
		}

		if n > 0 {
			fmt.Fprintf(appCtx.App.Writer, "== %s %v: applied (%s)\n", time.Now().Format(timeFmt), mig.Id, time.Since(start))

			// Additional migrations were run. No harm, but prevents us from tracking their execution duration.
			if n > 1 {
				fmt.Fprintf(appCtx.App.Writer, "warning: %v additional migrations were applied successfully\n", n-1)
			}
		} else {
			fmt.Fprintf(appCtx.App.Writer, "== %s %v: skipped (%s)\n", time.Now().Format(timeFmt), mig.Id, time.Since(start))
		}

		executed += n
	}

	fmt.Fprintf(appCtx.App.Writer, "\n%s: OK (applied %d migrations)\n", subCmd, executed)
	return nil
}
