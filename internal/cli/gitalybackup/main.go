package gitalybackup

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/version"
)

func init() {
	// Override the version printer so the output format matches what Praefect
	// used before the introduction of the CLI toolkit.
	cli.VersionPrinter = func(ctx *cli.Context) {
		fmt.Fprintln(ctx.App.Writer, version.GetVersionString(binaryName))
	}
}

const (
	progname = "gitaly-backup"

	pathFlagName = "path"
	binaryName   = "Gitaly Backup"
)

// NewApp returns a new praefect app.
func NewApp() *cli.App {
	return &cli.App{
		Name:    progname,
		Usage:   "create gitaly backups",
		Version: version.GetVersionString(binaryName),
		// serveAction is also here in the root to keep the CLI backwards compatible with
		// the previous way to launch Praefect with just `praefect -config FILE`.
		// We may want to deprecate this eventually.
		//
		// The 'DefaultCommand: "serve"' setting can't be used here because it won't be
		// possible to invoke sub-command not yet registered.
		// Action: serveAction,
		Commands: []*cli.Command{
			newCreateCommand(),
			newRestoreCommand(),
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				// We can't mark it required, because it is not for all sub-commands.
				// We need it as it is used by majority of the sub-commands and
				// because of the existing format of commands invocation.
				Name:  pathFlagName,
				Usage: "Directory where the backup files will be created/restored.",
			},
		},
	}
}
