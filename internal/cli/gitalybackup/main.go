package gitalybackup

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/version"
)

func init() {
	cli.VersionPrinter = func(ctx *cli.Context) {
		fmt.Fprintln(ctx.App.Writer, version.GetVersionString(binaryName))
	}
}

const (
	progname = "gitaly-backup"

	pathFlagName = "path"
	binaryName   = "Gitaly Backup"
)

// NewApp returns a new gitaly[backup app.
func NewApp() *cli.App {
	return &cli.App{
		Name:    progname,
		Usage:   "create gitaly backups",
		Version: version.GetVersionString(binaryName),
		Commands: []*cli.Command{
			newCreateCommand(),
			newRestoreCommand(),
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  pathFlagName,
				Usage: "Directory where the backup files will be created/restored.",
			},
		},
	}
}
