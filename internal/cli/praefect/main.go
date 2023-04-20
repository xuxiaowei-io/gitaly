// Command praefect provides a reverse-proxy server with high-availability
// specific features for Gitaly.
//
// Additionally, praefect has subcommands for common tasks:
//
// # SQL Ping
//
// The subcommand "sql-ping" checks if the database configured in the config
// file is reachable:
//
//	praefect -config PATH_TO_CONFIG sql-ping
//
// # SQL Migrate
//
// The subcommand "sql-migrate" will apply any outstanding SQL migrations.
//
//	praefect -config PATH_TO_CONFIG sql-migrate [-ignore-unknown=true|false]
//
// By default, the migration will ignore any unknown migrations that are
// not known by the Praefect binary.
//
// "-ignore-unknown=false" will disable this behavior.
//
// The subcommand "sql-migrate-status" will show which SQL migrations have
// been applied and which ones have not:
//
//	praefect -config PATH_TO_CONFIG sql-migrate-status
//
// # Dial Nodes
//
// The subcommand "dial-nodes" helps diagnose connection problems to Gitaly or
// Praefect. The subcommand works by sourcing the connection information from
// the config file, and then dialing and health checking the remote nodes.
//
//	praefect -config PATH_TO_CONFIG dial-nodes
//
// # Dataloss
//
// The subcommand "dataloss" identifies Gitaly nodes which are missing data from the
// previous write-enabled primary node. It does so by looking through incomplete
// replication jobs. This is useful for identifying potential data loss from a failover
// event.
//
//	praefect -config PATH_TO_CONFIG dataloss [-virtual-storage <virtual-storage>]
//
// "-virtual-storage" specifies which virtual storage to check for data loss. If not specified,
// the check is performed for every configured virtual storage.

package praefect

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/version"
)

func init() {
	// Override the version printer so the output format matches what Praefect
	// used before the introduction of the CLI toolkit.
	cli.VersionPrinter = func(ctx *cli.Context) {
		fmt.Fprintln(ctx.App.Writer, version.GetVersionString("Praefect"))
	}
}

const (
	progname = "praefect"

	configFlagName = "config"
)

// NewApp returns a new praefect app.
func NewApp() *cli.App {
	return &cli.App{
		Name:    progname,
		Usage:   "a gitaly proxy",
		Version: version.GetVersionString("Praefect"),
		// serveAction is also here in the root to keep the CLI backwards compatible with
		// the previous way to launch Praefect with just `praefect -config FILE`.
		// We may want to deprecate this eventually.
		//
		// The 'DefaultCommand: "serve"' setting can't be used here because it won't be
		// possible to invoke sub-command not yet registered.
		Action: serveAction,
		Commands: []*cli.Command{
			newServeCommand(),
			newConfigurationCommand(),
			newAcceptDatalossCommand(),
			newCheckCommand(service.AllChecks()),
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				// We can't mark it required, because it is not for all sub-commands.
				// We need it as it is used by majority of the sub-commands and
				// because of the existing format of commands invocation.
				Name:  configFlagName,
				Usage: "load configuration from `FILE`",
			},
		},
		CustomAppHelpTemplate: helpTextTemplate(),
	}
}

// mustProvideConfigFlag extracts value of the 'config' flag and returns it.
// If flag is not set the help for the command will be printed and terminated with exit code 2.
func mustProvideConfigFlag(ctx *cli.Context, command string) string {
	pathToConfigFile := ctx.String(configFlagName)
	if pathToConfigFile == "" {
		// We can't make 'config' flag required for all commands, but we still want the
		// same output to be printed if it is not provided.
		// It should be removed after migration to the `praefect CMD -config FILE`
		// where we can mark it as required for each sub-command.
		_ = cli.ShowCommandHelp(ctx, command)
		log.Printf("Required flag %q not set\n", configFlagName)
		os.Exit(2)
	}

	return pathToConfigFile
}

func helpTextTemplate() string {
	var cmds []string
	for k := range subcommands(nil) {
		cmds = append(cmds, k)
	}
	sort.Strings(cmds)

	// Because not all sub-commands are registered with the new approach they won't be shown
	// with the -help. To have them in the output we inject a simple list of their names into
	// the template to have them presented.
	return strings.Replace(
		cli.AppHelpTemplate,
		`COMMANDS:{{template "visibleCommandCategoryTemplate" .}}{{end}}`,
		`COMMANDS:{{template "visibleCommandCategoryTemplate" .}}{{end}}`+
			"\n   "+strings.Join(cmds, "\n   "),
		1,
	)
}
