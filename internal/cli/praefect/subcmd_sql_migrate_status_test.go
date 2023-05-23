package praefect

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
)

func TestSQLMigrateStatusSubcommand(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	dbCfg := testdb.GetConfig(t, db.Name)
	cfg := config.Config{
		ListenAddr:      "/dev/null",
		VirtualStorages: []*config.VirtualStorage{{Name: "p", Nodes: []*config.Node{{Storage: "s", Address: "localhost"}}}},
		DB:              dbCfg,
	}
	confPath := writeConfigToFile(t, cfg)

	for _, tc := range []struct {
		desc         string
		args         []string
		expectedOuts []string
		expectedErr  error
	}{
		{
			desc: "ok",
			expectedOuts: []string{
				migrations.All()[len(migrations.All())-1].Id,
				migrations.All()[0].Id,
			},
		},
		{
			desc:        "unexpected positional arguments",
			args:        []string{"positional-arg"},
			expectedErr: cli.Exit(unexpectedPositionalArgsError{Command: "sql-migrate-status"}, 1),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := cli.App{
				Reader:          bytes.NewReader(nil),
				Writer:          &stdout,
				ErrWriter:       &stderr,
				HideHelpCommand: true,
				Commands: []*cli.Command{
					newSQLMigrateStatusCommand(),
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "config",
						Value: confPath,
					},
				},
			}
			err := app.Run(append([]string{progname, sqlMigrateStatusCmdName}, tc.args...))
			require.Equal(t, tc.expectedErr, err)
			assert.Empty(t, stderr.String())
			for _, expectedOut := range tc.expectedOuts {
				assert.Contains(t, stdout.String(), expectedOut)
			}
		})
	}
}
