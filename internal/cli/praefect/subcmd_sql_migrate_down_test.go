package praefect

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
)

func TestSQLMigrateDownSubcommand(t *testing.T) {
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
		desc           string
		args           []string
		expectedErr    error
		expectedOutput []string
	}{
		{
			desc:        "no args passed",
			expectedErr: errors.New("sql-migrate-down requires a single positional argument"),
		},
		{
			desc:        "too many args passed",
			args:        []string{"123", "abc", "file.txt"},
			expectedErr: errors.New("sql-migrate-down accepts only single positional argument"),
		},
		{
			desc: "dry-run",
			args: []string{"1"},
			expectedOutput: []string{
				"DRY RUN -- would roll back:",
				migrations.All()[len(migrations.All())-1].Id,
				"To apply these migrations run with -f",
			},
		},
		{
			desc: "force run",
			args: []string{"-f", "1"},
			expectedOutput: []string{
				`OK (applied 1 "down" migrations)`,
			},
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
					newSQLMigrateDownCommand(),
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "config",
						Value: confPath,
					},
				},
			}
			err := app.Run(append([]string{progname, sqlMigrateDownCmdName}, tc.args...))
			require.Equal(t, tc.expectedErr, err)
			assert.Empty(t, stderr.String())
			for _, expectedOutput := range tc.expectedOutput {
				assert.Contains(t, stdout.String(), expectedOutput)
			}
		})
	}
}
