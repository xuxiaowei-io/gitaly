package praefect

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
)

func TestSQLPingSubcommand(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		confPath       func(t2 *testing.T) string
		args           []string
		expectedOutput string
		expectedErr    error
	}{
		{
			desc:        "unexpected positional arguments",
			args:        []string{"positional-arg"},
			confPath:    func(t *testing.T) string { return "stub" },
			expectedErr: unexpectedPositionalArgsError{Command: "sql-ping"},
		},
		{
			desc: "ok",
			confPath: func(t *testing.T) string {
				db := testdb.New(t)
				dbCfg := testdb.GetConfig(t, db.Name)
				cfg := config.Config{
					ListenAddr:      "/dev/null",
					VirtualStorages: []*config.VirtualStorage{{Name: "p", Nodes: []*config.Node{{Storage: "s", Address: "localhost"}}}},
					DB:              dbCfg,
				}
				return writeConfigToFile(t, cfg)
			},
			expectedOutput: "praefect sql-ping: OK\n",
		},
		{
			desc: "unreachable",
			confPath: func(t *testing.T) string {
				cfg := config.Config{
					ListenAddr:      "/dev/null",
					VirtualStorages: []*config.VirtualStorage{{Name: "p", Nodes: []*config.Node{{Storage: "s", Address: "localhost"}}}},
					DB:              config.DB{Host: "/dev/null", Port: 5432, User: "postgres", DBName: "gl"},
				}
				return writeConfigToFile(t, cfg)
			},
			expectedErr: errors.New("sql open: send ping: failed to connect to `host=/dev/null user=postgres database=gl`: dial error (dial unix /dev/null/.s.PGSQL.5432: connect: not a directory)"),
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
					newSQLPingCommand(),
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "config",
						Value: tc.confPath(t),
					},
				},
			}
			err := app.Run(append([]string{progname, sqlPingCmdName}, tc.args...))
			if tc.expectedErr != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
			}
			assert.Empty(t, stderr.String())
			if tc.expectedOutput != "" {
				assert.Equal(t, tc.expectedOutput, stdout.String())
			}
		})
	}
}
