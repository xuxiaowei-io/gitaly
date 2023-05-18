package praefect

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetReplicationFactorSubcommand(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	for _, tc := range []struct {
		desc   string
		args   []string
		store  praefect.AssignmentStore
		error  error
		stdout string
	}{
		{
			desc:  "unexpected positional arguments",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=1", "positonal-arg"},
			error: cli.Exit(unexpectedPositionalArgsError{Command: "set-replication-factor"}, 1),
		},
		{
			desc:  "missing virtual-storage",
			args:  []string{"-repository=relative-path", "-replication-factor=1"},
			error: errors.New(`Required flag "virtual-storage" not set`),
		},
		{
			desc:  "missing repository",
			args:  []string{"-virtual-storage=virtual-storage", "-replication-factor=1"},
			error: errors.New(`Required flag "repository" not set`),
		},
		{
			desc:  "missing replication-factor",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path"},
			error: errors.New(`Required flag "replication-factor" not set`),
		},
		{
			desc:  "replication factor too small",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=0"},
			error: status.Error(codes.InvalidArgument, "set replication factor: attempted to set replication factor 0 but minimum is 1"),
		},
		{
			desc:  "replication factor too big",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=3"},
			error: status.Error(codes.InvalidArgument, "set replication factor: attempted to set replication factor 3 but virtual storage only contains 2 storages"),
		},
		{
			desc:  "virtual storage not found",
			args:  []string{"-virtual-storage=non-existent", "-repository=relative-path", "-replication-factor=2"},
			error: status.Error(codes.InvalidArgument, `set replication factor: virtual storage "non-existent" not found`),
		},
		{
			desc:  "repository not found",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=non-existent", "-replication-factor=2"},
			error: status.Error(codes.InvalidArgument, `set replication factor: repository "virtual-storage"/"non-existent" not found`),
		},
		{
			desc:  "assignments are disabled",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=1"},
			store: praefect.NewDisabledAssignmentStore(nil),
			error: status.Error(codes.Internal, `set replication factor: assignments are disabled`),
		},
		{
			desc:   "successfully set",
			args:   []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=2"},
			stdout: "current assignments: primary, secondary\n",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			db.TruncateAll(t)

			store := tc.store
			if tc.store == nil {
				store = datastore.NewAssignmentStore(db, map[string][]string{"virtual-storage": {"primary", "secondary"}})
			}

			// create a repository record
			require.NoError(t,
				datastore.NewPostgresRepositoryStore(db, nil).CreateRepository(ctx, 1, "virtual-storage", "relative-path", "relative-path", "primary", nil, nil, false, false),
			)

			ln, clean := listenAndServe(t, []svcRegistrar{registerPraefectInfoServer(
				info.NewServer(config.Config{}, nil, store, nil, nil),
			)})
			defer clean()

			conf := config.Config{
				SocketPath: ln.Addr().String(),
				VirtualStorages: []*config.VirtualStorage{
					{
						Name: "praefect",
						Nodes: []*config.Node{
							{Storage: "storage", Address: "address"},
						},
					},
				},
				DB: testdb.GetConfig(t, db.Name),
			}

			confPath := writeConfigToFile(t, conf)
			var stdout bytes.Buffer
			app := cli.App{
				Reader:          bytes.NewReader(nil),
				Writer:          &stdout,
				ErrWriter:       io.Discard,
				HideHelpCommand: true,
				Commands: []*cli.Command{
					newSetReplicationFactorCommand(),
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "config",
						Value: confPath,
					},
				},
			}
			err := app.Run(append([]string{progname, setReplicationFactorCmdName}, tc.args...))
			testhelper.RequireGrpcError(t, tc.error, err)
			if tc.stdout != "" {
				require.Equal(t, tc.stdout, stdout.String())
			}
		})
	}
}
