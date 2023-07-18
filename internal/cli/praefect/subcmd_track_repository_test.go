package praefect

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestTrackRepositorySubcommand(t *testing.T) {
	t.Parallel()
	g1Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	g2Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))
	testcfg.BuildGitalyHooks(t, g2Cfg)
	testcfg.BuildGitalySSH(t, g2Cfg)

	g1Srv := testserver.StartGitalyServer(t, g1Cfg, setup.RegisterAll, testserver.WithDisablePraefect())
	g2Srv := testserver.StartGitalyServer(t, g2Cfg, setup.RegisterAll, testserver.WithDisablePraefect())
	defer g2Srv.Shutdown()
	defer g1Srv.Shutdown()

	g1Addr := g1Srv.Address()

	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)

	virtualStorageName := "praefect"
	conf := config.Config{
		AllowLegacyElectors: true,
		SocketPath:          testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorageName,
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
					{Storage: g2Cfg.Storages[0].Name, Address: g2Srv.Address()},
				},
				DefaultReplicationFactor: 2,
			},
		},
		DB: dbConf,
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: config.ElectionStrategyPerRepository,
		},
	}
	confPath := writeConfigToFile(t, conf)

	gitalyCC, err := client.Dial(g1Addr, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, gitalyCC.Close()) }()
	ctx := testhelper.Context(t)

	gitaly1RepositoryClient := gitalypb.NewRepositoryServiceClient(gitalyCC)

	createRepoThroughGitaly1 := func(relativePath string) error {
		_, err := gitaly1RepositoryClient.CreateRepository(
			ctx,
			&gitalypb.CreateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  g1Cfg.Storages[0].Name,
					RelativePath: relativePath,
				},
			})
		return err
	}

	authoritativeStorage := g1Cfg.Storages[0].Name
	repoDS := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())

	relativePathAlreadyExist := "path/to/test/repo_2"
	require.NoError(t, createRepoThroughGitaly1(relativePathAlreadyExist))
	require.DirExists(t, filepath.Join(g1Cfg.Storages[0].Path, relativePathAlreadyExist))
	require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, relativePathAlreadyExist))
	idRelativePathAlreadyExist, err := repoDS.ReserveRepositoryID(ctx, virtualStorageName, relativePathAlreadyExist)
	require.NoError(t, err)
	require.NoError(t, repoDS.CreateRepository(
		ctx,
		idRelativePathAlreadyExist,
		virtualStorageName,
		relativePathAlreadyExist,
		relativePathAlreadyExist,
		g1Cfg.Storages[0].Name,
		nil,
		nil,
		true,
		true,
	))

	t.Run("fails", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			args     []string
			errorMsg string
		}{
			{
				name:     "positional arguments",
				args:     []string{"-virtual-storage=v", "-repository=r", "-authoritative-storage=s", "positional-arg"},
				errorMsg: "track-repository doesn't accept positional arguments",
			},
			{
				name: "virtual-storage is not set",
				args: []string{
					"-repository", "path/to/test/repo_1",
					"-authoritative-storage", authoritativeStorage,
				},
				errorMsg: `Required flag "virtual-storage" not set`,
			},
			{
				name: "repository is not set",
				args: []string{
					"-virtual-storage", virtualStorageName,
					"-authoritative-storage", authoritativeStorage,
				},
				errorMsg: `Required flag "repository" not set`,
			},
			{
				name: "authoritative-storage is not set",
				args: []string{
					"-virtual-storage", virtualStorageName,
					"-repository", "path/to/test/repo_1",
				},
				errorMsg: `Required flag "authoritative-storage" not set`,
			},
			{
				name: "repository does not exist",
				args: []string{
					"-virtual-storage", virtualStorageName,
					"-repository", "path/to/test/repo_1",
					"-authoritative-storage", authoritativeStorage,
				},
				errorMsg: "attempting to track repository in praefect database: authoritative repository does not exist",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, stderr, err := runApp(append([]string{"-config", confPath, trackRepositoryCmdName}, tc.args...))
				assert.Empty(t, stderr)
				require.EqualError(t, err, tc.errorMsg)
			})
		}
	})

	t.Run("ok", func(t *testing.T) {
		testCases := []struct {
			relativePath         string
			desc                 string
			replicateImmediately bool
			repositoryExists     bool
			expectedOutput       []string
		}{
			{
				desc:                 "force replication",
				relativePath:         "path/to/test/repo1",
				replicateImmediately: true,
				expectedOutput:       []string{"Finished replicating repository to \"gitaly-2\".\n"},
			},
			{
				desc:                 "do not force replication",
				relativePath:         "path/to/test/repo2",
				replicateImmediately: false,
				expectedOutput:       []string{"Added replication job to replicate repository to \"gitaly-2\".\n"},
			},
			{
				desc:             "records already exist",
				relativePath:     relativePathAlreadyExist,
				repositoryExists: true,
				expectedOutput: []string{
					"repository is already tracked in praefect database",
					"Finished adding new repository to be tracked in praefect database.",
					"Added replication job to replicate repository to \"gitaly-2\".\n",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				nodeMgr, err := nodes.NewManager(
					testhelper.NewDiscardingLogEntry(t),
					conf,
					db.DB,
					nil,
					promtest.NewMockHistogramVec(),
					protoregistry.GitalyProtoPreregistered,
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
				nodeMgr.Start(0, time.Hour)
				defer nodeMgr.Stop()

				exists, err := repoDS.RepositoryExists(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)
				require.Equal(t, tc.repositoryExists, exists)

				// create the repo on Gitaly without Praefect knowing
				if !tc.repositoryExists {
					require.NoError(t, createRepoThroughGitaly1(tc.relativePath))
					require.DirExists(t, filepath.Join(g1Cfg.Storages[0].Path, tc.relativePath))
					require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, tc.relativePath))
				}

				args := []string{
					"-virtual-storage", virtualStorageName,
					"-repository", tc.relativePath,
					"-authoritative-storage", authoritativeStorage,
				}
				if tc.replicateImmediately {
					args = append(args, "-replicate-immediately")
				}
				stdout, stderr, err := runApp(append([]string{"-config", confPath, trackRepositoryCmdName}, args...))
				assert.Empty(t, stderr)
				require.NoError(t, err)

				as := datastore.NewAssignmentStore(db, conf.StorageNames())

				repositoryID, err := repoDS.GetRepositoryID(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)

				assignments, err := as.GetHostAssignments(ctx, virtualStorageName, repositoryID)
				require.NoError(t, err)
				if tc.repositoryExists {
					require.Len(t, assignments, 1)
				} else {
					require.Len(t, assignments, 2)
					assert.Contains(t, assignments, g2Cfg.Storages[0].Name)
				}
				assert.Contains(t, assignments, g1Cfg.Storages[0].Name)

				exists, err = repoDS.RepositoryExists(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)
				assert.True(t, exists)
				for _, expectedOutput := range tc.expectedOutput {
					assert.Contains(t, stdout, expectedOutput)
				}

				if !tc.replicateImmediately {
					queue := datastore.NewPostgresReplicationEventQueue(db)
					events, err := queue.Dequeue(ctx, virtualStorageName, g2Cfg.Storages[0].Name, 1)
					require.NoError(t, err)
					assert.Len(t, events, 1)
					assert.Equal(t, tc.relativePath, events[0].Job.RelativePath)
				}
			})
		}
	})

	t.Run("replication event exists", func(t *testing.T) {
		relativePath := "path/to/test/repo_3"

		require.NoError(t, createRepoThroughGitaly1(relativePath))
		require.DirExists(t, filepath.Join(g1Cfg.Storages[0].Path, relativePath))
		require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, relativePath))

		_, _, err := runApp([]string{
			"-config", confPath,
			trackRepositoryCmdName,
			"-virtual-storage", virtualStorageName,
			"-repository", relativePath,
			"-authoritative-storage", authoritativeStorage,
		})
		require.NoError(t, err)
		// running the command twice means we try creating the replication event
		// again, which should log the duplicate but not break the flow.
		stdout, stderr, err := runApp([]string{
			"-config", confPath,
			trackRepositoryCmdName,
			"-virtual-storage", virtualStorageName,
			"-repository", relativePath,
			"-authoritative-storage", authoritativeStorage,
		})
		require.NoError(t, err)
		assert.Empty(t, stderr)
		assert.Contains(t, stdout, "replication event queue already has similar entry: replication event \"\" -> \"praefect\" -> \"gitaly-2\" -> \"path/to/test/repo_3\" already exists.")
	})
}
