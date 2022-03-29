package repocleaner

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
)

func TestLogWarnAction_Perform(t *testing.T) {
	ctx := testhelper.Context(t)

	logger, hook := test.NewNullLogger()
	action := NewLogWarnAction(logger)
	err := action.Perform(ctx, "vs1", "g1", []string{"p/1", "p/2"})
	require.NoError(t, err)
	require.Len(t, hook.AllEntries(), 2)

	exp := []map[string]interface{}{{
		"Data": logrus.Fields{
			"component":             "repocleaner.log_warn_action",
			"virtual_storage":       "vs1",
			"storage":               "g1",
			"relative_replica_path": "p/1",
		},
		"Message": "repository is not managed by praefect",
	}, {
		"Data": logrus.Fields{
			"component":             "repocleaner.log_warn_action",
			"virtual_storage":       "vs1",
			"storage":               "g1",
			"relative_replica_path": "p/2",
		},
		"Message": "repository is not managed by praefect",
	}}

	require.ElementsMatch(t, exp, []map[string]interface{}{{
		"Data":    hook.AllEntries()[0].Data,
		"Message": hook.AllEntries()[0].Message,
	}, {
		"Data":    hook.AllEntries()[1].Data,
		"Message": hook.AllEntries()[1].Message,
	}})
}

func TestCleanAction_Perform(t *testing.T) {
	t.Parallel()

	const (
		repo1RelPath   = "repo-1.git"
		storage1       = "gitaly-3"
		virtualStorage = "praefect"
	)

	g1Cfg := testcfg.Build(t, testcfg.WithStorages(storage1))

	g1Addr := testserver.RunGitalyServer(t, g1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorage,
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
				},
			},
		},
		DB: dbConf,
	}

	gittest.CloneRepo(t, g1Cfg, g1Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo1RelPath})

	repoStore := datastore.NewPostgresRepositoryStore(db.DB, nil)

	ctx := testhelper.Context(t)
	require.NoError(t, repoStore.CreateRepository(ctx, 1, conf.VirtualStorages[0].Name, repo1RelPath, repo1RelPath, storage1, []string{}, nil, false, false))

	logger, hook := test.NewNullLogger()

	entry := logger.WithContext(ctx)
	clientHandshaker := backchannel.NewClientHandshaker(entry, praefect.NewBackchannelServerFactory(entry, transaction.NewServer(nil), nil))
	nodeSet, err := praefect.DialNodes(ctx, conf.VirtualStorages, protoregistry.GitalyProtoPreregistered, nil, clientHandshaker, nil)
	require.NoError(t, err)
	defer nodeSet.Close()

	cleanAction := NewCleanAction(logger, nodeSet.Connections())

	require.NoError(t, cleanAction.Perform(ctx, virtualStorage, storage1, []string{repo1RelPath}))
	resultLogEntry := hook.LastEntry()

	assert.Equal(t, []string{repo1RelPath}, resultLogEntry.Data["cleaned_repos"])
	assert.Equal(t, "cleaned repositories not managed by praefect", resultLogEntry.Message)

	storagePath, found := g1Cfg.StoragePath(storage1)
	require.True(t, found)
	assert.DirExists(t, filepath.Join(storagePath, "+gitaly", "lost+found", time.Now().Format("2006-01-02")))
	assert.NoDirExists(t, filepath.Join(storagePath, repo1RelPath))
}
