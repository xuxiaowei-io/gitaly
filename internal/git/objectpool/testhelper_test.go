package objectpool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func setupObjectPool(t *testing.T, ctx context.Context) (config.Cfg, *ObjectPool, *localrepo.Repo) {
	t.Helper()

	cfg := testcfg.Build(t)
	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gitCommandFactory := gittest.NewCommandFactory(t, cfg, git.WithSkipHooks())

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

	pool, err := Create(
		ctx,
		config.NewLocator(cfg),
		gitCommandFactory,
		catfileCache,
		txManager,
		housekeeping.NewManager(cfg.Prometheus, txManager),
		&gitalypb.ObjectPool{
			Repository: &gitalypb.Repository{
				StorageName:  repo.GetStorageName(),
				RelativePath: gittest.NewObjectPoolName(t),
			},
		},
		repo,
	)
	require.NoError(t, err)

	return cfg, pool, repo
}

func hashDependentSize(sha1Size, sha256Size uint64) uint64 {
	if gittest.ObjectHashIsSHA256() {
		return sha256Size
	}
	return sha1Size
}
