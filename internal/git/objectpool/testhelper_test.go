//go:build !gitaly_test_sha256

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
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type setupObjectPoolConfig struct {
	seededRepo bool
}

var withSeededRepo = setupObjectPoolConfig{
	seededRepo: true,
}

func setupObjectPool(t *testing.T, ctx context.Context, optionalCfg ...setupObjectPoolConfig) (config.Cfg, *ObjectPool, *gitalypb.Repository) {
	t.Helper()

	require.LessOrEqual(t, len(optionalCfg), 1, "must either pass one or no optional configs")
	var setupCfg setupObjectPoolConfig
	if len(optionalCfg) == 1 {
		setupCfg = optionalCfg[0]
	}

	var seed string
	if setupCfg.seededRepo {
		seed = gittest.SeedGitLabTest
	}

	cfg := testcfg.Build(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   seed,
	})

	if setupCfg.seededRepo {
		gittest.FixGitLabTestRepoForCommitGraphs(t, cfg, repoPath)
	}

	gitCommandFactory := gittest.NewCommandFactory(t, cfg, git.WithSkipHooks())

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

	pool, err := NewObjectPool(
		config.NewLocator(cfg),
		gitCommandFactory,
		catfileCache,
		txManager,
		housekeeping.NewManager(cfg.Prometheus, txManager),
		repo.GetStorageName(),
		gittest.NewObjectPoolName(t),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := pool.Remove(ctx); err != nil {
			panic(err)
		}
	})

	return cfg, pool, repo
}
