package repoutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestLock(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	repo := &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: gittest.NewRepositoryName(t),
	}

	repoPath, err := locator.GetPath(repo)
	require.NoError(t, err)

	unlock, err := Lock(ctx, locator, repo)
	require.NoError(t, err)

	require.FileExists(t, repoPath+".lock")

	_, err = Lock(ctx, locator, repo)
	require.ErrorIs(t, err, safe.ErrFileAlreadyLocked)

	unlock()

	require.NoFileExists(t, repoPath+".lock")
}
