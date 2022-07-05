package objectpool

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestClone_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	require.NoError(t, pool.clone(ctx, repo))

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestClone_existingPool(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// The first time around cloning should succeed, but ...
	require.NoError(t, pool.clone(ctx, repo))

	// ... when we try to clone the same pool a second time we should get an error because the
	// destination exists already.
	require.EqualError(t, pool.clone(ctx, repo), fmt.Sprintf(
		"cloning to pool: exit status 128, stderr: \"fatal: destination path '%s' already exists and is not an empty directory.\\n\"",
		pool.FullPath(),
	))
}
