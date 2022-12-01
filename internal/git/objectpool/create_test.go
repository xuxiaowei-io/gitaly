//go:build !gitaly_test_sha256

package objectpool

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	require.NoError(t, pool.Create(ctx, repo))
	require.True(t, pool.IsValid())

	// There should not be a "hooks" directory in the pool.
	require.NoDirExists(t, filepath.Join(pool.FullPath(), "hooks"))
	// The "origin" remote of the pool points to the pool member.
	require.Equal(t, repoPath, text.ChompBytes(gittest.Exec(t, cfg, "-C", pool.FullPath(), "remote", "get-url", "origin")))
	// The "master" branch points to the same commit as in the pool member.
	require.Equal(t, commitID, gittest.ResolveRevision(t, cfg, pool.FullPath(), "refs/heads/master"))
	// Objects exist in the pool repository.
	gittest.RequireObjectExists(t, cfg, pool.FullPath(), commitID)
}

func TestCreate_subdirsExist(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	err := pool.Create(ctx, repo)
	require.NoError(t, err)

	require.NoError(t, pool.Remove(ctx))

	// Recreate pool so the subdirs exist already
	err = pool.Create(ctx, repo)
	require.NoError(t, err)
}

func TestClone_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	_, pool, repo := setupObjectPool(t, ctx)

	require.NoError(t, pool.clone(ctx, repo))

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestClone_existingPool(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	_, pool, repo := setupObjectPool(t, ctx)

	// The first time around cloning should succeed, but ...
	require.NoError(t, pool.clone(ctx, repo))

	// ... when we try to clone the same pool a second time we should get an error because the
	// destination exists already.
	require.EqualError(t, pool.clone(ctx, repo), fmt.Sprintf(
		"cloning to pool: exit status 128, stderr: \"fatal: destination path '%s' already exists and is not an empty directory.\\n\"",
		pool.FullPath(),
	))
}

func TestClone_fsck(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repo := setupObjectPool(t, ctx)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	// Write a tree into the repository that's known-broken.
	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Content: "content", Path: "dup", Mode: "100644"},
		{Content: "content", Path: "dup", Mode: "100644"},
	})

	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(),
		gittest.WithBranch("master"),
		gittest.WithTree(treeID),
	)

	// While git-clone(1) would normally complain about the broken tree we have just cloned, we
	// don't expect the clone to fail. This is because we know that the tree is already in one
	// of our repositories that we have locally, so raising an error now doesn't make a whole
	// lot of sense in the first place.
	//
	// Note: this works because we use `git clone --local`, which only creates a copy of the
	// repository without performing consistency checks.
	require.NoError(t, pool.clone(ctx, repo))

	// Verify that the broken tree is indeed in the pool repository and that it is reported as
	// broken by git-fsck(1).
	var stderr bytes.Buffer
	fsckCmd := gittest.NewCommand(t, cfg, "-C", pool.FullPath(), "fsck")
	fsckCmd.Stderr = &stderr

	require.EqualError(t, fsckCmd.Run(), "exit status 1")
	require.Equal(t, fmt.Sprintf("error in tree %s: duplicateEntries: contains duplicate file entries\n", treeID), stderr.String())
}
