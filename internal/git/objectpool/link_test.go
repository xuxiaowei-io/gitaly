//go:build !gitaly_test_sha256

package objectpool

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"google.golang.org/grpc/peer"
)

func TestLink(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist prior to creation")
	require.NoError(t, pool.Create(ctx, repo), "create pool")

	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)
	require.NoFileExists(t, altPath)

	require.NoError(t, pool.Link(ctx, repo))

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content := testhelper.MustReadFile(t, altPath)
	require.True(t, strings.HasPrefix(string(content), "../"), "expected %q to be relative path", content)

	require.NoError(t, pool.Link(ctx, repo))

	newContent := testhelper.MustReadFile(t, altPath)
	require.Equal(t, content, newContent)

	require.False(t, gittest.RemoteExists(t, cfg, pool.FullPath(), repoProto.GetGlRepository()), "pool remotes should not include %v", repo)
}

func TestLink_transactional(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, poolMemberProto := setupObjectPool(t, ctx)
	poolMember := localrepo.NewTestRepo(t, cfg, poolMemberProto)
	require.NoError(t, pool.Create(ctx, poolMember))

	txManager := transaction.NewTrackingManager()
	pool.txManager = txManager

	alternatesPath, err := poolMember.InfoAlternatesPath()
	require.NoError(t, err)
	require.NoFileExists(t, alternatesPath)

	ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})

	require.NoError(t, pool.Link(ctx, poolMember))

	require.Equal(t, 2, len(txManager.Votes()))
}

func TestLink_removeBitmap(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	// Initialize the pool and pull in all references from the repository.
	require.NoError(t, pool.Init(ctx))
	poolPath := pool.FullPath()
	gittest.Exec(t, cfg, "-C", poolPath, "fetch", repoPath, "+refs/*:refs/*")

	// Repack both the object pool and the pool member such that they both have bitmaps.
	gittest.Exec(t, cfg, "-C", poolPath, "repack", "-adb")
	requireHasBitmap(t, poolPath, true)
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-adb")
	requireHasBitmap(t, repoPath, true)

	// After linking the repository to its pool it should not have a bitmap anymore as Git does
	// not allow for multiple bitmaps to exist.
	require.NoError(t, pool.Link(ctx, repo))
	requireHasBitmap(t, poolPath, true)
	requireHasBitmap(t, repoPath, false)

	// Sanity-check that the repository is still consistent.
	gittest.Exec(t, cfg, "-C", repoPath, "fsck")
}

func requireHasBitmap(t *testing.T, repoPath string, expected bool) {
	hasBitmap, err := stats.HasBitmap(repoPath)
	require.NoError(t, err)
	require.Equal(t, expected, hasBitmap)
}

func TestLink_absoluteLinkExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist prior to creation")
	require.NoError(t, pool.Create(ctx, repo), "create pool")

	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)

	fullPath := filepath.Join(pool.FullPath(), "objects")

	require.NoError(t, os.WriteFile(altPath, []byte(fullPath), 0o644))

	require.NoError(t, pool.Link(ctx, repo), "we expect this call to change the absolute link to a relative link")

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content := testhelper.MustReadFile(t, altPath)
	require.False(t, filepath.IsAbs(string(content)), "expected %q to be relative path", content)

	repoObjectsPath := filepath.Join(repoPath, "objects")
	require.Equal(t, fullPath, filepath.Join(repoObjectsPath, string(content)), "the content of the alternates file should be the relative version of the absolute pat")

	require.True(t, gittest.RemoteExists(t, cfg, pool.FullPath(), "origin"), "pool remotes should include %v", repo)
}
