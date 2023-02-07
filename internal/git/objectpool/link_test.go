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
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"google.golang.org/grpc/peer"
)

func TestLink(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repo := setupObjectPool(t, ctx)

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

	require.Empty(t, gittest.Exec(t, cfg, "-C", gittest.RepositoryPath(t, pool), "remote"))
}

func TestLink_transactional(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	_, pool, repo := setupObjectPool(t, ctx)

	txManager := transaction.NewTrackingManager()
	pool.txManager = txManager

	alternatesPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)
	require.NoFileExists(t, alternatesPath)

	ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})

	require.NoError(t, pool.Link(ctx, repo))

	require.Equal(t, 2, len(txManager.Votes()))
}

func TestLink_removeBitmap(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repo := setupObjectPool(t, ctx)
	poolPath := gittest.RepositoryPath(t, pool)
	repoPath := gittest.RepositoryPath(t, repo)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	// Pull in all references from the repository.
	gittest.Exec(t, cfg, "-C", poolPath, "fetch", repoPath, "+refs/*:refs/*")

	// Repack both the object pool and the pool member such that they both have bitmaps.
	gittest.Exec(t, cfg, "-C", poolPath, "repack", "-adb")
	requireHasBitmap(t, pool.Repo, true)
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-adb")
	requireHasBitmap(t, repo, true)

	// After linking the repository to its pool it should not have a bitmap anymore as Git does
	// not allow for multiple bitmaps to exist.
	require.NoError(t, pool.Link(ctx, repo))
	requireHasBitmap(t, pool.Repo, true)
	requireHasBitmap(t, repo, false)

	// Sanity-check that the repository is still consistent.
	gittest.Exec(t, cfg, "-C", repoPath, "fsck")
}

func requireHasBitmap(t *testing.T, repo *localrepo.Repo, expected bool) {
	packfilesInfo, err := stats.PackfilesInfoForRepository(repo)
	require.NoError(t, err)
	require.Equal(t, expected, packfilesInfo.Bitmap.Exists)
}

func TestLink_absoluteLinkExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repo := setupObjectPool(t, ctx)
	poolPath := gittest.RepositoryPath(t, pool)
	poolObjectsPath := gittest.RepositoryPath(t, pool, "objects")
	repoPath := gittest.RepositoryPath(t, repo)

	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(altPath, []byte(poolObjectsPath), perm.SharedFile))

	require.NoError(t, pool.Link(ctx, repo), "we expect this call to change the absolute link to a relative link")

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content := testhelper.MustReadFile(t, altPath)
	require.False(t, filepath.IsAbs(string(content)), "expected %q to be relative path", content)

	repoObjectsPath := filepath.Join(repoPath, "objects")
	require.Equal(t, poolObjectsPath, filepath.Join(repoObjectsPath, string(content)), "the content of the alternates file should be the relative version of the absolute pat")

	require.Empty(t, gittest.Exec(t, cfg, "-C", poolPath, "remote"))
}
