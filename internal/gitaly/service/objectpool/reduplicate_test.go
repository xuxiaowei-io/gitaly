//go:build !gitaly_test_sha256

package objectpool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestReduplicate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, _, client := setup(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	// Create the object pool and repack it. This is required for our test setup as Git won't
	// deduplicate objects in the pool member when they're not in a packfile in the object pool.
	_, pool, poolPath := createObjectPool(t, ctx, cfg, client, repoProto)
	gittest.Exec(t, cfg, "-C", poolPath, "repack", "-Ad")

	// Link the repository to the pool and garbage collect it to get rid of the duplicate
	// objects.
	require.NoError(t, pool.Link(ctx, repo))
	gittest.Exec(t, cfg, "-C", repoPath, "-c", "commitGraph.generationVersion=2", "gc")
	packedRefsStat, err := os.Stat(filepath.Join(repoPath, "packed-refs"))
	require.NoError(t, err)
	// Verify that the pool member has no objects on its own anymore.
	repoInfo, err := stats.RepositoryInfoForRepository(repo)
	require.NoError(t, err)
	require.Equal(t, stats.RepositoryInfo{
		References: stats.ReferencesInfo{
			PackedReferencesSize: uint64(packedRefsStat.Size()),
		},
		CommitGraph: stats.CommitGraphInfo{
			Exists:            true,
			HasGenerationData: true,
		},
		Alternates: []string{filepath.Join(poolPath, "objects")},
	}, repoInfo)

	// git-repack(1) generates these files. Manually remove them so that we can assert further
	// down that repository reduplication doesn't regenerate those paths.
	require.NoError(t, os.Remove(filepath.Join(repoPath, "info", "refs")))
	require.NoError(t, os.Remove(filepath.Join(repoPath, "objects", "info", "packs")))

	// Unlink the pool member and verify that we indeed can't find the commit anymore as a
	// sanity check.
	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)
	require.NoError(t, os.Remove(altPath))
	gittest.RequireObjectNotExists(t, cfg, repoPath, commitID)

	// Re-link the repository to the pool and reduplicate the objects. This should cause us to
	// pull all objects into the repository again.
	require.NoError(t, pool.Link(ctx, repo))
	//nolint:staticcheck
	_, err = client.ReduplicateRepository(ctx, &gitalypb.ReduplicateRepositoryRequest{Repository: repoProto})
	require.NoError(t, err)

	// So consequentially, if we unlink now we should be able to still find the commit.
	require.NoError(t, os.Remove(altPath))
	gittest.RequireObjectExists(t, cfg, repoPath, commitID)

	require.NoFileExists(t, filepath.Join(repoPath, "info", "refs"))
	require.NoFileExists(t, filepath.Join(repoPath, "objects", "info", "packs"))
}
