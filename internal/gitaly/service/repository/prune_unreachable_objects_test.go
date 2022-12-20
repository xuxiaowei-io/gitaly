//go:build !gitaly_test_sha256

package repository

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestPruneUnreachableObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	setObjectTime := func(t *testing.T, repoPath string, objectID git.ObjectID, when time.Time) {
		looseObjectPath := filepath.Join(repoPath, "objects", objectID.String()[:2], objectID.String()[2:])
		require.NoError(t, os.Chtimes(looseObjectPath, when, when))
	}

	t.Run("missing repository", func(t *testing.T) {
		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{})
		if testhelper.IsPraefectEnabled() {
			testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("repo scoped: empty Repository"), err)
		} else {
			testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("empty Repository"), err)
		}
	})

	t.Run("relative path points to removed repository", func(t *testing.T) {
		repo, repoPath := git.CreateRepository(t, ctx, cfg)
		require.NoError(t, os.RemoveAll(repoPath))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		testhelper.RequireGrpcError(t, structerr.NewNotFound("GetRepoPath: not a git repository: %q", repoPath), err)
	})

	t.Run("empty repository", func(t *testing.T) {
		repo, _ := git.CreateRepository(t, ctx, cfg)

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)
	})

	t.Run("repository with reachable objects", func(t *testing.T) {
		repo, repoPath := git.CreateRepository(t, ctx, cfg)

		// Create the commit and a branch pointing to it to make it reachable.
		commitID := git.WriteTestCommit(t, cfg, repoPath, git.WithBranch("branch"))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// Verify we can still read the commit.
		git.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", commitID.String()+"^{commit}")
	})

	t.Run("repository with recent unreachable objects", func(t *testing.T) {
		repo, repoPath := git.CreateRepository(t, ctx, cfg)

		// Create the commit, but don't create a reference pointing to it.
		commitID := git.WriteTestCommit(t, cfg, repoPath)
		// Set the object time to something that's close to 30 minutes, but gives us enough
		// room to not cause flakes.
		setObjectTime(t, repoPath, commitID, time.Now().Add(-28*time.Minute))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// Verify we can still read the commit. The commit isn't older than 30 minutes, so
		// it shouldn't be pruned.
		git.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", commitID.String()+"^{commit}")
	})

	t.Run("repository with old unreachable objects", func(t *testing.T) {
		repo, repoPath := git.CreateRepository(t, ctx, cfg)

		// Create the commit, but don't create a reference pointing to it.
		commitID := git.WriteTestCommit(t, cfg, repoPath)
		setObjectTime(t, repoPath, commitID, time.Now().Add(-31*time.Minute))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		cmd := git.NewCommand(t, cfg, "-C", repoPath, "rev-parse", "--verify", commitID.String()+"^{commit}")
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Equal(t, "fatal: Needed a single revision\n", string(output))
	})

	t.Run("repository with mixed objects", func(t *testing.T) {
		repo, repoPath := git.CreateRepository(t, ctx, cfg)

		reachableOldCommit := git.WriteTestCommit(t, cfg, repoPath, git.WithMessage("a"), git.WithBranch("branch"))
		setObjectTime(t, repoPath, reachableOldCommit, time.Now().Add(-31*time.Minute))

		unreachableRecentCommit := git.WriteTestCommit(t, cfg, repoPath, git.WithMessage("b"))
		setObjectTime(t, repoPath, unreachableRecentCommit, time.Now().Add(-28*time.Minute))

		unreachableOldCommit := git.WriteTestCommit(t, cfg, repoPath, git.WithMessage("c"))
		setObjectTime(t, repoPath, unreachableOldCommit, time.Now().Add(-31*time.Minute))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// The reachable old and unreachable recent commits should still exist.
		git.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", reachableOldCommit.String()+"^{commit}")
		git.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", unreachableRecentCommit.String()+"^{commit}")

		// But the unreachable old commit should have been pruned.
		cmd := git.NewCommand(t, cfg, "-C", repoPath, "rev-parse", "--verify", unreachableOldCommit.String()+"^{commit}")
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Equal(t, "fatal: Needed a single revision\n", string(output))
	})

	t.Run("repository with commit-graph", func(t *testing.T) {
		repo, repoPath := git.CreateRepository(t, ctx, cfg)

		// Write two commits into the repository and create a commit-graph. The second
		// commit will become unreachable and will be pruned, but will be contained in the
		// commit-graph.
		rootCommitID := git.WriteTestCommit(t, cfg, repoPath)
		unreachableCommitID := git.WriteTestCommit(t, cfg, repoPath, git.WithParents(rootCommitID), git.WithBranch("main"))
		git.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split", "--changed-paths")

		// Reset the "main" branch back to the initial root commit ID and prune the now
		// unreachable second commit.
		git.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/main", rootCommitID.String())

		// Modify the modification time of the unreachable commit ID so that it will be
		// pruned.
		setObjectTime(t, repoPath, unreachableCommitID, time.Now().Add(-30*time.Minute))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// The commit-graph chain should have been rewritten by PruneUnreachableObjects so
		// that it doesn't refer to the pruned commit anymore.
		git.Exec(t, cfg, "-C", repoPath, "commit-graph", "verify")
	})
}
