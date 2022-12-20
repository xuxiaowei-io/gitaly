//go:build !gitaly_test_sha256

package objectpool

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := git.CreateRepository(t, ctx, cfg, git.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	commitID := git.WriteTestCommit(t, cfg, repoPath, git.WithBranch("master"))

	createPool := func(t *testing.T, poolProto *gitalypb.ObjectPool) (*ObjectPool, string, error) {
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)
		txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

		pool, err := Create(
			ctx,
			config.NewLocator(cfg),
			git.NewCommandFactory(t, cfg, git.WithSkipHooks()),
			catfileCache,
			txManager,
			housekeeping.NewManager(cfg.Prometheus, txManager),
			poolProto,
			repo,
		)
		if err != nil {
			return nil, "", err
		}

		return pool, git.RepositoryPath(t, pool), nil
	}

	t.Run("successful", func(t *testing.T) {
		_, poolPath, err := createPool(t, &gitalypb.ObjectPool{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: git.NewObjectPoolName(t),
			},
		})
		require.NoError(t, err)

		// There should not be a "hooks" directory in the pool.
		require.NoDirExists(t, filepath.Join(poolPath, "hooks"))
		// The repository has no remote.
		require.Empty(t, git.Exec(t, cfg, "-C", poolPath, "remote"))
		// The "master" branch points to the same commit as in the pool member.
		require.Equal(t, commitID, git.ResolveRevision(t, cfg, poolPath, "refs/heads/master"))
		// Objects exist in the pool repository.
		git.RequireObjectExists(t, cfg, poolPath, commitID)
	})

	t.Run("target exists", func(t *testing.T) {
		relativePath := git.NewObjectPoolName(t)
		fullPath := filepath.Join(cfg.Storages[0].Path, relativePath)

		// We currently allow creating object pools when the target path is an empty
		// directory. This can be considered a bug, but for now we abide.
		require.NoError(t, os.MkdirAll(fullPath, 0o755))

		_, _, err := createPool(t, &gitalypb.ObjectPool{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: relativePath,
			},
		})
		testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("target path exists already"), err)
	})

	t.Run("consistency check", func(t *testing.T) {
		// Write a tree into the repository that's known-broken.
		treeID := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
			{Content: "content", Path: "dup", Mode: "100644"},
			{Content: "content", Path: "dup", Mode: "100644"},
		})
		git.WriteTestCommit(t, cfg, repoPath,
			git.WithParents(),
			git.WithBranch("master"),
			git.WithTree(treeID),
		)

		// While git-clone(1) would normally complain about the broken tree we have just
		// cloned, we don't expect the clone to fail. This is because we know that the tree
		// is already in one of our repositories that we have locally, so raising an error
		// now doesn't make a whole lot of sense in the first place.
		//
		// Note: this works because we use `git clone --local`, which only creates a copy of
		// the repository without performing consistency checks.
		_, poolPath, err := createPool(t, &gitalypb.ObjectPool{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: git.NewObjectPoolName(t),
			},
		})
		require.NoError(t, err)

		// Verify that the broken tree is indeed in the pool repository and that it is
		// reported as broken by git-fsck(1).
		var stderr bytes.Buffer
		fsckCmd := git.NewCommand(t, cfg, "-C", poolPath, "fsck")
		fsckCmd.Stderr = &stderr

		require.EqualError(t, fsckCmd.Run(), "exit status 1")
		require.Equal(t, fmt.Sprintf("error in tree %s: duplicateEntries: contains duplicate file entries\n", treeID), stderr.String())
	})
}
