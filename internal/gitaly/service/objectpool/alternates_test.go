//go:build !gitaly_test_sha256

package objectpool

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestDisconnectGitAlternates(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.RevlistForConnectivity).Run(
		t,
		testDisconnectGitAlternates,
	)
}

func testDisconnectGitAlternates(t *testing.T, ctx context.Context) {
	cfg, repoProto, repoPath, _, client := setup(ctx, t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	pool := initObjectPool(t, cfg, cfg.Storages[0])
	require.NoError(t, pool.Create(ctx, repo))
	require.NoError(t, pool.Link(ctx, repo))
	gittest.Exec(t, cfg, "-C", repoPath, "gc")

	existingObjectID := "55bc176024cfa3baaceb71db584c7e5df900ea65"

	// Corrupt the repository to check that existingObjectID can no longer be found
	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err, "find info/alternates")
	require.NoError(t, os.RemoveAll(altPath))

	cmd, err := gitCmdFactory.New(ctx, repo,
		git.SubCmd{Name: "cat-file", Flags: []git.Option{git.Flag{Name: "-e"}}, Args: []string{existingObjectID}})
	require.NoError(t, err)
	require.Error(t, cmd.Wait(), "expect cat-file to fail because object cannot be found")

	require.NoError(t, pool.Link(ctx, repo))
	require.FileExists(t, altPath, "objects/info/alternates should be back")

	// At this point we know that the repository has access to
	// existingObjectID, but only if objects/info/alternates is in place.

	_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repoProto})
	require.NoError(t, err, "call DisconnectGitAlternates")

	// Check that the object can still be found, even though
	// objects/info/alternates is gone. This is the purpose of
	// DisconnectGitAlternates.
	require.NoFileExists(t, altPath)
	gittest.Exec(t, cfg, "-C", repoPath, "cat-file", "-e", existingObjectID)
}

func TestDisconnectGitAlternatesNoAlternates(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.RevlistForConnectivity).Run(
		t,
		testDisconnectGitAlternatesNoAlternates,
	)
}

func testDisconnectGitAlternatesNoAlternates(t *testing.T, ctx context.Context) {
	cfg, repoProto, repoPath, _, client := setup(ctx, t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err, "find info/alternates")
	require.NoFileExists(t, altPath)

	_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repoProto})
	require.NoError(t, err, "call DisconnectGitAlternates on repository without alternates")

	gittest.Exec(t, cfg, "-C", repoPath, "fsck")
}

func TestDisconnectGitAlternatesUnexpectedAlternates(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.RevlistForConnectivity).Run(
		t,
		testDisconnectGitAlternatesUnexpectedAlternates,
	)
}

func testDisconnectGitAlternatesUnexpectedAlternates(t *testing.T, ctx context.Context) {
	cfg, _, _, _, client := setup(ctx, t)

	testCases := []struct {
		desc       string
		altContent string
	}{
		{desc: "multiple alternates", altContent: "/foo/bar\n/qux/baz\n"},
		{desc: "directory not found", altContent: "/does/not/exist/\n"},
		{desc: "not a directory", altContent: "../HEAD\n"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			altPath, err := repo.InfoAlternatesPath()
			require.NoError(t, err, "find info/alternates")

			require.NoError(t, os.WriteFile(altPath, []byte(tc.altContent), 0o644))

			_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repoProto})
			require.Error(t, err, "call DisconnectGitAlternates on repository with unexpected objects/info/alternates")

			contentAfterRPC := testhelper.MustReadFile(t, altPath)
			require.Equal(t, tc.altContent, string(contentAfterRPC), "objects/info/alternates content should not have changed")
		})
	}
}

func TestRemoveAlternatesIfOk(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.RevlistForConnectivity).Run(
		t,
		testRemoveAlternatesIfOk,
	)
}

func testRemoveAlternatesIfOk(t *testing.T, ctx context.Context) {
	t.Run("pack files are missing", func(t *testing.T) {
		cfg, repoProto, repoPath, _, _ := setup(ctx, t)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		altPath, err := repo.InfoAlternatesPath()
		require.NoError(t, err, "find info/alternates")
		altContent := "/var/empty\n"
		require.NoError(t, os.WriteFile(altPath, []byte(altContent), 0o644), "write alternates file")

		// Intentionally break the repository, so that 'git fsck' will fail later.
		testhelper.MustRunCommand(t, nil, "sh", "-c", fmt.Sprintf("rm %s/objects/pack/*.pack", repoPath))

		altBackup := altPath + ".backup"

		srv := server{gitCmdFactory: gittest.NewCommandFactory(t, cfg)}
		err = srv.removeAlternatesIfOk(ctx, repo, altPath, altBackup)
		require.Error(t, err, "removeAlternatesIfOk should fail")
		require.IsType(t, &connectivityError{}, err, "error must be because of fsck")

		// We expect objects/info/alternates to have been restored when
		// removeAlternatesIfOk returned.
		assertAlternates(t, altPath, altContent)

		// We expect the backup alternates file to still exist.
		assertAlternates(t, altBackup, altContent)
	})

	t.Run("commit graph exists but object is missing from odb", func(t *testing.T) {
		cfg, repoProto, repoPath, _, _ := setup(ctx, t)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		altPath, err := repo.InfoAlternatesPath()
		require.NoError(t, err, "find info/alternates")

		tmpDir := testhelper.TempDir(t)
		altContent := tmpDir + "\n"
		require.NoError(t, os.WriteFile(altPath, []byte(altContent), 0o644), "write alternates file")

		// In order to test the scenario where a commit is in a commit
		// graph but not in the object database, we will first write a
		// new commit, write the commit graph, then remove that commit
		// object from the object database.
		headCommitOid := gittest.ResolveRevision(t, cfg, repoPath, "HEAD")
		commitOid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(headCommitOid), gittest.WithBranch("master"))

		gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write")

		require.NoError(t, os.Remove(filepath.Join(repoPath, "objects", string(commitOid)[0:2], string(commitOid)[2:])))

		altBackup := altPath + ".backup"

		srv := server{gitCmdFactory: gittest.NewCommandFactory(t, cfg)}
		err = srv.removeAlternatesIfOk(ctx, repo, altPath, altBackup)
		require.Error(t, err, "removeAlternatesIfOk should fail")

		require.IsType(t, &connectivityError{}, err, "error must be because of connectivity check")

		connectivityErr := err.(*connectivityError)
		require.IsType(t, &exec.ExitError{}, connectivityErr.error, "error must be because of fsck")

		// We expect objects/info/alternates to have been restored when
		// removeAlternatesIfOk returned.
		assertAlternates(t, altPath, altContent)

		// We expect the backup alternates file to still exist.
		assertAlternates(t, altBackup, altContent)
	})
}

func assertAlternates(t *testing.T, altPath string, altContent string) {
	t.Helper()

	actualContent := testhelper.MustReadFile(t, altPath)

	require.Equal(t, altContent, string(actualContent), "%s content after fsck failure", altPath)
}
