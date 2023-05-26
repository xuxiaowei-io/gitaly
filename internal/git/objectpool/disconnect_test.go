package objectpool

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestRemoveAlternatesIfOk(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	assertAlternates := func(t *testing.T, altPath string, altContent string) {
		t.Helper()

		actualContent := testhelper.MustReadFile(t, altPath)

		require.Equal(t, altContent, string(actualContent), "%s content after fsck failure", altPath)
	}

	t.Run("pack files are missing", func(t *testing.T) {
		cfg := testcfg.Build(t)
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{SkipCreationViaService: true})

		repo := localrepo.NewTestRepo(t, cfg, repoProto)
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
		gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")

		// Change the alternates file to point to an empty directory. This is only done to
		// assert that we correctly restore the file if the repository doesn't pass the
		// consistency checks when trying to remove the alternates file.
		altPath, err := repo.InfoAlternatesPath()
		require.NoError(t, err)
		altContent := testhelper.TempDir(t) + "\n"
		require.NoError(t, os.WriteFile(altPath, []byte(altContent), perm.SharedFile))

		// Intentionally break the repository so that the consistency check will cause an
		// error.
		require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects", "pack")))

		// Now we try to remove the alternates file. This is expected to fail due to the
		// consistency check.
		altBackup := altPath + ".backup"
		err = removeAlternatesIfOk(ctx, repo, altPath, altBackup)
		require.Error(t, err, "removeAlternatesIfOk should fail")
		require.IsType(t, &connectivityError{}, err, "error must be because of fsck")

		// We expect objects/info/alternates to have been restored when removeAlternatesIfOk
		// returned.
		assertAlternates(t, altPath, altContent)
		// We expect the backup alternates file to still exist.
		assertAlternates(t, altBackup, altContent)
	})

	t.Run("commit graph exists but object is missing from odb", func(t *testing.T) {
		cfg := testcfg.Build(t)
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{SkipCreationViaService: true})

		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		altPath, err := repo.InfoAlternatesPath()
		require.NoError(t, err)
		altContent := testhelper.TempDir(t) + "\n"
		require.NoError(t, os.WriteFile(altPath, []byte(altContent), perm.SharedFile))

		// In order to test the scenario where a commit is in a commit graph but not in the
		// object database, we will first write a new commit, write the commit graph, then
		// remove that commit object from the object database.
		parentOID := gittest.WriteCommit(t, cfg, repoPath)
		commitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(parentOID), gittest.WithBranch("main"))
		gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write")

		// We now manually remove the object. It thus exists in the commit-graph, but not in
		// the ODB anymore while still being reachable. We should notice that the repository
		// is corrupted.
		require.NoError(t, os.Remove(filepath.Join(repoPath, "objects", string(commitOID)[0:2], string(commitOID)[2:])))

		// Now when we try to remove the alternates file we should notice the corruption and
		// abort.
		altBackup := altPath + ".backup"
		err = removeAlternatesIfOk(ctx, repo, altPath, altBackup)
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
