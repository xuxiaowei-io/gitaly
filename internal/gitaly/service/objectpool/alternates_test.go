//go:build !gitaly_test_sha256

package objectpool

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDisconnectGitAlternates(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.RevlistForConnectivity,
	).Run(
		t,
		testDisconnectGitAlternates,
	)
}

func testDisconnectGitAlternates(t *testing.T, ctx context.Context) {
	cfg, repoProto, repoPath, _, client := setup(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")

	// We create the object pool, link the original repository to it and then repack the pool
	// member. As the linking step should've pulled all objects into the pool, the repack should
	// get rid of the now-duplicate objects in the repository in favor of the pooled ones.
	_, pool, _ := createObjectPool(t, ctx, cfg, client, repoProto)
	require.NoError(t, pool.Link(ctx, repo))
	gittest.Exec(t, cfg, "-C", repoPath, "gc")

	// Corrupt the repository to check that the commit we have created can no longer be read.
	// This is done to ensure that the object really only exists in the pool repository now.
	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)
	require.NoError(t, os.Remove(altPath))
	gittest.RequireObjectNotExists(t, cfg, repoPath, commitID)

	// Recreate the alternates link and assert that we can now read the commit again.
	require.NoError(t, pool.Link(ctx, repo))
	require.FileExists(t, altPath, "objects/info/alternates should be back")
	gittest.RequireObjectExists(t, cfg, repoPath, commitID)

	// At this point we know that the repository has access to the commit, but only if
	// objects/info/alternates is in place.
	_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repoProto})
	require.NoError(t, err)

	// Check that the object can still be found, even though objects/info/alternates is gone.
	// This is the purpose of DisconnectGitAlternates.
	require.NoFileExists(t, altPath)
	gittest.RequireObjectExists(t, cfg, repoPath, commitID)
}

func TestDisconnectGitAlternatesNoAlternates(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.RevlistForConnectivity).Run(
		t,
		testDisconnectGitAlternatesNoAlternates,
	)
}

func testDisconnectGitAlternatesNoAlternates(t *testing.T, ctx context.Context) {
	cfg, repoProto, repoPath, _, client := setup(t, ctx)
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
	cfg, _, _, _, client := setup(t, ctx)

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
			repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			altPath, err := repo.InfoAlternatesPath()
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(altPath, []byte(tc.altContent), perm.SharedFile))

			_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repoProto})
			require.Error(t, err)

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
		cfg, repoProto, repoPath, _, _ := setup(t, ctx)
		srv := server{gitCmdFactory: gittest.NewCommandFactory(t, cfg)}

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
		err = srv.removeAlternatesIfOk(ctx, repo, altPath, altBackup)
		require.Error(t, err, "removeAlternatesIfOk should fail")
		require.IsType(t, &connectivityError{}, err, "error must be because of fsck")

		// We expect objects/info/alternates to have been restored when removeAlternatesIfOk
		// returned.
		assertAlternates(t, altPath, altContent)
		// We expect the backup alternates file to still exist.
		assertAlternates(t, altBackup, altContent)
	})

	t.Run("commit graph exists but object is missing from odb", func(t *testing.T) {
		cfg, repoProto, repoPath, _, _ := setup(t, ctx)
		srv := server{gitCmdFactory: gittest.NewCommandFactory(t, cfg)}
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

func TestDisconnectGitAlternates_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, _, _, _, client := setup(t, ctx)
	for _, tc := range []struct {
		desc        string
		req         *gitalypb.DisconnectGitAlternatesRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			req:  &gitalypb.DisconnectGitAlternatesRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.DisconnectGitAlternates(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func assertAlternates(t *testing.T, altPath string, altContent string) {
	t.Helper()

	actualContent := testhelper.MustReadFile(t, altPath)

	require.Equal(t, altContent, string(actualContent), "%s content after fsck failure", altPath)
}
