//go:build !gitaly_test_sha256

package objectpool

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestFetchFromOrigin_dangling(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, pool, repo := setupObjectPool(t, ctx)
	poolPath := git.RepositoryPath(t, pool)
	repoPath := git.RepositoryPath(t, repo)

	// Write some reachable objects into the object pool member and fetch them into the pool.
	blobID := git.WriteBlob(t, cfg, repoPath, []byte("contents"))
	treeID := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
		{Mode: "100644", OID: blobID, Path: "reachable"},
	})
	commitID := git.WriteTestCommit(t, cfg, repoPath,
		git.WithTree(treeID),
		git.WithBranch("master"),
	)
	require.NoError(t, pool.FetchFromOrigin(ctx, repo))

	// We now write a bunch of objects into the object pool that are not referenced by anything.
	// These are thus "dangling".
	unreachableBlob := git.WriteBlob(t, cfg, poolPath, []byte("unreachable"))
	unreachableTree := git.WriteTree(t, cfg, poolPath, []git.TreeEntry{
		{Mode: "100644", OID: blobID, Path: "unreachable"},
	})
	unreachableCommit := git.WriteTestCommit(t, cfg, poolPath,
		git.WithMessage("unreachable"),
		git.WithTree(treeID),
	)
	unreachableTag := git.WriteTag(t, cfg, poolPath, "unreachable", commitID.Revision(), git.WriteTagConfig{
		Message: "unreachable",
	})
	// `WriteTag()` automatically creates a reference and thus makes the annotated tag
	// reachable. We thus delete the reference here again.
	git.Exec(t, cfg, "-C", poolPath, "update-ref", "-d", "refs/tags/unreachable")

	// git-fsck(1) should report the newly created unreachable objects as dangling.
	fsckBefore := git.Exec(t, cfg, "-C", poolPath, "fsck", "--connectivity-only", "--dangling")
	require.Equal(t, strings.Join([]string{
		fmt.Sprintf("dangling blob %s", unreachableBlob),
		fmt.Sprintf("dangling tag %s", unreachableTag),
		fmt.Sprintf("dangling commit %s", unreachableCommit),
		fmt.Sprintf("dangling tree %s", unreachableTree),
	}, "\n"), text.ChompBytes(fsckBefore))

	// We expect this second run to convert the dangling objects into non-dangling objects.
	require.NoError(t, pool.FetchFromOrigin(ctx, repo))

	// Each of the dangling objects should have gotten a new dangling reference.
	danglingRefs := git.Exec(t, cfg, "-C", poolPath, "for-each-ref", "--format=%(refname) %(objectname)", "refs/dangling/")
	require.Equal(t, strings.Join([]string{
		fmt.Sprintf("refs/dangling/%[1]s %[1]s", unreachableBlob),
		fmt.Sprintf("refs/dangling/%[1]s %[1]s", unreachableTree),
		fmt.Sprintf("refs/dangling/%[1]s %[1]s", unreachableTag),
		fmt.Sprintf("refs/dangling/%[1]s %[1]s", unreachableCommit),
	}, "\n"), text.ChompBytes(danglingRefs))
	// And git-fsck(1) shouldn't report the objects as dangling anymore.
	require.Empty(t, git.Exec(t, cfg, "-C", poolPath, "fsck", "--connectivity-only", "--dangling"))
}

func TestFetchFromOrigin_fsck(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, pool, repo := setupObjectPool(t, ctx)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	require.NoError(t, pool.FetchFromOrigin(ctx, repo), "seed pool")

	// We're creating a new commit which has a root tree with duplicate entries. git-mktree(1)
	// allows us to create these trees just fine, but git-fsck(1) complains.
	git.WriteTestCommit(t, cfg, repoPath,
		git.WithTreeEntries(
			git.TreeEntry{OID: "4b825dc642cb6eb9a060e54bf8d69288fbee4904", Path: "dup", Mode: "040000"},
			git.TreeEntry{OID: "4b825dc642cb6eb9a060e54bf8d69288fbee4904", Path: "dup", Mode: "040000"},
		),
		git.WithBranch("branch"),
	)

	err = pool.FetchFromOrigin(ctx, repo)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicateEntries: contains duplicate file entries")
}

func TestFetchFromOrigin_deltaIslands(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, pool, repo := setupObjectPool(t, ctx)
	poolPath := git.RepositoryPath(t, pool)
	repoPath := git.RepositoryPath(t, repo)

	require.NoError(t, pool.FetchFromOrigin(ctx, repo), "seed pool")
	require.NoError(t, pool.Link(ctx, repo))

	// The setup of delta islands is done in the normal repository, and thus we pass `false`
	// for `isPoolRepo`. Verification whether we correctly handle repacking though happens in
	// the pool repository.
	git.TestDeltaIslands(t, cfg, repo, pool, false, func() error {
		return pool.FetchFromOrigin(ctx, repo)
	})
}

func TestFetchFromOrigin_bitmapHashCache(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, pool, repo := setupObjectPool(t, ctx)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	git.WriteTestCommit(t, cfg, repoPath, git.WithBranch("master"))

	require.NoError(t, pool.FetchFromOrigin(ctx, repo))

	bitmaps, err := filepath.Glob(git.RepositoryPath(t, pool, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.Len(t, bitmaps, 1)

	git.TestBitmapHasHashcache(t, bitmaps[0])
}

func TestFetchFromOrigin_refUpdates(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, pool, repo := setupObjectPool(t, ctx)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	poolPath := git.RepositoryPath(t, pool)

	// Seed the pool member with some preliminary data.
	oldRefs := map[string]git.ObjectID{}
	oldRefs["heads/csv"] = git.WriteTestCommit(t, cfg, repoPath, git.WithBranch("csv"), git.WithMessage("old"))
	oldRefs["tags/v1.1.0"] = git.WriteTag(t, cfg, repoPath, "v1.1.0", oldRefs["heads/csv"].Revision())

	// We now fetch that data into the object pool and verify that it exists as expected.
	require.NoError(t, pool.FetchFromOrigin(ctx, repo))
	for ref, oid := range oldRefs {
		require.Equal(t, oid, git.ResolveRevision(t, cfg, poolPath, "refs/remotes/origin/"+ref))
	}

	// Next, we force-overwrite both old references with new objects.
	newRefs := map[string]git.ObjectID{}
	newRefs["heads/csv"] = git.WriteTestCommit(t, cfg, repoPath, git.WithBranch("csv"), git.WithMessage("new"))
	newRefs["tags/v1.1.0"] = git.WriteTag(t, cfg, repoPath, "v1.1.0", newRefs["heads/csv"].Revision(), git.WriteTagConfig{
		Force: true,
	})

	// Create a bunch of additional references. This is to trigger OptimizeRepository to indeed
	// repack the loose references as we expect it to in this test. It's debatable whether we
	// should test this at all here given that this is business of the housekeeping package. But
	// it's easy enough to do, so it doesn't hurt.
	for i := 0; i < 32; i++ {
		branchName := fmt.Sprintf("branch-%d", i)
		newRefs["heads/"+branchName] = git.WriteTestCommit(t, cfg, repoPath,
			git.WithMessage(strconv.Itoa(i)),
			git.WithBranch(branchName),
		)
	}

	// Now we fetch again and verify that all references should have been updated accordingly.
	require.NoError(t, pool.FetchFromOrigin(ctx, repo))
	for ref, oid := range newRefs {
		require.Equal(t, oid, git.ResolveRevision(t, cfg, poolPath, "refs/remotes/origin/"+ref))
	}
}

func TestFetchFromOrigin_refs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, pool, repo := setupObjectPool(t, ctx)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	// Verify that the object pool ain't yet got any references.
	poolPath := git.RepositoryPath(t, pool)
	require.Empty(t, git.Exec(t, cfg, "-C", poolPath, "for-each-ref", "--format=%(refname)"))

	// Initialize the repository with a bunch of references.
	commitID := git.WriteTestCommit(t, cfg, repoPath)
	for _, ref := range []git.ReferenceName{"refs/heads/master", "refs/environments/1", "refs/tags/lightweight-tag"} {
		git.WriteRef(t, cfg, repoPath, ref, commitID)
	}
	git.WriteTag(t, cfg, repoPath, "annotated-tag", commitID.Revision(), git.WriteTagConfig{
		Message: "tag message",
	})

	// Fetch from the pool member. This should pull in all references we have just created in
	// that repository into the pool.
	require.NoError(t, pool.FetchFromOrigin(ctx, repo))
	require.Equal(t,
		[]string{
			"refs/remotes/origin/environments/1",
			"refs/remotes/origin/heads/master",
			"refs/remotes/origin/tags/annotated-tag",
			"refs/remotes/origin/tags/lightweight-tag",
		},
		strings.Split(text.ChompBytes(git.Exec(t, cfg, "-C", poolPath, "for-each-ref", "--format=%(refname)")), "\n"),
	)

	// We don't want to see "FETCH_HEAD" though: it's useless and may take quite some time to
	// write out in Git.
	require.NoFileExists(t, filepath.Join(poolPath, "FETCH_HEAD"))
}

func TestFetchFromOrigin_missingPool(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, pool, repo := setupObjectPool(t, ctx)

	// Remove the object pool to assert that we raise an error when fetching into a non-existent
	// object pool.
	require.NoError(t, pool.Remove(ctx))

	require.Equal(t, structerr.NewInvalidArgument("object pool does not exist"), pool.FetchFromOrigin(ctx, repo))
	require.False(t, pool.Exists())
}

func TestObjectPool_logStats(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc           string
		setup          func(t *testing.T) *ObjectPool
		expectedFields logrus.Fields
	}{
		{
			desc: "empty object pool",
			setup: func(t *testing.T) *ObjectPool {
				_, pool, _ := setupObjectPool(t, ctx)
				return pool
			},
			expectedFields: logrus.Fields{
				"references.dangling": referencedObjectTypes{},
				"references.normal":   referencedObjectTypes{},
				"repository_info":     stats.RepositoryInfo{},
			},
		},
		{
			desc: "normal reference",
			setup: func(t *testing.T) *ObjectPool {
				cfg, pool, _ := setupObjectPool(t, ctx)
				git.WriteTestCommit(t, cfg, git.RepositoryPath(t, pool), git.WithBranch("main"))
				return pool
			},
			expectedFields: logrus.Fields{
				"references.dangling": referencedObjectTypes{},
				"references.normal": referencedObjectTypes{
					Commits: 1,
				},
				"repository_info": stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 2,
						Size:  142,
					},
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
				},
			},
		},
		{
			desc: "dangling reference",
			setup: func(t *testing.T) *ObjectPool {
				cfg, pool, _ := setupObjectPool(t, ctx)
				pool.WriteTestCommit(t, localrepo.WithReference("refs/dangling/commit"))
				return pool
			},
			expectedFields: logrus.Fields{
				"references.dangling": referencedObjectTypes{
					Commits: 1,
				},
				"references.normal": referencedObjectTypes{},
				"repository_info": stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 2,
						Size:  142,
					},
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			pool := tc.setup(t)

			require.NoError(t, pool.logStats(ctx, logrus.NewEntry(logger)))

			logEntries := hook.AllEntries()
			require.Len(t, logEntries, 1)
			require.Equal(t, "pool dangling ref stats", logEntries[0].Message)
			require.Equal(t, tc.expectedFields, logEntries[0].Data)
		})
	}
}
