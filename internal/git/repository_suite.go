package git

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

// GetRepositoryFunc is used to get a clean test repository for the different implementations of the
// Repository interface in the common test suite TestRepository.
type GetRepositoryFunc func(t testing.TB, ctx context.Context) (Repository, string)

// TestRepository tests an implementation of Repository.
func TestRepository(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	for _, tc := range []struct {
		desc string
		test func(*testing.T, config.Cfg, GetRepositoryFunc)
	}{
		{
			desc: "ResolveRevision",
			test: testRepositoryResolveRevision,
		},
		{
			desc: "HasBranches",
			test: testRepositoryHasBranches,
		},
		{
			desc: "GetDefaultBranch",
			test: testRepositoryGetDefaultBranch,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tc.test(t, cfg, getRepository)
		})
	}
}

func testRepositoryResolveRevision(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	repo, repoPath := getRepository(t, ctx)

	localRepo := localrepo.NewTestRepo(t, cfg, repo)

	firstParentCommitID := localRepo.WriteTestCommit(t, localrepo.WithMessage("first parent"))
	secondParentCommitID := localRepo.WriteTestCommit(t, localrepo.WithMessage("second parent"))
	masterCommitID := localRepo.WriteTestCommit(t, localrepo.WithBranch("master"), localrepo.WithParents(firstParentCommitID, secondParentCommitID))

	for _, tc := range []struct {
		desc     string
		revision string
		expected ObjectID
	}{
		{
			desc:     "unqualified master branch",
			revision: "master",
			expected: masterCommitID,
		},
		{
			desc:     "fully qualified master branch",
			revision: "refs/heads/master",
			expected: masterCommitID,
		},
		{
			desc:     "typed commit",
			revision: "refs/heads/master^{commit}",
			expected: masterCommitID,
		},
		{
			desc:     "extended SHA notation",
			revision: "refs/heads/master^2",
			expected: secondParentCommitID,
		},
		{
			desc:     "nonexistent branch",
			revision: "refs/heads/foobar",
		},
		{
			desc:     "SHA notation gone wrong",
			revision: "refs/heads/master^3",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := repo.ResolveRevision(ctx, Revision(tc.revision))
			if tc.expected == "" {
				require.Equal(t, err, ErrReferenceNotFound)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, oid)
		})
	}
}

func testRepositoryHasBranches(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	repo, repoPath := getRepository(t, ctx)

	emptyCommit := text.ChompBytes(Exec(t, cfg, "-C", repoPath, "commit-tree", DefaultObjectHash.EmptyTreeOID.String()))

	Exec(t, cfg, "-C", repoPath, "update-ref", "refs/headsbranch", emptyCommit)

	hasBranches, err := repo.HasBranches(ctx)
	require.NoError(t, err)
	require.False(t, hasBranches)

	Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/branch", emptyCommit)

	hasBranches, err = repo.HasBranches(ctx)
	require.NoError(t, err)
	require.True(t, hasBranches)
}

func testRepositoryGetDefaultBranch(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc         string
		repo         func(t *testing.T) Repository
		expectedName ReferenceName
	}{
		{
			desc: "default ref",
			repo: func(t *testing.T) Repository {
				repo, repoPath := getRepository(t, ctx)
				localRepo := localrepo.NewTestRepo(t, cfg, repo)
				oid := localRepo.WriteTestCommit(t, localrepo.WithBranch("apple"))
				localrepo.WriteTestCommit(t, localrepo.WithParents(oid), localrepo.WithBranch("main"))
				return repo
			},
			expectedName: DefaultRef,
		},
		{
			desc: "legacy default ref",
			repo: func(t *testing.T) Repository {
				repo, repoPath := getRepository(t, ctx)
				localRepo := localrepo.NewTestRepo(t, cfg, repo)
				oid := localRepo.WriteTestCommit(t, localrepo.WithBranch("apple"))
				localRepo.WriteTestCommit(t, localrepo.WithParents(oid), localrepo.WithBranch("master"))
				return repo
			},
			expectedName: LegacyDefaultRef,
		},
		{
			desc: "no branches",
			repo: func(t *testing.T) Repository {
				repo, _ := getRepository(t, ctx)
				return repo
			},
		},
		{
			desc: "one branch",
			repo: func(t *testing.T) Repository {
				repo, repoPath := getRepository(t, ctx)
				localRepo := localrepo.NewTestRepo(t, cfg, repo)
				localRepo.WriteTestCommit(t, localrepo.WithBranch("apple"))
				return repo
			},
			expectedName: NewReferenceNameFromBranchName("apple"),
		},
		{
			desc: "no default branches",
			repo: func(t *testing.T) Repository {
				repo, repoPath := getRepository(t, ctx)
				localRepo := localrepo.NewTestRepo(t, cfg, repo)
				oid := localRepo.WriteTestCommit(t, localrepo.WithBranch("apple"))
				localRepo.WriteTestCommit(t, localrepo.WithParents(oid), localrepo.WithBranch("banana"))
				return repo
			},
			expectedName: NewReferenceNameFromBranchName("apple"),
		},
		{
			desc: "test repo HEAD set",
			repo: func(t *testing.T) Repository {
				repo, repoPath := getRepository(t, ctx)
				localRepo := localrepo.NewTestRepo(t, cfg, repo)

				localRepo.WriteTestCommit(t, localrepo.WithBranch("feature"))
				Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

				return repo
			},
			expectedName: NewReferenceNameFromBranchName("feature"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			name, err := tc.repo(t).GetDefaultBranch(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, name)
		})
	}
}
