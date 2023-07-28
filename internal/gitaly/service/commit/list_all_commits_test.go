package commit

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestListAllCommits(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	type setupData struct {
		request              *gitalypb.ListAllCommitsRequest
		skipCommitValidation bool
		expectedErr          error
		expectedCommitIDs    []git.ObjectID
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "unset repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListAllCommitsRequest{
						Repository: nil,
					},
					skipCommitValidation: true,
					expectedErr:          structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.ListAllCommitsRequest{
						Repository: repo,
					},
				}
			},
		},
		{
			desc: "normal repository",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				reachableCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("reachable"), gittest.WithMessage("reachable"))
				transitivelyReachableCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("transitively reachable"))
				commitWithHistory := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(transitivelyReachableCommit), gittest.WithBranch("commit-with-history"))
				unreachableCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unreachable"))

				return setupData{
					request: &gitalypb.ListAllCommitsRequest{
						Repository: repo,
					},
					expectedCommitIDs: []git.ObjectID{
						transitivelyReachableCommit,
						commitWithHistory,
						reachableCommit,
						unreachableCommit,
					},
				}
			},
		},
		{
			desc: "pagination",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				child := gittest.WriteCommit(t, cfg, repoPath)
				parent := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(child), gittest.WithBranch("branch"))

				// Git walks commits in object ID order, so we expect the smaller of both object IDs to
				// be returned.
				smallerOID := child
				if parent < child {
					smallerOID = parent
				}

				return setupData{
					request: &gitalypb.ListAllCommitsRequest{
						Repository: repo,
						PaginationParams: &gitalypb.PaginationParameter{
							Limit: 1,
						},
					},
					expectedCommitIDs: []git.ObjectID{
						smallerOID,
					},
				}
			},
		},
		{
			desc: "empty quarantine",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unquarantined"))

				quarantineDir := filepath.Join("objects", "incoming-123456")
				require.NoError(t, os.Mkdir(filepath.Join(repoPath, quarantineDir), perm.PublicDir))

				repo.GitObjectDirectory = quarantineDir
				repo.GitAlternateObjectDirectories = nil

				return setupData{
					request: &gitalypb.ListAllCommitsRequest{
						Repository: repo,
					},
				}
			},
		},
		{
			desc: "populated quarantine",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unquarantined"))

				quarantineDir := filepath.Join("objects", "incoming-123456")
				require.NoError(t, os.Mkdir(filepath.Join(repoPath, quarantineDir), perm.PublicDir))

				repo.GitObjectDirectory = quarantineDir
				repo.GitAlternateObjectDirectories = nil

				quarantinedCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithMessage("quarantined"),
					gittest.WithAlternateObjectDirectory(filepath.Join(repoPath, quarantineDir)),
				)

				return setupData{
					request: &gitalypb.ListAllCommitsRequest{
						Repository: repo,
					},
					expectedCommitIDs: []git.ObjectID{
						quarantinedCommit,
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t)

			stream, err := client.ListAllCommits(ctx, setup.request)
			require.NoError(t, err)

			var actualCommits []*gitalypb.GitCommit
			for {
				var response *gitalypb.ListAllCommitsResponse
				response, err = stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}

					break
				}

				actualCommits = append(actualCommits, response.Commits...)
			}
			testhelper.RequireGrpcError(t, setup.expectedErr, err)

			if setup.skipCommitValidation {
				return
			}

			var expectedCommits []*gitalypb.GitCommit
			repo := localrepo.NewTestRepo(t, cfg, setup.request.GetRepository())
			for _, expectedCommitID := range setup.expectedCommitIDs {
				commit, err := repo.ReadCommit(ctx, expectedCommitID.Revision())
				require.NoError(t, err)
				expectedCommits = append(expectedCommits, commit)
			}

			// The order isn't clearly defined, so we simply sort both slices before doing the comparison.
			sort.Slice(expectedCommits, func(i, j int) bool {
				return expectedCommits[i].Id < expectedCommits[j].Id
			})
			sort.Slice(actualCommits, func(i, j int) bool {
				return actualCommits[i].Id < actualCommits[j].Id
			})
			testhelper.ProtoEqual(t, expectedCommits, actualCommits)
		})
	}
}

func BenchmarkListAllCommits(b *testing.B) {
	b.StopTimer()
	ctx := testhelper.Context(b)

	_, repo, _, client := setupCommitServiceWithRepo(b, ctx)

	b.Run("ListAllCommits", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
				Repository: repo,
			})
			require.NoError(b, err)

			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(b, err)
			}
		}
	})
}
