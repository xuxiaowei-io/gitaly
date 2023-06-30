//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestWriteRef(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	txManager := transaction.NewTrackingManager()
	cfg, client := setupRepositoryServiceWithoutRepo(t, testserver.WithTransactionManager(txManager))

	type setupData struct {
		request       *gitalypb.WriteRefRequest
		expectedErr   error
		expectedRefs  []git.Reference
		expectedVotes []transaction.PhasedVote
	}

	votes := func(ref git.ReferenceName, oldID, newID git.ObjectID) []transaction.PhasedVote {
		return []transaction.PhasedVote{
			{
				Phase: voting.Prepared,
				Vote:  voting.VoteFromData([]byte(fmt.Sprintf("%s %s %s\n", oldID, newID, ref))),
			},
			{
				Phase: voting.Committed,
				Vote:  voting.VoteFromData([]byte(fmt.Sprintf("%s %s %s\n", oldID, newID, ref))),
			},
		}
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "empty revision",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Ref:        []byte("refs/heads/master"),
					},
					expectedErr: structerr.NewInvalidArgument("invalid revision: empty revision"),
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
					},
					expectedVotes: []transaction.PhasedVote{},
				}
			},
		},
		{
			desc: "empty ref name",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Revision:   []byte(commitID),
					},
					expectedErr: structerr.NewInvalidArgument("invalid ref: empty revision"),
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
					},
					expectedVotes: []transaction.PhasedVote{},
				}
			},
		},
		{
			desc: "non-prefixed ref name",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Ref:        []byte("master"),
						Revision:   []byte(commitID),
					},
					expectedErr: structerr.NewInvalidArgument("ref has to be a full reference"),
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
					},
					expectedVotes: []transaction.PhasedVote{},
				}
			},
		},
		{
			desc: "revision contains \\x00",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := []byte(gittest.WriteCommit(t, cfg, repoPath).String())
				commitID[10] = '\x00'

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Ref:        []byte("refs/heads/master"),
						Revision:   commitID,
					},
					expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't contain NUL"),
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
					},
					expectedVotes: []transaction.PhasedVote{},
				}
			},
		},
		{
			desc: "ref contains whitespace",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath).String()

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Ref:        []byte("refs/heads /master"),
						Revision:   []byte(commitID),
					},
					expectedErr: structerr.NewInvalidArgument("invalid ref: revision can't contain whitespace"),
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
					},
					expectedVotes: []transaction.PhasedVote{},
				}
			},
		},
		{
			desc: "invalid revision",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Ref:        []byte("refs/heads/master"),
						Revision:   []byte("--output=/meow"),
					},
					expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
					},
					expectedVotes: []transaction.PhasedVote{},
				}
			},
		},
		{
			desc: "update default branch",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				defaultCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("new-default"))

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Ref:        []byte("HEAD"),
						Revision:   []byte("refs/heads/new-default"),
					},
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", "refs/heads/new-default"),
						git.NewReference(git.DefaultRef, defaultCommit),
						git.NewReference("refs/heads/new-default", newCommit),
					},
					expectedVotes: []transaction.PhasedVote{
						{Phase: voting.Prepared, Vote: voting.VoteFromData([]byte("ref: refs/heads/new-default\n"))},
						{Phase: voting.Committed, Vote: voting.VoteFromData([]byte("ref: refs/heads/new-default\n"))},
					},
				}
			},
		},
		{
			desc: "reference update without expected commit ID",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommit))

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository: repo,
						Ref:        []byte("refs/heads/branch"),
						Revision:   []byte(newCommit),
					},
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
						git.NewReference("refs/heads/branch", newCommit),
					},
					expectedVotes: votes("refs/heads/branch", gittest.DefaultObjectHash.ZeroOID, newCommit),
				}
			},
		},
		{
			desc: "reference update with expected commit ID",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommit))

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository:  repo,
						Ref:         []byte("refs/heads/branch"),
						Revision:    []byte(newCommit),
						OldRevision: []byte(oldCommit),
					},
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
						git.NewReference("refs/heads/branch", newCommit),
					},
					expectedVotes: votes("refs/heads/branch", oldCommit, newCommit),
				}
			},
		},
		{
			desc: "reference creation with expected commit ID",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository:  repo,
						Ref:         []byte("refs/heads/branch"),
						Revision:    []byte(commitID),
						OldRevision: []byte(gittest.DefaultObjectHash.ZeroOID),
					},
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
						git.NewReference("refs/heads/branch", commitID),
					},
					expectedVotes: votes("refs/heads/branch", gittest.DefaultObjectHash.ZeroOID, commitID),
				}
			},
		},
		{
			desc: "reference deletion with expected commit ID",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))

				return setupData{
					request: &gitalypb.WriteRefRequest{
						Repository:  repo,
						Ref:         []byte("refs/heads/branch"),
						Revision:    []byte(gittest.DefaultObjectHash.ZeroOID),
						OldRevision: []byte(commitID),
					},
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.DefaultRef),
					},
					expectedVotes: votes("refs/heads/branch", commitID, gittest.DefaultObjectHash.ZeroOID),
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t)

			txManager.Reset()

			ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
			require.NoError(t, err)
			ctx = metadata.IncomingToOutgoing(ctx)

			_, err = client.WriteRef(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)

			repo := localrepo.NewTestRepo(t, cfg, setup.request.GetRepository())
			refs, err := repo.GetReferences(ctx)
			require.NoError(t, err)
			defaultBranch, err := repo.HeadReference(ctx)
			require.NoError(t, err)
			require.Equal(t, setup.expectedRefs, append([]git.Reference{
				git.NewSymbolicReference("HEAD", defaultBranch),
			}, refs...))

			require.Equal(t, setup.expectedVotes, txManager.Votes())
		})
	}
}

func TestWriteRef_missingRevisions(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.WriteRefRequest
		expectedErr error
	}{
		{
			desc: "revision refers to missing reference",
			request: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/main"),
				Revision:   []byte("refs/heads/missing"),
			},
			expectedErr: structerr.NewInternal("resolving new revision: reference not found"),
		},
		{
			desc: "revision refers to missing object",
			request: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/main"),
				Revision:   bytes.Repeat([]byte("1"), gittest.DefaultObjectHash.EncodedLen()),
			},
			expectedErr: structerr.NewInternal("resolving new revision: reference not found"),
		},
		{
			desc: "old revision refers to missing reference",
			request: &gitalypb.WriteRefRequest{
				Repository:  repo,
				Ref:         []byte("refs/heads/main"),
				Revision:    []byte(commitID),
				OldRevision: bytes.Repeat([]byte("1"), gittest.DefaultObjectHash.EncodedLen()),
			},
			expectedErr: structerr.NewInternal("resolving old revision: reference not found"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.WriteRef(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
