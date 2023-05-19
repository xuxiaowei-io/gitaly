//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestWriteRef_successful(t *testing.T) {
	t.Parallel()

	txManager := transaction.NewTrackingManager()
	cfg, repo, repoPath, client := setupRepositoryService(t, testhelper.Context(t), testserver.WithTransactionManager(txManager))

	testCases := []struct {
		desc          string
		req           *gitalypb.WriteRefRequest
		expectedVotes []transaction.PhasedVote
	}{
		{
			desc: "shell update HEAD to refs/heads/master",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("HEAD"),
				Revision:   []byte("refs/heads/master"),
			},
			expectedVotes: []transaction.PhasedVote{
				{
					Phase: voting.Prepared,
					Vote:  voting.Vote{0xac, 0xba, 0xef, 0x27, 0x5e, 0x46, 0xa7, 0xf1, 0x4c, 0x1e, 0xf4, 0x56, 0xff, 0xf2, 0xc8, 0xbb, 0xe8, 0xc8, 0x47, 0x24},
				},
				{
					Phase: voting.Committed,
					Vote:  voting.Vote{0xac, 0xba, 0xef, 0x27, 0x5e, 0x46, 0xa7, 0xf1, 0x4c, 0x1e, 0xf4, 0x56, 0xff, 0xf2, 0xc8, 0xbb, 0xe8, 0xc8, 0x47, 0x24},
				},
			},
		},
		{
			desc: "shell update refs/heads/master",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/master"),
				Revision:   []byte("b83d6e391c22777fca1ed3012fce84f633d7fed0"),
			},
			expectedVotes: []transaction.PhasedVote{
				{
					Phase: voting.Prepared,
					Vote:  voting.Vote{0xd4, 0x6c, 0x98, 0x49, 0xde, 0x17, 0x55, 0xae, 0xb, 0x48, 0x7f, 0xac, 0x57, 0x77, 0x7d, 0xae, 0xb0, 0xf9, 0x64, 0x72},
				},
				{
					Phase: voting.Committed,
					Vote:  voting.Vote{0xd4, 0x6c, 0x98, 0x49, 0xde, 0x17, 0x55, 0xae, 0xb, 0x48, 0x7f, 0xac, 0x57, 0x77, 0x7d, 0xae, 0xb0, 0xf9, 0x64, 0x72},
				},
			},
		},
		{
			desc: "shell update refs/heads/master w/ validation",
			req: &gitalypb.WriteRefRequest{
				Repository:  repo,
				Ref:         []byte("refs/heads/master"),
				Revision:    []byte("498214de67004b1da3d820901307bed2a68a8ef6"),
				OldRevision: []byte("b83d6e391c22777fca1ed3012fce84f633d7fed0"),
			},
			expectedVotes: []transaction.PhasedVote{
				{
					Phase: voting.Prepared,
					Vote:  voting.Vote{0xcb, 0xfc, 0x7f, 0x79, 0x80, 0x7a, 0x33, 0x8e, 0xd5, 0x6a, 0xb7, 0x8e, 0x7f, 0xf8, 0xe, 0xf8, 0x8, 0xb5, 0x52, 0x1a},
				},
				{
					Phase: voting.Committed,
					Vote:  voting.Vote{0xcb, 0xfc, 0x7f, 0x79, 0x80, 0x7a, 0x33, 0x8e, 0xd5, 0x6a, 0xb7, 0x8e, 0x7f, 0xf8, 0xe, 0xf8, 0x8, 0xb5, 0x52, 0x1a},
				},
			},
		},
		{
			desc: "race-free creation of reference",
			req: &gitalypb.WriteRefRequest{
				Repository:  repo,
				Ref:         []byte("refs/heads/branch"),
				Revision:    []byte("498214de67004b1da3d820901307bed2a68a8ef6"),
				OldRevision: []byte("0000000000000000000000000000000000000000"),
			},
			expectedVotes: []transaction.PhasedVote{
				{
					Phase: voting.Prepared,
					Vote:  voting.Vote{0xb0, 0xcc, 0xb4, 0x9a, 0x88, 0xc3, 0x67, 0x63, 0xd0, 0xfb, 0x94, 0xd5, 0x84, 0x43, 0x67, 0x18, 0x1e, 0xdb, 0xa0, 0x1e},
				},
				{
					Phase: voting.Committed,
					Vote:  voting.Vote{0xb0, 0xcc, 0xb4, 0x9a, 0x88, 0xc3, 0x67, 0x63, 0xd0, 0xfb, 0x94, 0xd5, 0x84, 0x43, 0x67, 0x18, 0x1e, 0xdb, 0xa0, 0x1e},
				},
			},
		},
		{
			desc: "race-free delete of reference",
			req: &gitalypb.WriteRefRequest{
				Repository:  repo,
				Ref:         []byte("refs/heads/branch"),
				Revision:    []byte("0000000000000000000000000000000000000000"),
				OldRevision: []byte("498214de67004b1da3d820901307bed2a68a8ef6"),
			},
			expectedVotes: []transaction.PhasedVote{
				{
					Phase: voting.Prepared,
					Vote:  voting.Vote{0xa8, 0x2f, 0xc7, 0x22, 0xac, 0x82, 0xe1, 0x12, 0xb7, 0x78, 0xa6, 0x3f, 0xd6, 0x4b, 0x68, 0xc0, 0x12, 0xb6, 0x17, 0x8c},
				},
				{
					Phase: voting.Committed,
					Vote:  voting.Vote{0xa8, 0x2f, 0xc7, 0x22, 0xac, 0x82, 0xe1, 0x12, 0xb7, 0x78, 0xa6, 0x3f, 0xd6, 0x4b, 0x68, 0xc0, 0x12, 0xb6, 0x17, 0x8c},
				},
			},
		},
	}

	ctx, err := txinfo.InjectTransaction(testhelper.Context(t), 1, "node", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			txManager.Reset()
			_, err = client.WriteRef(ctx, tc.req)
			require.NoError(t, err)

			require.Equal(t, tc.expectedVotes, txManager.Votes())

			if bytes.Equal(tc.req.Ref, []byte("HEAD")) {
				content := testhelper.MustReadFile(t, filepath.Join(repoPath, "HEAD"))

				refRevision := bytes.Join([][]byte{[]byte("ref: "), tc.req.Revision, []byte("\n")}, nil)

				require.EqualValues(t, refRevision, content)
				return
			}

			revParseCmd := gittest.NewCommand(t, cfg, "-C", repoPath, "rev-parse", "--verify", string(tc.req.Ref))
			output, err := revParseCmd.CombinedOutput()

			if git.ObjectHashSHA1.IsZeroOID(git.ObjectID(tc.req.GetRevision())) {
				// If the new OID is the all-zeroes object ID then it indicates we
				// should delete the branch. It's thus expected to get an error when
				// we try to resolve the reference here given that it should not
				// exist anymore.
				require.Error(t, err)
				require.Equal(t, "fatal: Needed a single revision\n", string(output))
			} else {
				require.NoError(t, err)
				require.Equal(t, string(tc.req.Revision), text.ChompBytes(output))
			}
		})
	}
}

func TestWriteRef_validation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(t, ctx)

	testCases := []struct {
		desc string
		req  *gitalypb.WriteRefRequest
	}{
		{
			desc: "empty revision",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/master"),
			},
		},
		{
			desc: "empty ref name",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Revision:   []byte("498214de67004b1da3d820901307bed2a68a8ef6"),
			},
		},
		{
			desc: "non-prefixed ref name for shell",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("master"),
				Revision:   []byte("498214de67004b1da3d820901307bed2a68a8ef6"),
			},
		},
		{
			desc: "revision contains \\x00",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/master"),
				Revision:   []byte("012301230123\x001243"),
			},
		},
		{
			desc: "ref contains \\x00",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/head\x00s/master\x00"),
				Revision:   []byte("0123012301231243"),
			},
		},
		{
			desc: "ref contains whitespace",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/heads /master"),
				Revision:   []byte("0123012301231243"),
			},
		},
		{
			desc: "invalid revision",
			req: &gitalypb.WriteRefRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/master"),
				Revision:   []byte("--output=/meow"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.WriteRef(ctx, tc.req)

			testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
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
