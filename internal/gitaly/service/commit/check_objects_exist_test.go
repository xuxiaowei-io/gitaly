//go:build !gitaly_test_sha256

package commit

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckObjectsExist(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	commitID1 := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("master"), gittest.WithMessage("commit-1"),
	)
	commitID2 := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("feature"), gittest.WithMessage("commit-2"), gittest.WithParents(commitID1),
	)
	commitID3 := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("commit-3"), gittest.WithParents(commitID1),
	)

	for _, tc := range []struct {
		desc            string
		requests        []*gitalypb.CheckObjectsExistRequest
		expectedResults map[string]bool
		expectedErr     error
	}{
		{
			desc:     "no repository provided",
			requests: []*gitalypb.CheckObjectsExistRequest{{Repository: nil}},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:     "no requests",
			requests: []*gitalypb.CheckObjectsExistRequest{},
			// Ideally, we'd return an invalid-argument error in case there aren't any
			// requests. We can't do this though as this would diverge from Praefect's
			// behaviour, which always returns `io.EOF`.
			expectedErr: status.Error(codes.Internal, io.EOF.Error()),
		},
		{
			desc: "missing repository",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Revisions: [][]byte{},
				},
			},
			expectedErr: func() error {
				if testhelper.IsPraefectEnabled() {
					return structerr.NewInvalidArgument("repo scoped: empty Repository")
				}
				return structerr.NewInvalidArgument("empty Repository")
			}(),
		},
		{
			desc: "request without revisions",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
				},
			},
		},
		{
			desc: "commit ids and refs that exist",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
						[]byte("master"),
						[]byte(commitID2),
						[]byte(commitID3),
						[]byte("feature"),
					},
				},
			},
			expectedResults: map[string]bool{
				commitID1.String(): true,
				"master":           true,
				commitID2.String(): true,
				commitID3.String(): true,
				"feature":          true,
			},
		},
		{
			desc: "ref and objects missing",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
						[]byte("master"),
						[]byte(commitID2),
						[]byte(commitID3),
						[]byte("feature"),
						[]byte("refs/does/not/exist"),
					},
				},
			},
			expectedResults: map[string]bool{
				commitID1.String():    true,
				"master":              true,
				commitID2.String():    true,
				commitID3.String():    true,
				"feature":             true,
				"refs/does/not/exist": false,
			},
		},
		{
			desc: "chunked input",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
					},
				},
				{
					Revisions: [][]byte{
						[]byte(commitID2),
					},
				},
				{
					Revisions: [][]byte{
						[]byte("refs/does/not/exist"),
					},
				},
				{
					Revisions: [][]byte{
						[]byte(commitID3),
					},
				},
			},
			expectedResults: map[string]bool{
				commitID1.String():    true,
				commitID2.String():    true,
				commitID3.String():    true,
				"refs/does/not/exist": false,
			},
		},
		{
			desc: "invalid input",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte("-not-a-rev"),
					},
				},
			},
			expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't start with '-'").
				WithInterceptedMetadata("revision", "-not-a-rev"),
		},
		{
			desc: "input with whitespace",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(fmt.Sprintf("%s\n%s", commitID1, commitID2)),
					},
				},
			},
			expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't contain whitespace").
				WithInterceptedMetadata("revision", fmt.Sprintf("%s\n%s", commitID1, commitID2)),
		},
		{
			desc: "chunked invalid input",
			requests: []*gitalypb.CheckObjectsExistRequest{
				{
					Repository: repo,
					Revisions: [][]byte{
						[]byte(commitID1),
					},
				},
				{
					Revisions: [][]byte{
						[]byte("-not-a-rev"),
					},
				},
			},
			expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't start with '-'").
				WithInterceptedMetadata("revision", "-not-a-rev"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			client, err := client.CheckObjectsExist(ctx)
			require.NoError(t, err)

			for _, request := range tc.requests {
				require.NoError(t, client.Send(request))
			}
			require.NoError(t, client.CloseSend())

			var results map[string]bool
			for {
				var response *gitalypb.CheckObjectsExistResponse
				response, err = client.Recv()
				if err != nil {
					break
				}

				for _, revision := range response.GetRevisions() {
					if results == nil {
						results = map[string]bool{}
					}
					results[string(revision.GetName())] = revision.GetExists()
				}
			}

			if tc.expectedErr == nil {
				tc.expectedErr = io.EOF
			}

			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedResults, results)
		})
	}
}
