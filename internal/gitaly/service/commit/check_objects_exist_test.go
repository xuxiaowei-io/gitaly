//go:build !gitaly_test_sha256

package commit

import (
	"fmt"
	"io"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckObjectsExist(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	logger, hook := test.NewNullLogger()
	cfg, client := setupCommitService(t, ctx, testserver.WithLogger(logger))

	repo, repoPath := git.CreateRepository(t, ctx, cfg)

	commitID1 := WriteTestCommit(t, git, cfg, repoPath,
		git.WithBranch("master"), git.WithMessage("commit-1"))

	commitID2 := git.WriteTestCommit(t, cfg, repoPath,
		git.WithBranch("feature"), git.WithMessage("commit-2"), git.WithParents(commitID1),
	)
	commitID3 := WriteTestCommit(t, git, cfg, repoPath,
		git.WithMessage("commit-3"), git.WithParents(commitID1))

	for _, tc := range []struct {
		desc                  string
		requests              []*gitalypb.CheckObjectsExistRequest
		expectedResults       map[string]bool
		expectedErr           error
		expectedErrorMetadata any
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
			expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
			expectedErrorMetadata: map[string]any{
				"revision": "-not-a-rev",
			},
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
			expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't contain whitespace"),
			expectedErrorMetadata: map[string]any{
				"revision": fmt.Sprintf("%s\n%s", commitID1, commitID2),
			},
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
			expectedErr: structerr.NewInvalidArgument("invalid revision: revision can't start with '-'"),
			expectedErrorMetadata: map[string]any{
				"revision": "-not-a-rev",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			hook.Reset()

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

			if tc.expectedErrorMetadata == nil {
				tc.expectedErrorMetadata = map[string]any{}
			}

			metadata := map[string]any{}
			for _, entry := range hook.AllEntries() {
				errorMetadata, ok := entry.Data["error_metadata"]
				if !ok {
					continue
				}

				for key, value := range errorMetadata.(map[string]any) {
					metadata[key] = value
				}
			}
			require.Equal(t, tc.expectedErrorMetadata, metadata)
		})
	}
}
