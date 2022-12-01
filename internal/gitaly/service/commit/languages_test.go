//go:build !gitaly_test_sha256

package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLanguages(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	cfg.SocketPath = startTestServices(t, cfg)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	client := newCommitServiceClient(t, cfg.SocketPath)

	request := &gitalypb.CommitLanguagesRequest{
		Repository: repo,
		Revision:   []byte("cb19058ecc02d01f8e4290b7e79cafd16a8839b6"),
	}

	resp, err := client.CommitLanguages(ctx, request)
	require.NoError(t, err)

	expectedLanguages := []*gitalypb.CommitLanguagesResponse_Language{
		{Name: "Ruby", Share: 65.28394, Color: "#701516", Bytes: 2943},
		{Name: "JavaScript", Share: 22.493345, Color: "#f1e05a", Bytes: 1014},
		{Name: "HTML", Share: 7.741792, Color: "#e34c26", Bytes: 349},
		{Name: "CoffeeScript", Share: 2.373558, Color: "#244776", Bytes: 107},
		{Name: "Modula-2", Share: 2.1073646, Color: "#10253f", Bytes: 95},
	}

	testhelper.ProtoEqual(t, expectedLanguages, resp.Languages)
}

func TestLanguagesEmptyRevision(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupCommitServiceWithRepo(t, ctx)

	request := &gitalypb.CommitLanguagesRequest{
		Repository: repo,
	}
	resp, err := client.CommitLanguages(ctx, request)
	require.NoError(t, err)

	require.NotZero(t, len(resp.Languages), "number of languages in response")

	foundRuby := false
	for _, l := range resp.Languages {
		if l.Name == "Ruby" {
			foundRuby = true
		}
	}
	require.True(t, foundRuby, "expected to find Ruby as a language on HEAD")
}

func TestInvalidCommitLanguagesRequestRevision(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupCommitServiceWithRepo(t, ctx)

	_, err := client.CommitLanguages(ctx, &gitalypb.CommitLanguagesRequest{
		Repository: repo,
		Revision:   []byte("--output=/meow"),
	})
	testhelper.RequireGrpcError(t, status.Error(codes.InvalidArgument, "revision can't start with '-'"), err)
}

func TestAmbiguousRefCommitLanguagesRequestRevision(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupCommitServiceWithRepo(t, ctx)

	// gitlab-test repo has both a branch and a tag named 'v1.1.0'
	// b83d6e391c22777fca1ed3012fce84f633d7fed0 refs/heads/v1.1.0
	// 8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b refs/tags/v1.1.0
	_, err := client.CommitLanguages(ctx, &gitalypb.CommitLanguagesRequest{
		Repository: repo,
		Revision:   []byte("v1.1.0"),
	})
	require.NoError(t, err)
}

func TestCommitLanguages_validateRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupCommitServiceWithRepo(t, ctx)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.CommitLanguagesRequest
		expectedErr error
	}{
		{
			desc: "no repository provided",
			req:  &gitalypb.CommitLanguagesRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "invalid revision provided",
			req: &gitalypb.CommitLanguagesRequest{
				Repository: repo,
				Revision:   []byte("invalid revision"),
			},
			expectedErr: status.Error(codes.InvalidArgument, "revision can't contain whitespace"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CommitLanguages(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
