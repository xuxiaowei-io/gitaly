//go:build !gitaly_test_sha256

package diff

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulDiffStatsRequest(t *testing.T) {
	ctx := testhelper.Context(t)
	_, repo, _, client := setupDiffService(t, ctx)

	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.DiffStatsRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	expectedStats := []diff.NumStat{
		{
			Path:      []byte("CONTRIBUTING.md"),
			Additions: 1,
			Deletions: 1,
		},
		{
			Path:      []byte("MAINTENANCE.md"),
			Additions: 1,
			Deletions: 1,
		},
		{
			Path:      []byte("README.md"),
			Additions: 1,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/deleted-file"),
			Additions: 0,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/file-with-multiple-chunks"),
			Additions: 28,
			Deletions: 23,
		},
		{
			Path:      []byte("gitaly/logo-white.png"),
			Additions: 0,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/mode-file"),
			Additions: 0,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/mode-file-with-mods"),
			Additions: 2,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/named-file-with-mods"),
			Additions: 0,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/no-newline-at-the-end"),
			Additions: 1,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/renamed-file"),
			Additions: 0,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/renamed-file-with-mods"),
			Additions: 1,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/tab\tnewline\n file"),
			Additions: 1,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/テスト.txt"),
			Additions: 0,
			Deletions: 0,
		},
	}

	stream, err := client.DiffStats(ctx, rpcRequest)
	require.NoError(t, err)

	for {
		fetchedStats, err := stream.Recv()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		stats := fetchedStats.GetStats()

		for index, fetchedStat := range stats {
			expectedStat := expectedStats[index]

			require.Equal(t, expectedStat.Path, fetchedStat.Path)
			require.Equal(t, expectedStat.Additions, fetchedStat.Additions)
			require.Equal(t, expectedStat.Deletions, fetchedStat.Deletions)
		}
	}
}

func TestFailedDiffStatsRequest(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg, repo, _, client := setupDiffService(t, ctx)

	tests := []struct {
		desc          string
		repo          *gitalypb.Repository
		leftCommitID  string
		rightCommitID string
		expectedErr   error
	}{
		{
			desc:          "repository not provided",
			repo:          nil,
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			expectedErr: testhelper.GitalyOrPraefect(
				structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				structerr.NewInvalidArgument("repo scoped: %w", storage.ErrRepositoryNotSet),
			),
		},
		{
			desc:          "repository not found",
			repo:          &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "bar.git"},
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			expectedErr: testhelper.GitalyOrPraefect(
				testhelper.WithInterceptedMetadata(
					structerr.NewNotFound("%w", storage.ErrRepositoryNotFound),
					"repository_path", filepath.Join(cfg.Storages[0].Path, "bar.git"),
				),
				testhelper.ToInterceptedMetadata(
					structerr.New(
						"accessor call: route repository accessor: consistent storages: %w",
						storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "bar.git"),
					),
				),
			),
		},
		{
			desc:          "storage not found",
			repo:          &gitalypb.Repository{StorageName: "foo", RelativePath: "bar.git"},
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			expectedErr: testhelper.GitalyOrPraefect(
				testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
					"%w", storage.NewStorageNotFoundError("foo"),
				)),
				testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
					"repo scoped: %w", storage.NewStorageNotFoundError("foo"),
				)),
			),
		},
		{
			desc:          "left commit ID not found",
			repo:          repo,
			leftCommitID:  "",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			expectedErr:   status.Error(codes.InvalidArgument, "empty LeftCommitId"),
		},
		{
			desc:          "right commit ID not found",
			repo:          repo,
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "",
			expectedErr:   status.Error(codes.InvalidArgument, "empty RightCommitId"),
		},
		{
			desc:          "invalid left commit",
			repo:          repo,
			leftCommitID:  "invalidinvalidinvalid",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			expectedErr:   status.Error(codes.Unavailable, "exit status 128"),
		},
		{
			desc:          "invalid right commit",
			repo:          repo,
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "invalidinvalidinvalid",
			expectedErr:   status.Error(codes.Unavailable, "exit status 128"),
		},
		{
			desc:          "left commit not found",
			repo:          repo,
			leftCommitID:  "a4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			expectedErr:   status.Error(codes.Unavailable, "exit status 128"),
		},
		{
			desc:          "right commit not found",
			repo:          repo,
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "a4003da16c1c2c3fc4567700121b17bf8e591c6c",
			expectedErr:   status.Error(codes.Unavailable, "exit status 128"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := &gitalypb.DiffStatsRequest{Repository: tc.repo, RightCommitId: tc.rightCommitID, LeftCommitId: tc.leftCommitID}
			stream, err := client.DiffStats(ctx, rpcRequest)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
