package repository

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRenameRepositorySuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryServiceWithoutRepo(t)
	locator := config.NewLocator(cfg)
	originalRepo, originalPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, originalPath)

	targetPath := "a-new-location"
	_, err := client.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
		Repository:   originalRepo,
		RelativePath: targetPath,
	})
	require.NoError(t, err)

	// A repository should not exist with the previous relative path
	exists, err := client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: originalRepo,
	})
	require.NoError(t, err)
	testhelper.ProtoEqual(t, &gitalypb.RepositoryExistsResponse{Exists: false}, exists)

	// A repository should exist with the new relative path
	renamedRepo := &gitalypb.Repository{StorageName: originalRepo.StorageName, RelativePath: targetPath}
	exists, err = client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: renamedRepo,
	})
	require.NoError(t, err)
	testhelper.ProtoEqual(t, &gitalypb.RepositoryExistsResponse{Exists: true}, exists)

	if testhelper.IsPraefectEnabled() {
		// Praefect does not move repositories on the disk.
		targetPath = gittest.GetReplicaPath(t, ctx, cfg, renamedRepo)
	}
	newDirectory := filepath.Join(cfg.Storages[0].Path, targetPath)

	require.NoError(t, locator.ValidateRepository(&gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: targetPath,
	}), "moved Git repository has been corrupted")
	// ensure the git directory that got renamed contains the original commit.
	gittest.RequireObjectExists(t, cfg, newDirectory, commitID)
}

func TestRenameRepositoryDestinationExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryServiceWithoutRepo(t)

	existingDestinationRepo, destinationRepoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, destinationRepoPath)
	renamedRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	_, err := client.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
		Repository:   renamedRepo,
		RelativePath: existingDestinationRepo.RelativePath,
	})
	testhelper.RequireGrpcCode(t, err, codes.AlreadyExists)

	// ensure the git directory that already existed didn't get overwritten
	gittest.RequireObjectExists(t, cfg, destinationRepoPath, commitID)
}

func TestRenameRepositoryInvalidRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryServiceWithoutRepo(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	testCases := []struct {
		desc string
		req  *gitalypb.RenameRepositoryRequest
		exp  error
	}{
		{
			desc: "empty repository",
			req:  &gitalypb.RenameRepositoryRequest{Repository: nil, RelativePath: "/tmp/abc"},
			exp:  structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty destination relative path",
			req:  &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: ""},
			exp:  status.Error(codes.InvalidArgument, "destination relative path is empty"),
		},
		{
			desc: "destination relative path contains path traversal",
			req:  &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: "../usr/bin"},
			exp: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("%w", storage.ErrRelativePathEscapesRoot),
				"relative_path", "../usr/bin",
			),
		},
		{
			desc: "repository storage doesn't exist",
			req:  &gitalypb.RenameRepositoryRequest{Repository: &gitalypb.Repository{StorageName: "stub", RelativePath: repo.RelativePath}, RelativePath: "usr/bin"},
			exp: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("stub"),
			)),
		},
		{
			desc: "repository relative path doesn't exist",
			req:  &gitalypb.RenameRepositoryRequest{Repository: &gitalypb.Repository{StorageName: repo.StorageName, RelativePath: "stub"}, RelativePath: "non-existent/directory"},
			exp: testhelper.GitalyOrPraefect(
				testhelper.WithInterceptedMetadata(
					structerr.NewNotFound("%w", storage.ErrRepositoryNotFound),
					"repository_path", filepath.Join(cfg.Storages[0].Path, "stub"),
				),
				testhelper.ToInterceptedMetadata(
					structerr.NewNotFound("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "stub")),
				),
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameRepository(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.exp, err)
		})
	}
}
