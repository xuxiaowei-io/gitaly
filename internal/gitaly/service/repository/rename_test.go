//go:build !gitaly_test_sha256

package repository

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRenameRepositorySuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, originalRepo, originalPath, client := setupRepositoryService(t, ctx)

	const targetPath = "a-new-location"
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

	newDirectory := filepath.Join(cfg.Storages[0].Path, targetPath)
	if testhelper.IsPraefectEnabled() {
		// Praefect does not move repositories on the disk.
		newDirectory = originalPath
	}

	require.DirExists(t, newDirectory)
	require.True(t, storage.IsGitDirectory(newDirectory), "moved Git repository has been corrupted")
	// ensure the git directory that got renamed contains a sha in the seed repo
	gittest.RequireObjectExists(t, cfg, newDirectory, git.ObjectID("913c66a37b4a45b9769037c55c2d238bd0942d2e"))
}

func TestRenameRepositoryDestinationExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryServiceWithoutRepo(t)

	existingDestinationRepo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "repository-1"}
	_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: existingDestinationRepo})
	require.NoError(t, err)

	renamedRepo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "repository-2"}
	_, err = client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: renamedRepo})
	require.NoError(t, err)

	destinationRepoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, existingDestinationRepo))
	commitID := gittest.WriteCommit(t, cfg, destinationRepoPath)

	_, err = client.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
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

	_, repo, repoPath, client := setupRepositoryService(t, ctx)
	storagePath := strings.TrimSuffix(repoPath, "/"+repo.RelativePath)

	testCases := []struct {
		desc string
		req  *gitalypb.RenameRepositoryRequest
		exp  error
	}{
		{
			desc: "empty repository",
			req:  &gitalypb.RenameRepositoryRequest{Repository: nil, RelativePath: "/tmp/abc"},
			exp:  status.Error(codes.InvalidArgument, "empty Repository"),
		},
		{
			desc: "empty destination relative path",
			req:  &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: ""},
			exp:  status.Error(codes.InvalidArgument, "destination relative path is empty"),
		},
		{
			desc: "destination relative path contains path traversal",
			req:  &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: "../usr/bin"},
			exp:  status.Error(codes.InvalidArgument, "GetRepoPath: relative path escapes root directory"),
		},
		{
			desc: "repository storage doesn't exist",
			req:  &gitalypb.RenameRepositoryRequest{Repository: &gitalypb.Repository{StorageName: "stub", RelativePath: repo.RelativePath}, RelativePath: "usr/bin"},
			exp:  status.Error(codes.InvalidArgument, `GetStorageByName: no such storage: "stub"`),
		},
		{
			desc: "repository relative path doesn't exist",
			req:  &gitalypb.RenameRepositoryRequest{Repository: &gitalypb.Repository{StorageName: repo.StorageName, RelativePath: "stub"}, RelativePath: "non-existent/directory"},
			exp:  status.Error(codes.NotFound, fmt.Sprintf(`GetRepoPath: not a git repository: "%s/stub"`, testhelper.GitalyOrPraefect(storagePath, repo.GetStorageName()))),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameRepository(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.exp, err)
		})
	}
}
