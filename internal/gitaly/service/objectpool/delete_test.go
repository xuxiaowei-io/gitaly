//go:build !gitaly_test_sha256

package objectpool

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestDelete(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, _, _, client := setup(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repositoryClient := gitalypb.NewRepositoryServiceClient(extractConn(client))

	poolProto, _, _ := createObjectPool(t, ctx, cfg, client, repoProto)
	validPoolPath := poolProto.GetRepository().GetRelativePath()

	for _, tc := range []struct {
		desc         string
		noPool       bool
		relativePath string
		expectedErr  error
	}{
		{
			desc:   "no pool in request fails",
			noPool: true,
			expectedErr: testhelper.GitalyOrPraefect(
				structerr.NewInvalidArgument("GetStorageByName: no such storage: %q", ""),
				structerr.NewInvalidArgument("no object pool repository"),
			),
		},
		{
			desc:         "deleting outside pools directory fails",
			relativePath: ".",
			expectedErr:  errInvalidPoolDir,
		},
		{
			desc:         "deleting outside pools directory fails",
			relativePath: ".",
			expectedErr:  errInvalidPoolDir,
		},
		{
			desc:         "deleting pools directory fails",
			relativePath: "@pools",
			expectedErr:  errInvalidPoolDir,
		},
		{
			desc:         "deleting first level subdirectory fails",
			relativePath: "@pools/ab",
			expectedErr:  errInvalidPoolDir,
		},
		{
			desc:         "deleting second level subdirectory fails",
			relativePath: "@pools/ab/cd",
			expectedErr:  errInvalidPoolDir,
		},
		{
			desc:         "deleting pool subdirectory fails",
			relativePath: filepath.Join(validPoolPath, "objects"),
			expectedErr:  errInvalidPoolDir,
		},
		{
			desc:         "path traversing fails",
			relativePath: validPoolPath + "/../../../../..",
			expectedErr: testhelper.GitalyOrPraefect(
				structerr.NewInvalidArgument("GetRepoPath: %w", storage.ErrRelativePathEscapesRoot),
				errInvalidPoolDir,
			),
		},
		{
			desc:         "deleting pool succeeds",
			relativePath: validPoolPath,
		},
		{
			desc:         "deleting non-existent pool succeeds",
			relativePath: validPoolPath,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.DeleteObjectPoolRequest{ObjectPool: &gitalypb.ObjectPool{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: tc.relativePath,
				},
			}}

			if tc.noPool {
				request.ObjectPool = nil
			}

			_, err := client.DeleteObjectPool(ctx, request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)

			response, err := repositoryClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
				Repository: poolProto.GetRepository(),
			})
			require.NoError(t, err)
			require.Equal(t, tc.expectedErr != nil, response.GetExists())
		})
	}
}
