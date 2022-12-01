//go:build !gitaly_test_sha256

package repository

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRepositoryExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "other", "broken"))
	cfg := cfgBuilder.Build(t)

	require.NoError(t, os.RemoveAll(cfg.Storages[2].Path), "third storage needs to be invalid")

	client, socketPath := runRepositoryService(t, cfg, nil)
	cfg.SocketPath = socketPath

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{})

	queries := []struct {
		desc        string
		request     *gitalypb.RepositoryExistsRequest
		expectedErr error
		exists      bool
	}{
		{
			desc: "repository nil",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: nil,
			},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(gitalyerrors.ErrEmptyRepository.Error(), "missing repository")),
		},
		{
			desc: "storage name empty",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "",
					RelativePath: repo.GetRelativePath(),
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(gitalyerrors.ErrEmptyStorageName.Error(), "repository missing storage name")),
		},
		{
			desc: "relative path empty",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: "",
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(gitalyerrors.ErrEmptyRelativePath.Error(), "repository missing relative path")),
		},
		{
			desc: "exists true",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: repo.GetRelativePath(),
				},
			},
			exists: true,
		},
		{
			desc: "exists false, wrong storage",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "other",
					RelativePath: repo.GetRelativePath(),
				},
			},
			exists: false,
		},
		{
			desc: "storage not configured",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "unconfigured",
					RelativePath: "foobar.git",
				},
			},
			expectedErr: func() error {
				if testhelper.IsPraefectEnabled() {
					// Praefect doesn't check for storage existence but just returns that the repository doesn't exist.
					return nil
				}

				return status.Errorf(codes.InvalidArgument, `GetStorageByName: no such storage: "unconfigured"`)
			}(),
		},
		{
			desc: "storage directory does not exist",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "broken",
					RelativePath: "foobar.git",
				},
			},
			expectedErr: func() error {
				if testhelper.IsPraefectEnabled() {
					// Praefect checks repository existence from the database so it can't encounter an error where a storage is configured
					// but doesn't exist.
					return nil
				}

				return status.Errorf(codes.NotFound, "GetPath: does not exist: stat %s: no such file or directory", cfg.Storages[2].Path)
			}(),
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.RepositoryExists(ctx, tc.request)
			testhelper.ProtoEqual(t, tc.expectedErr, err)
			if err != nil {
				// Ignore the response message if there was an error
				return
			}

			require.Equal(t, tc.exists, response.Exists)
		})
	}
}
