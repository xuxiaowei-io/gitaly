//go:build !gitaly_test_sha256

package repository

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetCustomHooks_successful(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamReader func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) *tar.Reader
	}{
		{
			desc: "GetCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.GetCustomHooksRequest{Repository: repo}
				stream, err := client.GetCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
		{
			desc: "BackupCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.BackupCustomHooksRequest{Repository: repo}
				//nolint:staticcheck
				stream, err := client.BackupCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg, client := setupRepositoryServiceWithoutRepo(t)
			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			expectedTarResponse := []string{
				"custom_hooks/",
				"custom_hooks/pre-commit.sample",
				"custom_hooks/prepare-commit-msg.sample",
				"custom_hooks/pre-push.sample",
			}
			require.NoError(t, os.Mkdir(filepath.Join(repoPath, "custom_hooks"), perm.PrivateDir), "Could not create custom_hooks dir")
			for _, fileName := range expectedTarResponse[1:] {
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, fileName), []byte("Some hooks"), perm.PrivateExecutable), fmt.Sprintf("Could not create %s", fileName))
			}

			reader := tc.streamReader(t, ctx, repo, client)
			fileLength := 0
			for {
				file, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				fileLength++
				require.Contains(t, expectedTarResponse, file.Name)
			}
			require.Equal(t, fileLength, len(expectedTarResponse))
		})
	}
}

func TestGetCustomHooks_symlink(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamReader func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) *tar.Reader
	}{
		{
			desc: "GetCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.GetCustomHooksRequest{Repository: repo}
				stream, err := client.GetCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
		{
			desc: "BackupCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.BackupCustomHooksRequest{Repository: repo}
				//nolint:staticcheck
				stream, err := client.BackupCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg, client := setupRepositoryServiceWithoutRepo(t)
			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			linkTarget := "/var/empty"
			require.NoError(t, os.Symlink(linkTarget, filepath.Join(repoPath, "custom_hooks")), "Could not create custom_hooks symlink")

			reader := tc.streamReader(t, ctx, repo, client)

			file, err := reader.Next()
			require.NoError(t, err)

			require.Equal(t, "custom_hooks", file.Name, "tar entry name")
			require.Equal(t, byte(tar.TypeSymlink), file.Typeflag, "tar entry type")
			require.Equal(t, linkTarget, file.Linkname, "link target")

			_, err = reader.Next()
			require.Equal(t, io.EOF, err, "custom_hooks should have been the only entry")
		})
	}
}

func TestGetCustomHooks_nonexistentHooks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamReader func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) *tar.Reader
	}{
		{
			desc: "GetCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.GetCustomHooksRequest{Repository: repo}
				stream, err := client.GetCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
		{
			desc: "BackupCustomHooks",
			streamReader: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) *tar.Reader {
				request := &gitalypb.BackupCustomHooksRequest{Repository: repo}
				//nolint:staticcheck
				stream, err := client.BackupCustomHooks(ctx, request)
				require.NoError(t, err)

				return tar.NewReader(streamio.NewReader(func() ([]byte, error) {
					response, err := stream.Recv()
					return response.GetData(), err
				}))
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg, client := setupRepositoryServiceWithoutRepo(t)
			repo, _ := gittest.CreateRepository(t, ctx, cfg)

			reader := tc.streamReader(t, ctx, repo, client)

			buf := bytes.NewBuffer(nil)
			_, err := io.Copy(buf, reader)
			require.NoError(t, err)

			require.Empty(t, buf.String(), "Returned stream should be empty")
		})
	}
}

func TestGetCustomHooks_validate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.GetCustomHooksRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			req:  &gitalypb.GetCustomHooksRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"validating repository: empty Repository",
				"repo scoped: empty Repository",
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetCustomHooks(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestBackupCustomHooks_validate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.BackupCustomHooksRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			req:  &gitalypb.BackupCustomHooksRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"validating repository: empty Repository",
				"repo scoped: empty Repository",
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			stream, err := client.BackupCustomHooks(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
