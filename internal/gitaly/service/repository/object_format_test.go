package repository

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestObjectFormat(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	type setupData struct {
		request          *gitalypb.ObjectFormatRequest
		expectedErr      error
		expectedResponse *gitalypb.ObjectFormatResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "unset repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ObjectFormatRequest{},
					expectedErr: testhelper.GitalyOrPraefect(
						structerr.NewInvalidArgument("%w", errors.ErrEmptyRepository),
						structerr.NewInvalidArgument("repo scoped: %w", errors.ErrEmptyRepository),
					),
				}
			},
		},
		{
			desc: "missing storage name",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ObjectFormatRequest{
						Repository: &gitalypb.Repository{},
					},
					expectedErr: testhelper.GitalyOrPraefect(
						structerr.NewInvalidArgument("%w", errors.ErrEmptyStorageName),
						structerr.NewInvalidArgument("repo scoped: %w", errors.ErrInvalidRepository),
					),
				}
			},
		},
		{
			desc: "missing relative path",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ObjectFormatRequest{
						Repository: &gitalypb.Repository{
							StorageName: cfg.Storages[0].Name,
						},
					},
					expectedErr: testhelper.GitalyOrPraefect(
						structerr.NewInvalidArgument("%w", errors.ErrEmptyRelativePath),
						structerr.NewInvalidArgument("repo scoped: %w", errors.ErrInvalidRepository),
					),
				}
			},
		},
		{
			desc: "nonexistent repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ObjectFormatRequest{
						Repository: &gitalypb.Repository{
							StorageName:  cfg.Storages[0].Name,
							RelativePath: "nonexistent.git",
						},
					},
					expectedErr: testhelper.GitalyOrPraefect(
						structerr.NewNotFound(
							"GetRepoPath: not a git repository: %q", filepath.Join(cfg.Storages[0].Path, "nonexistent.git"),
						),
						structerr.NewNotFound(
							"accessor call: route repository accessor: consistent storages: repository %q/%q not found",
							cfg.Storages[0].Name, "nonexistent.git",
						),
					),
				}
			},
		},
		{
			desc: "SHA1",
			setup: func(t *testing.T) setupData {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					ObjectFormat: "sha1",
				})

				return setupData{
					request: &gitalypb.ObjectFormatRequest{
						Repository: repoProto,
					},
					expectedResponse: &gitalypb.ObjectFormatResponse{
						Format: gitalypb.ObjectFormat_OBJECT_FORMAT_SHA1,
					},
				}
			},
		},
		{
			desc: "SHA256",
			setup: func(t *testing.T) setupData {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					ObjectFormat: "sha256",
				})

				return setupData{
					request: &gitalypb.ObjectFormatRequest{
						Repository: repoProto,
					},
					expectedResponse: &gitalypb.ObjectFormatResponse{
						Format: gitalypb.ObjectFormat_OBJECT_FORMAT_SHA256,
					},
				}
			},
		},
		{
			desc: "invalid object format",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// We write the config file manually so that we can use an
				// exact-match for the error down below.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "config"), []byte(
					strings.Join([]string{
						"[core]",
						"repositoryformatversion = 1",
						"bare = true",
						"[extensions]",
						"objectFormat = blake2b",
					}, "\n"),
				), perm.SharedFile))

				return setupData{
					request: &gitalypb.ObjectFormatRequest{
						Repository: repoProto,
					},
					expectedErr: structerr.New("detecting object hash: reading object format: exit status 128").WithInterceptedMetadata(
						"stderr",
						fmt.Sprintf("error: invalid value for 'extensions.objectformat': 'blake2b'\n"+
							"fatal: bad config line 5 in file %s\n", filepath.Join(repoPath, "config"),
						),
					),
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setupData := tc.setup(t)
			response, err := client.ObjectFormat(ctx, setupData.request)
			testhelper.RequireGrpcError(t, setupData.expectedErr, err)
			testhelper.ProtoEqual(t, setupData.expectedResponse, response)
		})
	}
}
