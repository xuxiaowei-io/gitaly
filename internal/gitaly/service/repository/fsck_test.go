//go:build !gitaly_test_sha256

package repository

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestFsck(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	type setupData struct {
		repo             *gitalypb.Repository
		expectedErr      error
		expectedResponse *gitalypb.FsckResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "request is missing repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					repo: nil,
					expectedErr: structerr.NewInvalidArgument(
						testhelper.GitalyOrPraefect(
							"empty Repository",
							"repo scoped: empty Repository",
						),
					),
				}
			},
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repo:             repo,
					expectedResponse: &gitalypb.FsckResponse{},
				}
			},
		},
		{
			desc: "repository with commit",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				return setupData{
					repo:             repo,
					expectedResponse: &gitalypb.FsckResponse{},
				}
			},
		},
		{
			desc: "invalid object directory",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// This makes the repo severely broken so that `git` does not
				// identify it as a proper repository anymore.
				require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects")))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects"), nil, 0o644))

				return setupData{
					repo: repo,
					expectedResponse: &gitalypb.FsckResponse{
						Error: []byte(fmt.Sprintf("fatal: not a git repository: '%s'\n", repoPath)),
					},
				}
			},
		},
		{
			desc: "missing objects",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// We write a commit and repack it into a packfile...
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")
				// ... but then subsequently delete all the packfiles. This should
				// lead to git-fsck(1) complaining about missing objects.
				require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects", "pack")))

				expectedErr := strings.Join([]string{
					"error: refs/heads/main: invalid sha1 pointer " + commitID.String(),
					"error: HEAD: invalid sha1 pointer " + commitID.String(),
					"notice: No default references",
				}, "\n") + "\n"

				return setupData{
					repo: repo,
					expectedResponse: &gitalypb.FsckResponse{
						Error: []byte(expectedErr),
					},
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setupData := tc.setup(t)

			response, err := client.Fsck(ctx, &gitalypb.FsckRequest{
				Repository: setupData.repo,
			})
			testhelper.RequireGrpcError(t, setupData.expectedErr, err)
			testhelper.ProtoEqual(t, setupData.expectedResponse, response)
		})
	}
}
