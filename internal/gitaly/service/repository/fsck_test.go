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
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
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
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects"), nil, perm.SharedFile))

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
		{
			desc: "dangling blob",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// A dangling blob should not cause the consistency check to fail as
				// it is totally expected that repositories accumulate unreachable
				// objects.
				gittest.WriteBlob(t, cfg, repoPath, []byte("content"))

				return setupData{
					repo:             repo,
					expectedResponse: &gitalypb.FsckResponse{},
				}
			},
		},
		{
			desc: "invalid tree object",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Create the default branch so that git-fsck(1) doesn't complain
				// about it missing.
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				// Write a tree object that has the same path twice. We use this as
				// an example to verify that git-fsck(1) indeed also verifies
				// objects as expected just because writing trees with two entries
				// is so easy.
				//
				// Furthermore, the tree is also dangling on purpose to verify that
				// we don't complain about it dangling.
				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{Path: "duplicate", Mode: "100644", Content: "foo"},
					{Path: "duplicate", Mode: "100644", Content: "bar"},
				})

				expectedErr := "error in tree " + treeID.String() + ": duplicateEntries: contains duplicate file entries\n"

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
