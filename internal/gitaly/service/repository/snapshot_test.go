//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func TestGetSnapshot(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	getSnapshot := func(tb testing.TB, ctx context.Context, client gitalypb.RepositoryServiceClient, req *gitalypb.GetSnapshotRequest) ([]byte, error) {
		stream, err := client.GetSnapshot(ctx, req)
		if err != nil {
			return nil, err
		}

		reader := streamio.NewReader(func() ([]byte, error) {
			response, err := stream.Recv()
			return response.GetData(), err
		})

		buf := bytes.NewBuffer(nil)
		_, err = io.Copy(buf, reader)

		return buf.Bytes(), err
	}

	type setupData struct {
		repo            *gitalypb.Repository
		expectedEntries []string
		expectedError   error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "repository does not exist",
			setup: func(t *testing.T) setupData {
				// If the requested repository does not exist, the RPC should return an error.
				return setupData{
					repo: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "does-not-exist.git",
					},
					expectedError: testhelper.WithInterceptedMetadataItems(
						structerr.NewNotFound("repository not found"),
						structerr.MetadataItem{Key: "relative_path", Value: "does-not-exist.git"},
						structerr.MetadataItem{Key: "storage_name", Value: cfg.Storages[0].Name},
					),
				}
			},
		},
		{
			desc: "storage not set in request",
			setup: func(t *testing.T) setupData {
				// If the request does not contain a storage, the RPC should return an error.
				return setupData{
					repo: &gitalypb.Repository{
						StorageName:  "",
						RelativePath: "some-relative-path",
					},
					expectedError: structerr.NewInvalidArgument("%w", storage.ErrStorageNotSet),
				}
			},
		},
		{
			desc: "relative path not set in request",
			setup: func(t *testing.T) setupData {
				// If the request does not contain a relative path, the RPC should return an error.
				return setupData{
					repo: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "",
					},
					expectedError: structerr.NewInvalidArgument("%w", storage.ErrRepositoryPathNotSet),
				}
			},
		},
		{
			desc: "repository contains symlink",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Make packed-refs into a symlink so the RPC returns an error.
				packedRefsFile := filepath.Join(repoPath, "packed-refs")
				require.NoError(t, os.Symlink("HEAD", packedRefsFile))

				return setupData{
					repo: repoProto,
					expectedError: structerr.NewInternal(
						"building snapshot failed: open %s: too many levels of symbolic links",
						packedRefsFile,
					),
				}
			},
		},
		{
			desc: "repository snapshot success",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Write commit and perform `git-repack` to generate a packfile and index in the
				// repository.
				treeID := gittest.WriteTree(t, cfg, repoPath, nil)
				commitID := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithBranch("master"),
					gittest.WithTree(treeID),
				)
				gittest.Exec(t, cfg, "-C", repoPath, "repack")

				// The only entries in the pack directory should be the generated packfile and its
				// corresponding index.
				packEntries, err := os.ReadDir(filepath.Join(repoPath, "objects/pack"))
				require.NoError(t, err)
				index := packEntries[0].Name()
				packfile := packEntries[1].Name()

				// Unreachable objects should also be included in the snapshot.
				unreachableCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unreachable"))

				// Generate packed-refs file, but also keep around the loose reference.
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all", "--no-prune")

				// The shallow file, used if the repository is a shallow clone, is also included in snapshots.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "shallow"), nil, perm.SharedFile))

				// Custom Git hooks are not included in snapshots.
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "hooks"), perm.SharedDir))

				// Create a file in the objects directory that does not match the regex.
				require.NoError(t, os.WriteFile(
					filepath.Join(repoPath, "objects/this-should-not-be-included"),
					nil,
					perm.SharedFile,
				))

				return setupData{
					repo: repoProto,
					expectedEntries: []string{
						"HEAD",
						"packed-refs",
						"refs/",
						"refs/heads/",
						"refs/heads/master",
						"refs/tags/",
						fmt.Sprintf("objects/%s/%s", treeID[0:2], treeID[2:]),
						fmt.Sprintf("objects/%s/%s", commitID[0:2], commitID[2:]),
						fmt.Sprintf("objects/%s/%s", unreachableCommitID[0:2], unreachableCommitID[2:]),
						filepath.Join("objects/pack", index),
						filepath.Join("objects/pack", packfile),
						"shallow",
					},
				}
			},
		},
		{
			desc: "alternate object database does not exist",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				altFile, err := repo.InfoAlternatesPath()
				require.NoError(t, err)

				// Write a non-existent object database to the repository's alternates file. The RPC
				// should skip over the alternates database and continue generating the snapshot.
				altObjectDir := filepath.Join(repoPath, "does-not-exist")
				require.NoError(t, os.WriteFile(
					altFile,
					[]byte(fmt.Sprintf("%s\n", altObjectDir)),
					perm.SharedFile,
				))

				return setupData{
					repo:            repoProto,
					expectedEntries: []string{"HEAD", "refs/", "refs/heads/", "refs/tags/"},
				}
			},
		},
		{
			desc: "alternate file with bad permissions",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				altFile, err := repo.InfoAlternatesPath()
				require.NoError(t, err)

				// Write an object database with bad permissions to the repository's alternates
				// file. The RPC should skip over the alternates database and continue generating
				// the snapshot.
				altObjectDir := filepath.Join(repoPath, "alt-object-dir")
				require.NoError(t, os.WriteFile(altFile, []byte(fmt.Sprintf("%s\n", altObjectDir)), 0o000))

				return setupData{
					repo:            repoProto,
					expectedEntries: []string{"HEAD", "refs/", "refs/heads/", "refs/tags/"},
				}
			},
		},
		{
			desc: "alternate object database escapes storage root",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				altFile, err := repo.InfoAlternatesPath()
				require.NoError(t, err)

				altObjectDir := filepath.Join(cfg.Storages[0].Path, "../alt-dir")
				treeID := gittest.WriteTree(t, cfg, repoPath, nil)
				commitID := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(treeID),
					gittest.WithAlternateObjectDirectory(altObjectDir),
				)

				// We haven't yet written the alternates file, and thus we shouldn't be able to find
				// this commit yet.
				gittest.RequireObjectNotExists(t, cfg, repoPath, commitID)

				// Write the alternates file and validate that the object is now reachable.
				require.NoError(t, os.WriteFile(
					altFile,
					[]byte(fmt.Sprintf("%s\n", altObjectDir)),
					perm.SharedFile,
				))
				gittest.RequireObjectExists(t, cfg, repoPath, commitID)

				return setupData{
					repo: repoProto,
					expectedEntries: []string{
						"HEAD",
						"refs/",
						"refs/heads/",
						"refs/tags/",
						// The commit ID is not included because it exists in an alternate object
						// database that is outside the storage root.
						fmt.Sprintf("objects/%s/%s", treeID[0:2], treeID[2:]),
					},
				}
			},
		},
		{
			desc: "alternate object database is subdirectory",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				altFile, err := repo.InfoAlternatesPath()
				require.NoError(t, err)

				altObjectDir := filepath.Join(repoPath, "objects/alt-objects")
				treeID := gittest.WriteTree(t, cfg, repoPath, nil)
				commitID := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(treeID),
					gittest.WithAlternateObjectDirectory(altObjectDir),
				)

				// We haven't yet written the alternates file, and thus we shouldn't be able to find
				// this commit yet.
				gittest.RequireObjectNotExists(t, cfg, repoPath, commitID)

				// Write the alternates file and validate that the object is now reachable.
				require.NoError(t, os.WriteFile(altFile, []byte("./alt-objects\n"), perm.SharedFile))
				gittest.RequireObjectExists(t, cfg, repoPath, commitID)

				return setupData{
					repo: repoProto,
					expectedEntries: []string{
						"HEAD",
						"refs/",
						"refs/heads/",
						"refs/tags/",
						fmt.Sprintf("objects/%s/%s", treeID[0:2], treeID[2:]),
						fmt.Sprintf("objects/%s/%s", commitID[0:2], commitID[2:]),
						// If the alternate object database is under the object database it will be
						// included again in the snapshot.
						fmt.Sprintf("objects/alt-objects/%s/%s", commitID[0:2], commitID[2:]),
					},
				}
			},
		},
		{
			desc: "alternate object database is absolute path",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				altFile, err := repo.InfoAlternatesPath()
				require.NoError(t, err)

				altObjectDir := filepath.Join(cfg.Storages[0].Path, gittest.NewObjectPoolName(t), "objects")
				treeID := gittest.WriteTree(t, cfg, repoPath, nil)
				commitID := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(treeID),
					gittest.WithAlternateObjectDirectory(altObjectDir),
				)

				// We haven't yet written the alternates file, and thus we shouldn't be able to find
				// this commit yet.
				gittest.RequireObjectNotExists(t, cfg, repoPath, commitID)

				// Write the alternates file and validate that the object is now reachable.
				require.NoError(t, os.WriteFile(
					altFile,
					[]byte(fmt.Sprintf("%s\n", altObjectDir)),
					perm.SharedFile,
				))
				gittest.RequireObjectExists(t, cfg, repoPath, commitID)

				return setupData{
					repo: repoProto,
					expectedEntries: []string{
						"HEAD",
						"refs/",
						"refs/heads/",
						"refs/tags/",
						fmt.Sprintf("objects/%s/%s", treeID[0:2], treeID[2:]),
						fmt.Sprintf("objects/%s/%s", commitID[0:2], commitID[2:]),
					},
				}
			},
		},
		{
			desc: "alternate object database is relative path",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				altFile, err := repo.InfoAlternatesPath()
				require.NoError(t, err)

				altObjectDir := filepath.Join(cfg.Storages[0].Path, gittest.NewObjectPoolName(t), "objects")
				treeID := gittest.WriteTree(t, cfg, repoPath, nil)
				commitID := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(treeID),
					gittest.WithAlternateObjectDirectory(altObjectDir),
				)

				// We haven't yet written the alternates file, and thus we shouldn't be able to find
				// this commit yet.
				gittest.RequireObjectNotExists(t, cfg, repoPath, commitID)

				// Write the alternates file and validate that the object is now reachable.
				relAltObjectDir, err := filepath.Rel(filepath.Join(repoPath, "objects"), altObjectDir)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(
					altFile,
					[]byte(fmt.Sprintf("%s\n", relAltObjectDir)),
					perm.SharedFile,
				))
				gittest.RequireObjectExists(t, cfg, repoPath, commitID)

				return setupData{
					repo: repoProto,
					expectedEntries: []string{
						"HEAD",
						"refs/",
						"refs/heads/",
						"refs/tags/",
						fmt.Sprintf("objects/%s/%s", treeID[0:2], treeID[2:]),
						fmt.Sprintf("objects/%s/%s", commitID[0:2], commitID[2:]),
					},
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			data, err := getSnapshot(t, ctx, client, &gitalypb.GetSnapshotRequest{Repository: setup.repo})
			testhelper.RequireGrpcError(t, setup.expectedError, err)
			if err != nil {
				return
			}

			entries, err := archive.TarEntries(bytes.NewReader(data))
			require.NoError(t, err)

			require.ElementsMatch(t, entries, setup.expectedEntries)
		})
	}
}
