package remoterepo_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestRepository(t *testing.T) {
	t.Parallel()

	cfg := setupGitalyServer(t)

	pool := client.NewPool()
	defer pool.Close()

	gittest.TestRepository(t, cfg, func(tb testing.TB, ctx context.Context) (git.Repository, string) {
		tb.Helper()

		ctx, err := storage.InjectGitalyServers(ctx, "default", cfg.SocketPath, cfg.Auth.Token)
		require.NoError(tb, err)

		repoProto, repoPath := gittest.CreateRepository(tb, ctx, cfg)

		repo, err := remoterepo.New(metadata.OutgoingToIncoming(ctx), repoProto, pool)
		require.NoError(tb, err)
		return repo, repoPath
	})
}

func TestRepository_ObjectHash(t *testing.T) {
	t.Parallel()

	cfg := setupGitalyServer(t)

	ctx := testhelper.Context(t)
	ctx, err := storage.InjectGitalyServers(ctx, "default", cfg.SocketPath, cfg.Auth.Token)
	require.NoError(t, err)
	ctx = metadata.OutgoingToIncoming(ctx)

	pool := client.NewPool()
	defer pool.Close()

	type setupData struct {
		repo           *remoterepo.Repo
		expectedErr    error
		expectedFormat string
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "SHA1",
			setup: func(t *testing.T) setupData {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					ObjectFormat: "sha1",
				})

				repo, err := remoterepo.New(ctx, repoProto, pool)
				require.NoError(t, err)

				return setupData{
					repo:           repo,
					expectedFormat: "sha1",
				}
			},
		},
		{
			desc: "SHA256",
			setup: func(t *testing.T) setupData {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					ObjectFormat: "sha256",
				})

				repo, err := remoterepo.New(ctx, repoProto, pool)
				require.NoError(t, err)

				return setupData{
					repo:           repo,
					expectedFormat: "sha256",
				}
			},
		},
		{
			desc: "invalid object format",
			setup: func(t *testing.T) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					ObjectFormat: "sha256",
				})

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

				repo, err := remoterepo.New(ctx, repoProto, pool)
				require.NoError(t, err)

				return setupData{
					repo: repo,
					expectedErr: structerr.NewInternal("detecting object hash: reading object format: exit status 128").WithInterceptedMetadata(
						"stderr",
						fmt.Sprintf("error: invalid value for 'extensions.objectformat': 'blake2b'\nfatal: bad config line 5 in file %s/%s\n", repoPath, "config"),
					),
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setupData := tc.setup(t)

			objectHash, err := setupData.repo.ObjectHash(ctx)
			testhelper.RequireGrpcError(t, setupData.expectedErr, err)
			require.Equal(t, setupData.expectedFormat, objectHash.Format)
		})
	}
}
