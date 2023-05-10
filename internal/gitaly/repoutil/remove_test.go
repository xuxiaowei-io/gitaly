package repoutil

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestRemove(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc        string
		createRepo  func(tb testing.TB, ctx context.Context, cfg config.Cfg) (*gitalypb.Repository, string)
		expectedErr error
	}{
		{
			desc: "success",
			createRepo: func(tb testing.TB, ctx context.Context, cfg config.Cfg) (*gitalypb.Repository, string) {
				return gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
			},
		},
		{
			desc: "does not exist",
			createRepo: func(tb testing.TB, ctx context.Context, cfg config.Cfg) (*gitalypb.Repository, string) {
				repo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "/does/not/exist"}
				return repo, ""
			},
			expectedErr: structerr.NewNotFound("repository does not exist"),
		},
		{
			desc: "locked",
			createRepo: func(tb testing.TB, ctx context.Context, cfg config.Cfg) (*gitalypb.Repository, string) {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				// Simulate a concurrent RPC holding the repository lock.
				lockPath := repoPath + ".lock"
				require.NoError(t, os.WriteFile(lockPath, []byte{}, perm.SharedFile))
				tb.Cleanup(func() {
					require.NoError(t, os.RemoveAll(lockPath))
				})

				return repo, repoPath
			},
			expectedErr: structerr.NewFailedPrecondition("repository is already locked"),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)
			locator := config.NewLocator(cfg)
			txManager := transaction.NewTrackingManager()

			repo, repoPath := tc.createRepo(t, ctx, cfg)

			if repoPath != "" {
				require.DirExists(t, repoPath)
			}

			err := Remove(ctx, locator, txManager, repo)

			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)

			if repoPath != "" {
				require.NoDirExists(t, repoPath)
			}
		})
	}
}
