package localrepo

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestRepoCreateBundle(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		setup          func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string)
		expectedErr    error
		expectComplete bool
	}{
		{
			desc:        "empty bundle",
			setup:       func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string) {},
			expectedErr: fmt.Errorf("create bundle: %w", ErrEmptyBundle),
		},
		{
			desc: "all refs",
			setup: func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string) {
				masterID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(masterID), gittest.WithBranch(git.DefaultBranch))
			},
			expectComplete: true,
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			cfg, repo, repoPath := setupRepo(t)
			tc.setup(t, cfg, repo, repoPath)

			bundle, err := os.Create(filepath.Join(testhelper.TempDir(t), "bundle"))
			require.NoError(t, err)

			err = repo.CreateBundle(ctx, bundle)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.expectedErr, err)
			}

			if tc.expectComplete {
				output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundle.Name())
				require.Contains(t, string(output), "The bundle records a complete history")
			}
		})
	}
}
