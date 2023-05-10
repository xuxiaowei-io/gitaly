package localrepo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestRepoCreateBundle(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		opts           *CreateBundleOpts
		setup          func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string)
		expectedErr    error
		expectComplete bool
		expectedRefs   []git.ReferenceName
	}{
		{
			desc:        "empty bundle",
			setup:       func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string) {},
			expectedErr: fmt.Errorf("create bundle: %w", ErrEmptyBundle),
		},
		{
			desc: "complete",
			setup: func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string) {
				masterID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(masterID), gittest.WithBranch("feature"))
			},
			expectComplete: true,
			expectedRefs:   []git.ReferenceName{"refs/heads/feature", git.DefaultRef, "HEAD"},
		},
		{
			desc: "complete patterns",
			setup: func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string) {
				masterID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(masterID), gittest.WithBranch("feature"))
			},
			opts: &CreateBundleOpts{
				Patterns: strings.NewReader("refs/heads/feature\nrefs/heads/ignored_missing_ref\n" + git.DefaultRef.String()),
			},
			expectComplete: true,
			expectedRefs:   []git.ReferenceName{"refs/heads/feature", git.DefaultRef},
		},
		{
			desc: "partial patterns",
			setup: func(t *testing.T, cfg config.Cfg, repo *Repo, repoPath string) {
				masterID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(masterID), gittest.WithBranch("feature"))
			},
			opts: &CreateBundleOpts{
				Patterns: strings.NewReader("refs/heads/feature"),
			},
			expectedRefs: []git.ReferenceName{"refs/heads/feature"},
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

			err = repo.CreateBundle(ctx, bundle, tc.opts)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, bundle.Close())

			if tc.expectComplete {
				output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundle.Name())
				require.Contains(t, string(output), "The bundle records a complete history")
			}

			var refNames []git.ReferenceName
			output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "list-heads", bundle.Name())
			decoder := git.NewShowRefDecoder(bytes.NewReader(output))
			for {
				var ref git.Reference
				err := decoder.Decode(&ref)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)

				refNames = append(refNames, ref.Name)
			}

			require.Equal(t, tc.expectedRefs, refNames)
		})
	}
}
