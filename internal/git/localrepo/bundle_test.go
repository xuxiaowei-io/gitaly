package localrepo

import (
	"bytes"
	"context"
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
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
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

func TestRepo_FetchBundle(t *testing.T) {
	t.Parallel()

	createBundle := func(tb testing.TB, cfg config.Cfg, repoPath string) io.Reader {
		tmp := testhelper.TempDir(tb)
		bundlePath := filepath.Join(tmp, "test.bundle")
		gittest.BundleRepo(t, cfg, repoPath, bundlePath)

		bundle, err := os.Open(bundlePath)
		require.NoError(t, err)
		t.Cleanup(func() { testhelper.MustClose(t, bundle) })

		return bundle
	}

	type testSetup struct {
		reader          io.Reader
		expectedHeadRef git.ReferenceName
		expectedRefs    []git.Reference
	}

	for _, tc := range []struct {
		desc                string
		opts                *CreateBundleOpts
		setup               func(t *testing.T, ctx context.Context, cfg config.Cfg, targetRepoPath string) testSetup
		updateHead          bool
		expectedErrAs       any
		expectedErrContains string
	}{
		{
			desc: "no HEAD update",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg, targetRepoPath string) testSetup {
				_, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				gittest.WriteCommit(t, cfg, targetRepoPath, gittest.WithBranch(git.DefaultBranch))

				mainOid := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch(git.DefaultBranch))
				featureOid := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("feature"), gittest.WithParents(mainOid))
				gittest.Exec(t, cfg, "-C", sourceRepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

				return testSetup{
					reader:          createBundle(t, cfg, sourceRepoPath),
					expectedHeadRef: git.DefaultRef,
					expectedRefs: []git.Reference{
						git.NewReference(git.NewReferenceNameFromBranchName("feature"), featureOid.String()),
						git.NewReference(git.DefaultRef, mainOid.String()),
					},
				}
			},
		},
		{
			desc:       "HEAD update",
			updateHead: true,
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg, targetRepoPath string) testSetup {
				_, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				gittest.WriteCommit(t, cfg, targetRepoPath, gittest.WithBranch(git.DefaultBranch))

				mainOid := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch(git.DefaultBranch))
				featureOid := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("feature"), gittest.WithParents(mainOid))
				gittest.Exec(t, cfg, "-C", sourceRepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

				return testSetup{
					reader:          createBundle(t, cfg, sourceRepoPath),
					expectedHeadRef: git.NewReferenceNameFromBranchName("feature"),
					expectedRefs: []git.Reference{
						git.NewReference(git.NewReferenceNameFromBranchName("feature"), featureOid.String()),
						git.NewReference(git.DefaultRef, mainOid.String()),
					},
				}
			},
		},
		{
			desc: "empty bundle",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg, targetRepoPath string) testSetup {
				return testSetup{reader: new(bytes.Reader)}
			},
			expectedErrAs:       FetchFailedError{},
			expectedErrContains: "fetch bundle: exit status 128",
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			txManager := transaction.NewTrackingManager()
			cfg, repo, repoPath := setupRepo(t)
			data := tc.setup(t, ctx, cfg, repoPath)

			err := repo.FetchBundle(ctx, txManager, data.reader, &FetchBundleOpts{
				UpdateHead: tc.updateHead,
			})
			if tc.expectedErrAs != nil {
				require.ErrorAs(t, err, &tc.expectedErrAs)
			}
			if tc.expectedErrContains != "" {
				require.ErrorContains(t, err, tc.expectedErrContains)
				return
			}
			require.NoError(t, err)

			headRef, err := repo.GetDefaultBranch(ctx)
			require.NoError(t, err)
			require.Equal(t, data.expectedHeadRef, headRef)

			refs, err := repo.GetReferences(ctx)
			require.NoError(t, err)
			require.Equal(t, data.expectedRefs, refs)
		})
	}
}
