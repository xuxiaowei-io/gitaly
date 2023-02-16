package localrepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestMergeTree(t *testing.T) {
	cfg := testcfg.Build(t)

	testCases := []struct {
		desc        string
		expectedErr error
		setupFunc   func(t *testing.T, ctx context.Context, repoPath string) (ours, theirs git.ObjectID, expectedTreeEntries []gittest.TreeEntry)
	}{
		{
			desc: "normal merge",
			setupFunc: func(t *testing.T, ctx context.Context, repoPath string) (git.ObjectID, git.ObjectID, []gittest.TreeEntry) {
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithCommitterName("Andy"),
					gittest.WithAuthorName("Andy"),
					gittest.WithTree(tree1),
				)
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "baz",
					},
				})
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
					{
						Mode:    "100644",
						Path:    "file3",
						Content: "bar",
					},
				})
				ours := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(tree2),
					gittest.WithParents(baseCommit),
					gittest.WithAuthorName("Woody"),
					gittest.WithCommitterName("Woody"),
				)
				theirs := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(tree3),
					gittest.WithParents(baseCommit),
					gittest.WithAuthorName("Buzz"),
					gittest.WithCommitterName("Buzz"),
				)

				return ours, theirs, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "baz",
					},
					{
						Mode:    "100644",
						Path:    "file3",
						Content: "bar",
					},
				}
			},
		},
		{
			desc: "no shared ancestors",
			expectedErr: &MergeTreeError{
				InfoMessage: "unrelated histories",
			},
			setupFunc: func(t *testing.T, ctx context.Context, repoPath string) (git.ObjectID, git.ObjectID, []gittest.TreeEntry) {
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
				})
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "baz",
					},
				})
				ours := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(tree1),
					gittest.WithAuthorName("Woody"),
					gittest.WithCommitterName("Woody"),
				)
				theirs := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(tree2),
					gittest.WithAuthorName("Buzz"),
					gittest.WithCommitterName("Buzz"),
				)

				return ours, theirs, nil
			},
		},
		{
			desc: "with conflicts",
			expectedErr: &MergeTreeError{
				ConflictingFiles: []string{"file2"},
				InfoMessage:      "Auto-merging file2\nCONFLICT (add/add): Merge conflict in file2",
			},
			setupFunc: func(t *testing.T, ctx context.Context, repoPath string) (git.ObjectID, git.ObjectID, []gittest.TreeEntry) {
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithCommitterName("Andy"),
					gittest.WithAuthorName("Andy"),
					gittest.WithTree(tree1),
				)
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "baz",
					},
				})
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "bar",
					},
				})
				ours := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(tree2),
					gittest.WithParents(baseCommit),
					gittest.WithAuthorName("Woody"),
					gittest.WithCommitterName("Woody"),
				)
				theirs := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTree(tree3),
					gittest.WithParents(baseCommit),
					gittest.WithAuthorName("Buzz"),
					gittest.WithCommitterName("Buzz"),
				)

				return ours, theirs, nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := NewTestRepo(t, cfg, repoProto)

			ours, theirs, treeEntries := tc.setupFunc(t, ctx, repoPath)

			mergeTreeResult, err := repo.MergeTree(ctx, string(ours), string(theirs))

			require.Equal(t, tc.expectedErr, err)
			if tc.expectedErr == nil {
				gittest.RequireTree(
					t,
					cfg,
					repoPath,
					string(mergeTreeResult),
					treeEntries,
				)
			}
		})
	}

	t.Run("allow unrelated histories", func(t *testing.T) {
		ctx := testhelper.Context(t)

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := NewTestRepo(t, cfg, repoProto)

		tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{
				Mode:    "100644",
				Path:    "file1",
				Content: "foo",
			},
		})
		tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{
				Mode:    "100644",
				Path:    "file2",
				Content: "baz",
			},
		})
		ours := gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithTree(tree1),
			gittest.WithAuthorName("Woody"),
			gittest.WithCommitterName("Woody"),
		)
		theirs := gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithTree(tree2),
			gittest.WithAuthorName("Buzz"),
			gittest.WithCommitterName("Buzz"),
		)

		mergeTreeResult, err := repo.MergeTree(
			ctx,
			string(ours),
			string(theirs),
			WithAllowUnrelatedHistories(),
		)
		require.NoError(t, err)

		gittest.RequireTree(
			t,
			cfg,
			repoPath,
			string(mergeTreeResult),
			[]gittest.TreeEntry{
				{
					Mode:    "100644",
					Path:    "file1",
					Content: "foo",
				},
				{
					Mode:    "100644",
					Path:    "file2",
					Content: "baz",
				},
			},
		)
	})
}

func TestParseResult(t *testing.T) {
	testCases := []struct {
		desc        string
		output      string
		oid         string
		expectedErr *MergeTreeError
	}{
		{
			desc: "single file conflict",
			output: `4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4
file

Auto-merging file
CONFLICT (content): Merge conflict in file
`,
			oid: "4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4",
			expectedErr: &MergeTreeError{
				ConflictingFiles: []string{
					"file",
				},
				InfoMessage: "Auto-merging file\nCONFLICT (content): Merge conflict in file",
			},
		},
		{
			desc: "multiple files conflict",
			output: `4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4
file1
file2

Auto-merging file
CONFLICT (content): Merge conflict in file1
`,
			oid: "4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4",
			expectedErr: &MergeTreeError{
				ConflictingFiles: []string{
					"file1",
					"file2",
				},
				InfoMessage: "Auto-merging file\nCONFLICT (content): Merge conflict in file1",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := parseMergeTreeError(tc.output)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
