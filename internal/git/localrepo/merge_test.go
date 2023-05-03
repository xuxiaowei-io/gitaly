package localrepo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
			desc:        "no shared ancestors",
			expectedErr: &MergeTreeError{},
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
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "file2",
						OID:      "",
						Stage:    0,
					},
				},
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

			mergeTreeResult, err := repo.MergeTree(ctx, string(ours), string(theirs), WithConflictingFileNamesOnly())

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
		oid         git.ObjectID
		expectedErr error
	}{
		{
			desc: "single file conflict",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"1",
				"a",
				"CONFLICT (contents)",
				"CONFLICT (content): Merge conflict in a\n",
				"",
			}, "\x00"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: &MergeTreeError{
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
				},
				ConflictInfoMessage: []ConflictInfoMessage{
					{
						Paths:   []string{"a"},
						Type:    "Auto-merging",
						Message: "Auto-merging a\n",
					},
					{
						Paths:   []string{"a"},
						Type:    "CONFLICT (contents)",
						Message: "CONFLICT (content): Merge conflict in a\n",
					},
				},
			},
		},
		{
			desc: "single file with ancestor",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s 1\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"1",
				"a",
				"CONFLICT (contents)",
				"CONFLICT (content): Merge conflict in a\n",
				"",
			}, "\x00"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: &MergeTreeError{
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageAncestor,
					},
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
				},
			},
		},
		{
			desc: "single file rename",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s 1\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 2\tc", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\td", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"3",
				"a",
				"c",
				"d",
				"CONFLICT (rename/rename)",
				"CONFLICT (rename/rename): a renamed to c in @ and to d in master.\n",
				"",
			}, "\x00"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: &MergeTreeError{
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageAncestor,
					},
					{
						FileName: "c",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "d",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
				},
			},
		},
		{
			desc: "multiple files conflict with ancestor",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 1\ta",
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 2\ta",
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 3\ta",
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 2\tb",
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 3\tb",
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"1",
				"a",
				"CONFLICT (contents)",
				"CONFLICT (content): Merge conflict in a\n",
				"1",
				"b",
				"Auto-merging",
				"Auto-merging b\n",
				"1",
				"b",
				"CONFLICT (contents)",
				"CONFLICT (content): Merge conflict in b\n",
				"",
			}, "\x00"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: &MergeTreeError{
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageAncestor,
					},
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
					{
						FileName: "b",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "b",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
				},
			},
		},
		{
			desc: "multiple files conflict with file deletion",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 1\ta",
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 3\ta",
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 2\tb",
				"100644 " + gittest.DefaultObjectHash.EmptyTreeOID.String() + " 3\tb",
				"",
				"1",
				"a",
				"CONFLICT (modify/delete)",
				"CONFLICT (modify/delete): a deleted in @ and modified in master.  Version master of a left in tree.\n",
				"1",
				"b",
				"Auto-merging",
				"Auto-merging b\n",
				"1",
				"b",
				"CONFLICT (contents)",
				"CONFLICT (content): Merge conflict in b\n",
				"",
			}, "\x00"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: &MergeTreeError{
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageAncestor,
					},
					{
						FileName: "a",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
					{
						FileName: "b",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "b",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
				},
			},
		},
		{
			desc: "no tab in conflicting file info",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s 1a", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"",
			}, "\x00"),
			oid:         gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("parsing conflicting file info: 100644 %s 1a", gittest.DefaultObjectHash.EmptyTreeOID),
		},
		{
			desc: "incorrect number of fields in conflicting file info",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s 1\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("%s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"",
			}, "\x00"),
			oid:         gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("parsing conflicting file info: %s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
		},
		{
			desc: "invalid OID in conflicting file info",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s 1\ta", "$$$"),
				fmt.Sprintf("100644 %s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"",
			}, "\x00"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("hex to oid: %w", git.InvalidObjectIDLengthError{
				OID:           "$$$",
				CorrectLength: gittest.DefaultObjectHash.EncodedLen(),
				Length:        3,
			}),
		},
		{
			desc: "invalid stage type in conflicting file info",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s foo\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"",
			}, "\x00"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("converting stage to int: %w", &strconv.NumError{
				Func: "Atoi",
				Num:  "foo",
				Err:  errors.New("invalid syntax"),
			}),
		},
		{
			desc: "invalid stage value in conflicting file info",
			output: strings.Join([]string{
				gittest.DefaultObjectHash.EmptyTreeOID.String(),
				fmt.Sprintf("100644 %s 5\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 2\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				fmt.Sprintf("100644 %s 3\ta", gittest.DefaultObjectHash.EmptyTreeOID),
				"",
				"1",
				"a",
				"Auto-merging",
				"Auto-merging a\n",
				"",
			}, "\x00"),
			oid:         gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("invalid value for stage: 5"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := parseMergeTreeError(gittest.DefaultObjectHash, mergeTreeConfig{}, tc.output)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.Equal(t, tc.oid, oid)
			require.NoError(t, err)
		})
	}
}
