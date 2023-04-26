package localrepo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "file2",
						OID:      "",
						Stage:    0,
					},
				},
				InfoMessage: "Auto-merging file2\nCONFLICT (add/add): Merge conflict in file2",
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
			output: fmt.Sprintf(`%s
100644 %s 2%sfile
100644 %s 3%sfile

Auto-merging file
CONFLICT (content): Merge conflict in file
`, gittest.DefaultObjectHash.EmptyTreeOID, gittest.DefaultObjectHash.EmptyTreeOID, "\t", gittest.DefaultObjectHash.EmptyTreeOID, "\t"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: &MergeTreeError{
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "file",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "file",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
				},
				InfoMessage: "Auto-merging file\nCONFLICT (content): Merge conflict in file",
			},
		},
		{
			desc: "multiple files conflict",
			output: fmt.Sprintf(`%s
100644 %s 2%sfile1
100644 %s 3%sfile2

Auto-merging file
CONFLICT (content): Merge conflict in file1
`, gittest.DefaultObjectHash.EmptyTreeOID, gittest.DefaultObjectHash.EmptyTreeOID, "\t", gittest.DefaultObjectHash.EmptyTreeOID, "\t"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: &MergeTreeError{
				ConflictingFileInfo: []ConflictingFileInfo{
					{
						FileName: "file1",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageOurs,
					},
					{
						FileName: "file2",
						OID:      gittest.DefaultObjectHash.EmptyTreeOID,
						Stage:    MergeStageTheirs,
					},
				},
				InfoMessage: "Auto-merging file\nCONFLICT (content): Merge conflict in file1",
			},
		},
		{
			desc: "no tab in conflicting file info",
			output: fmt.Sprintf(`%s
100644 %s 2 file1

Auto-merging file
CONFLICT (content): Merge conflict in file1
`, gittest.DefaultObjectHash.EmptyTreeOID, gittest.DefaultObjectHash.EmptyTreeOID),
			oid:         gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("parsing conflicting file info: 100644 %s 2 file1", gittest.DefaultObjectHash.EmptyTreeOID),
		},
		{
			desc: "incorrect number of fields in conflicting file info",
			output: fmt.Sprintf(`%s
%s 2%sfile1

Auto-merging file
CONFLICT (content): Merge conflict in file1
`, gittest.DefaultObjectHash.EmptyTreeOID, gittest.DefaultObjectHash.EmptyTreeOID, "\t"),
			oid:         gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("parsing conflicting file info: %s 2\tfile1", gittest.DefaultObjectHash.EmptyTreeOID),
		},
		{
			desc: "invalid OID in conflicting file info",
			output: fmt.Sprintf(`%s
100644 23 2%sfile1

Auto-merging file
CONFLICT (content): Merge conflict in file1
`, gittest.DefaultObjectHash.EmptyTreeOID, "\t"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("hex to oid: %w", git.InvalidObjectIDLengthError{
				OID:           "23",
				CorrectLength: gittest.DefaultObjectHash.EncodedLen(),
				Length:        2,
			}),
		},
		{
			desc: "invalid stage type in conflicting file info",
			output: fmt.Sprintf(`%s
100644 %s foo%sfile1

Auto-merging file
CONFLICT (content): Merge conflict in file1
`, gittest.DefaultObjectHash.EmptyTreeOID, gittest.DefaultObjectHash.EmptyTreeOID, "\t"),
			oid: gittest.DefaultObjectHash.EmptyTreeOID,
			expectedErr: fmt.Errorf("converting stage to int: %w", &strconv.NumError{
				Func: "Atoi",
				Num:  "foo",
				Err:  errors.New("invalid syntax"),
			}),
		},
		{
			desc: "invalid stage value in conflicting file info",
			output: fmt.Sprintf(`%s
100644 %s 5%sfile1

Auto-merging file
CONFLICT (content): Merge conflict in file1
`, gittest.DefaultObjectHash.EmptyTreeOID, gittest.DefaultObjectHash.EmptyTreeOID, "\t"),
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
