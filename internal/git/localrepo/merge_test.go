package localrepo

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestMergeTree(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	type setupData struct {
		ours   git.ObjectID
		theirs git.ObjectID

		expectedTreeEntries []gittest.TreeEntry
		expectedErr         error
	}

	testCases := []struct {
		desc             string
		mergeTreeOptions []MergeTreeOption
		setup            func(t *testing.T, repoPath string) setupData
	}{
		{
			desc: "normal merge",
			mergeTreeOptions: []MergeTreeOption{
				WithConflictingFileNamesOnly(),
			},
			setup: func(t *testing.T, repoPath string) setupData {
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))
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
				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2), gittest.WithParents(baseCommit))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree3), gittest.WithParents(baseCommit))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedTreeEntries: []gittest.TreeEntry{
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
					},
				}
			},
		},
		{
			desc: "no shared ancestors",
			mergeTreeOptions: []MergeTreeOption{
				WithConflictingFileNamesOnly(),
			},
			setup: func(t *testing.T, repoPath string) setupData {
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
				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2))

				return setupData{
					ours:        ours,
					theirs:      theirs,
					expectedErr: ErrMergeTreeUnrelatedHistory,
				}
			},
		},
		{
			desc: "with single file conflict",
			setup: func(t *testing.T, repoPath string) setupData {
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("baz"))
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
					{
						OID:  blob1,
						Mode: "100644",
						Path: "file2",
					},
				})
				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
					{
						OID:  blob2,
						Mode: "100644",
						Path: "file2",
					},
				})
				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2), gittest.WithParents(baseCommit))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree3), gittest.WithParents(baseCommit))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedErr: &MergeTreeConflictError{
						ConflictingFileInfo: []ConflictingFileInfo{
							{
								FileName: "file2",
								OID:      blob1,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
							{
								FileName: "file2",
								OID:      blob2,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
						},
						ConflictInfoMessage: []ConflictInfoMessage{
							{
								Paths:   []string{"file2"},
								Type:    "Auto-merging",
								Message: "Auto-merging file2\n",
							},
							{
								Paths:   []string{"file2"},
								Type:    "CONFLICT (contents)",
								Message: "CONFLICT (add/add): Merge conflict in file2\n",
							},
						},
					},
				}
			},
		},
		{
			desc: "with single file conflict with ancestor",
			setup: func(t *testing.T, repoPath string) setupData {
				blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob1,
						Mode: "100644",
						Path: "file1",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))

				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("baz"))
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob2,
						Mode: "100644",
						Path: "file1",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "goo",
					},
				})

				blob3 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob3,
						Mode: "100644",
						Path: "file1",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "goo",
					},
				})

				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2), gittest.WithParents(baseCommit))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree3), gittest.WithParents(baseCommit))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedErr: &MergeTreeConflictError{
						ConflictingFileInfo: []ConflictingFileInfo{
							{
								FileName: "file1",
								OID:      blob1,
								Stage:    MergeStageAncestor,
								Mode:     0o100644,
							},
							{
								FileName: "file1",
								OID:      blob2,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
							{
								FileName: "file1",
								OID:      blob3,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
						},
						ConflictInfoMessage: []ConflictInfoMessage{
							{
								Paths:   []string{"file1"},
								Type:    "Auto-merging",
								Message: "Auto-merging file1\n",
							},
							{
								Paths:   []string{"file1"},
								Type:    "CONFLICT (contents)",
								Message: "CONFLICT (content): Merge conflict in file1\n",
							},
						},
					},
				}
			},
		},
		{
			desc: "with single file conflict due to rename",
			setup: func(t *testing.T, repoPath string) setupData {
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))

				blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob2,
						Mode: "100644",
						Path: "file2",
					},
				})

				blob3 := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob3,
						Mode: "100644",
						Path: "file3",
					},
				})

				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2), gittest.WithParents(baseCommit))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree3), gittest.WithParents(baseCommit))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedErr: &MergeTreeConflictError{
						ConflictingFileInfo: []ConflictingFileInfo{
							{
								FileName: "file2",
								OID:      blob2,
								Stage:    MergeStageAncestor,
								Mode:     0o100644,
							},
							{
								FileName: "file2",
								OID:      blob2,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
						},
						ConflictInfoMessage: []ConflictInfoMessage{
							{
								Paths:   []string{"file2", "file1"},
								Type:    "CONFLICT (rename/delete)",
								Message: fmt.Sprintf("CONFLICT (rename/delete): file1 renamed to file2 in %s, but deleted in %s.\n", ours, theirs),
							},
						},
					},
				}
			},
		},
		{
			desc: "with multiple file conflicts",
			setup: func(t *testing.T, repoPath string) setupData {
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "foo",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))

				blob2a := gittest.WriteBlob(t, cfg, repoPath, []byte("baz"))
				blob2b := gittest.WriteBlob(t, cfg, repoPath, []byte("xyz"))
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob2a,
						Mode: "100644",
						Path: "file2",
					},
					{
						OID:  blob2b,
						Mode: "100644",
						Path: "file3",
					},
				})

				blob3a := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))
				blob3b := gittest.WriteBlob(t, cfg, repoPath, []byte("mno"))
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob3a,
						Mode: "100644",
						Path: "file2",
					},
					{
						OID:  blob3b,
						Mode: "100644",
						Path: "file3",
					},
				})

				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2), gittest.WithParents(baseCommit))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree3), gittest.WithParents(baseCommit))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedErr: &MergeTreeConflictError{
						ConflictingFileInfo: []ConflictingFileInfo{
							{
								FileName: "file2",
								OID:      blob2a,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
							{
								FileName: "file2",
								OID:      blob3a,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
							{
								FileName: "file3",
								OID:      blob2b,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
							{
								FileName: "file3",
								OID:      blob3b,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
						},
						ConflictInfoMessage: []ConflictInfoMessage{
							{
								Paths:   []string{"file2"},
								Type:    "Auto-merging",
								Message: "Auto-merging file2\n",
							},
							{
								Paths:   []string{"file2"},
								Type:    "CONFLICT (contents)",
								Message: "CONFLICT (add/add): Merge conflict in file2\n",
							},
							{
								Paths:   []string{"file3"},
								Type:    "Auto-merging",
								Message: "Auto-merging file3\n",
							},
							{
								Paths:   []string{"file3"},
								Type:    "CONFLICT (contents)",
								Message: "CONFLICT (add/add): Merge conflict in file3\n",
							},
						},
					},
				}
			},
		},
		{
			desc: "with multiple file conflicts and common ancestor",
			setup: func(t *testing.T, repoPath string) setupData {
				blob := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob,
						Mode: "100644",
						Path: "file1",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))

				blob2a := gittest.WriteBlob(t, cfg, repoPath, []byte("baz"))
				blob2b := gittest.WriteBlob(t, cfg, repoPath, []byte("xyz"))
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob2a,
						Mode: "100644",
						Path: "file1",
					},
					{
						OID:  blob2b,
						Mode: "100644",
						Path: "file2",
					},
				})

				blob3a := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))
				blob3b := gittest.WriteBlob(t, cfg, repoPath, []byte("mno"))
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob3a,
						Mode: "100644",
						Path: "file1",
					},
					{
						OID:  blob3b,
						Mode: "100644",
						Path: "file2",
					},
				})

				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2), gittest.WithParents(baseCommit))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree3), gittest.WithParents(baseCommit))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedErr: &MergeTreeConflictError{
						ConflictingFileInfo: []ConflictingFileInfo{
							{
								FileName: "file1",
								OID:      blob,
								Stage:    MergeStageAncestor,
								Mode:     0o100644,
							},
							{
								FileName: "file1",
								OID:      blob2a,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
							{
								FileName: "file1",
								OID:      blob3a,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
							{
								FileName: "file2",
								OID:      blob2b,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
							{
								FileName: "file2",
								OID:      blob3b,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
						},
						ConflictInfoMessage: []ConflictInfoMessage{
							{
								Paths:   []string{"file1"},
								Type:    "Auto-merging",
								Message: "Auto-merging file1\n",
							},
							{
								Paths:   []string{"file1"},
								Type:    "CONFLICT (contents)",
								Message: "CONFLICT (content): Merge conflict in file1\n",
							},
							{
								Paths:   []string{"file2"},
								Type:    "Auto-merging",
								Message: "Auto-merging file2\n",
							},
							{
								Paths:   []string{"file2"},
								Type:    "CONFLICT (contents)",
								Message: "CONFLICT (add/add): Merge conflict in file2\n",
							},
						},
					},
				}
			},
		},
		{
			desc: "with multiple file conflicts due to file deletion",
			setup: func(t *testing.T, repoPath string) setupData {
				blob := gittest.WriteBlob(t, cfg, repoPath, []byte("foo"))
				tree1 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob,
						Mode: "100644",
						Path: "file1",
					},
				})
				baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))

				blob2b := gittest.WriteBlob(t, cfg, repoPath, []byte("xyz"))
				tree2 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob2b,
						Mode: "100644",
						Path: "file2",
					},
				})

				blob3a := gittest.WriteBlob(t, cfg, repoPath, []byte("bar"))
				blob3b := gittest.WriteBlob(t, cfg, repoPath, []byte("mno"))
				tree3 := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						OID:  blob3a,
						Mode: "100644",
						Path: "file1",
					},
					{
						OID:  blob3b,
						Mode: "100644",
						Path: "file2",
					},
				})

				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2), gittest.WithParents(baseCommit))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree3), gittest.WithParents(baseCommit))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedErr: &MergeTreeConflictError{
						ConflictingFileInfo: []ConflictingFileInfo{
							{
								FileName: "file1",
								OID:      blob,
								Stage:    MergeStageAncestor,
								Mode:     0o100644,
							},
							{
								FileName: "file1",
								OID:      blob3a,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
							{
								FileName: "file2",
								OID:      blob2b,
								Stage:    MergeStageOurs,
								Mode:     0o100644,
							},
							{
								FileName: "file2",
								OID:      blob3b,
								Stage:    MergeStageTheirs,
								Mode:     0o100644,
							},
						},
						ConflictInfoMessage: []ConflictInfoMessage{
							{
								Paths:   []string{"file1"},
								Type:    "CONFLICT (modify/delete)",
								Message: fmt.Sprintf("CONFLICT (modify/delete): file1 deleted in %s and modified in %s.  Version %s of file1 left in tree.\n", ours, theirs, theirs),
							},
							{
								Paths:   []string{"file2"},
								Type:    "Auto-merging",
								Message: "Auto-merging file2\n",
							},
							{
								Paths:   []string{"file2"},
								Type:    "CONFLICT (contents)",
								Message: "CONFLICT (add/add): Merge conflict in file2\n",
							},
						},
					},
				}
			},
		},
		{
			desc: "allow unrelated histories",
			mergeTreeOptions: []MergeTreeOption{
				WithConflictingFileNamesOnly(),
				WithAllowUnrelatedHistories(),
			},
			setup: func(t *testing.T, repoPath string) setupData {
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
				ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree1))
				theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(tree2))

				return setupData{
					ours:   ours,
					theirs: theirs,
					expectedTreeEntries: []gittest.TreeEntry{
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
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := NewTestRepo(t, cfg, repoProto)

			data := tc.setup(t, repoPath)

			mergeTreeResult, err := repo.MergeTree(ctx, string(data.ours), string(data.theirs), tc.mergeTreeOptions...)

			require.Equal(t, data.expectedErr, err)
			if data.expectedErr == nil {
				gittest.RequireTree(
					t,
					cfg,
					repoPath,
					string(mergeTreeResult),
					data.expectedTreeEntries,
				)
			}
		})
	}
}

func TestParseResult(t *testing.T) {
	testCases := []struct {
		desc        string
		output      string
		oid         git.ObjectID
		expectedErr error
	}{
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
