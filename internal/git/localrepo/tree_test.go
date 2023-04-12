package localrepo

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestWriteTree(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	gitVersion, err := gittest.NewCommandFactory(t, cfg).GitVersion(ctx)
	require.NoError(t, err)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := NewTestRepo(t, cfg, repoProto)

	differentContentBlobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("different content"))
	require.NoError(t, err)

	blobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("foobar\n"))
	require.NoError(t, err)

	treeID, err := repo.WriteTree(ctx, []*TreeEntry{
		{
			OID:  blobID,
			Mode: "100644",
			Path: "file",
		},
	})
	require.NoError(t, err)

	nonExistentBlobID, err := repo.WriteBlob(ctx, "file1", bytes.NewBufferString("content"))
	require.NoError(t, err)

	nonExistentBlobPath := filepath.Join(repoPath, "objects", string(nonExistentBlobID)[0:2], string(nonExistentBlobID)[2:])
	require.NoError(t, os.Remove(nonExistentBlobPath))

	for _, tc := range []struct {
		desc              string
		entries           []*TreeEntry
		expectedEntries   []TreeEntry
		expectedErrString string
	}{
		{
			desc: "entry with blob OID",
			entries: []*TreeEntry{
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file",
				},
			},
		},
		{
			desc: "entry with tree OID",
			entries: []*TreeEntry{
				{
					OID:  treeID,
					Mode: "040000",
					Path: "dir",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:  blobID,
					Mode: "100644",
					Path: "dir/file",
				},
			},
		},
		{
			desc: "mixed tree and blob entries",
			entries: []*TreeEntry{
				{
					OID:  treeID,
					Mode: "040000",
					Path: "dir",
				},
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file1",
				},
				{
					OID:  differentContentBlobID,
					Mode: "100644",
					Path: "file2",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:  blobID,
					Mode: "100644",
					Path: "dir/file",
				},
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file1",
				},
				{
					OID:  differentContentBlobID,
					Mode: "100644",
					Path: "file2",
				},
			},
		},
		{
			desc: "entry with nonexistent object",
			entries: []*TreeEntry{
				{
					OID:  nonExistentBlobID,
					Mode: "100644",
					Path: "file",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:  nonExistentBlobID,
					Mode: "100644",
					Path: "file",
				},
			},
		},
		{
			desc: "entry with duplicate file",
			entries: []*TreeEntry{
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file",
				},
				{
					OID:  nonExistentBlobID,
					Mode: "100644",
					Path: "file",
				},
			},
			expectedErrString: "duplicateEntries: contains duplicate file entries",
		},
		{
			desc: "entry with malformed mode",
			entries: []*TreeEntry{
				{
					OID:  blobID,
					Mode: "1006442",
					Path: "file",
				},
			},
			expectedErrString: "badFilemode: contains bad file modes",
		},
		{
			desc: "tries to write .git file",
			entries: []*TreeEntry{
				{
					OID:  blobID,
					Mode: "040000",
					Path: ".git",
				},
			},
			expectedErrString: "hasDotgit: contains '.git'",
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			oid, err := repo.WriteTree(ctx, tc.entries)
			if tc.expectedErrString != "" {
				if gitVersion.HashObjectFsck() {
					switch e := err.(type) {
					case structerr.Error:
						stderr := e.Metadata()["stderr"].(string)
						strings.Contains(stderr, tc.expectedErrString)
					default:
						strings.Contains(err.Error(), tc.expectedErrString)
					}
				}
				return
			}

			require.NoError(t, err)

			output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "ls-tree", "-r", string(oid)))

			if len(output) > 0 {
				var actualEntries []TreeEntry
				for _, line := range bytes.Split([]byte(output), []byte("\n")) {
					// Format: <mode> SP <type> SP <object> TAB <file>
					tabSplit := bytes.Split(line, []byte("\t"))
					require.Len(t, tabSplit, 2)

					spaceSplit := bytes.Split(tabSplit[0], []byte(" "))
					require.Len(t, spaceSplit, 3)

					path := string(tabSplit[1])

					objectHash, err := repo.ObjectHash(ctx)
					require.NoError(t, err)

					objectID, err := objectHash.FromHex(text.ChompBytes(spaceSplit[2]))
					require.NoError(t, err)

					actualEntries = append(actualEntries, TreeEntry{
						OID:  objectID,
						Mode: string(spaceSplit[0]),
						Path: path,
					})
				}

				require.Equal(t, tc.expectedEntries, actualEntries)
				return
			}

			require.Nil(t, tc.expectedEntries)
		})
	}
}

func TestTreeEntryByPath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc     string
		input    []*TreeEntry
		expected []*TreeEntry
	}{
		{
			desc: "all blobs",
			input: []*TreeEntry{
				{
					Type: Blob,
					Path: "abc",
				},
				{
					Type: Blob,
					Path: "ab",
				},
				{
					Type: Blob,
					Path: "a",
				},
			},
			expected: []*TreeEntry{
				{
					Type: Blob,
					Path: "a",
				},
				{
					Type: Blob,
					Path: "ab",
				},
				{
					Type: Blob,
					Path: "abc",
				},
			},
		},
		{
			desc: "blobs and trees",
			input: []*TreeEntry{
				{
					Type: Blob,
					Path: "abc",
				},
				{
					Type: Tree,
					Path: "ab",
				},
				{
					Type: Blob,
					Path: "a",
				},
			},
			expected: []*TreeEntry{
				{
					Type: Blob,
					Path: "a",
				},
				{
					Type: Tree,
					Path: "ab",
				},
				{
					Type: Blob,
					Path: "abc",
				},
			},
		},
		{
			desc: "trees get sorted with / appended",
			input: []*TreeEntry{
				{
					Type: Tree,
					Path: "a",
				},
				{
					Type: Blob,
					Path: "a+",
				},
			},
			expected: []*TreeEntry{
				{
					Type: Blob,
					Path: "a+",
				},
				{
					Type: Tree,
					Path: "a",
				},
			},
		},
		{
			desc: "blobs get sorted without / appended",
			input: []*TreeEntry{
				{
					Type: Blob,
					Path: "a",
				},
				{
					Type: Blob,
					Path: "a+",
				},
			},
			expected: []*TreeEntry{
				{
					Type: Blob,
					Path: "a",
				},
				{
					Type: Blob,
					Path: "a+",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			sort.Stable(TreeEntriesByPath(tc.input))

			require.Equal(
				t,
				tc.expected,
				tc.input,
			)
		})
	}
}

func TestGetTree(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := NewTestRepo(t, cfg, repoProto)

	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("blob contents"))
	emptyTreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{})
	treeWithBlob := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{OID: blobID, Mode: "100644", Path: "nonexecutable"},
		{OID: blobID, Mode: "100755", Path: "executable"},
	})
	treeWithSubtree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{OID: emptyTreeID, Mode: "040000", Path: "subdir"},
	})
	treeWithNestedSubtrees := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{OID: treeWithSubtree, Mode: "040000", Path: "nested-subdir"},
	})
	treeWithSubtreeAndBlob := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{OID: treeWithSubtree, Mode: "040000", Path: "subdir"},
		{OID: blobID, Mode: "100644", Path: "blob"},
	})
	treeWithSubtreeContainingBlob := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{OID: treeWithSubtreeAndBlob, Mode: "040000", Path: "subdir"},
	})

	for _, tc := range []struct {
		desc            string
		treeish         git.Revision
		cfg             *getTreeConfig
		expectedResults []*TreeEntry
		expectedErr     error
	}{
		{
			desc:    "empty tree",
			treeish: emptyTreeID.Revision(),
		},
		{
			desc:    "tree with blob",
			treeish: treeWithBlob.Revision(),
			expectedResults: []*TreeEntry{
				{Mode: "100755", Type: Blob, OID: blobID, Path: "executable"},
				{Mode: "100644", Type: Blob, OID: blobID, Path: "nonexecutable"},
			},
		},
		{
			desc:    "tree with subtree",
			treeish: treeWithSubtree.Revision(),
			expectedResults: []*TreeEntry{
				{Mode: "040000", Type: Tree, OID: emptyTreeID, Path: "subdir"},
			},
		},
		{
			desc:    "nested trees",
			treeish: treeWithNestedSubtrees.Revision(),
			expectedResults: []*TreeEntry{
				{Mode: "040000", Type: Tree, OID: treeWithSubtree, Path: "nested-subdir"},
			},
		},
		{
			desc:    "recursive nested trees",
			treeish: treeWithNestedSubtrees.Revision(),
			cfg: &getTreeConfig{
				recursive: true,
			},
			expectedResults: []*TreeEntry{
				{Mode: "040000", Type: Tree, OID: treeWithSubtree, Path: "nested-subdir"},
				{Mode: "040000", Type: Tree, OID: emptyTreeID, Path: "nested-subdir/subdir"},
			},
		},
		{
			desc:    "nested subtree",
			treeish: treeWithNestedSubtrees.Revision(),
			cfg: &getTreeConfig{
				relativePath: "nested-subdir",
			},
			expectedResults: []*TreeEntry{
				{Mode: "040000", Type: Tree, OID: emptyTreeID, Path: "subdir"},
			},
		},
		{
			desc:    "nested recursive subtree",
			treeish: treeWithSubtreeContainingBlob.Revision(),
			cfg: &getTreeConfig{
				relativePath: "subdir",
				recursive:    true,
			},
			expectedResults: []*TreeEntry{
				{Mode: "100644", Type: Blob, OID: blobID, Path: "blob"},
				{Mode: "040000", Type: Tree, OID: treeWithSubtree, Path: "subdir"},
				{Mode: "040000", Type: Tree, OID: emptyTreeID, Path: "subdir/subdir"},
			},
		},
		{
			desc:    "recursive nested trees and blobs",
			treeish: treeWithSubtreeAndBlob.Revision(),
			cfg: &getTreeConfig{
				recursive: true,
			},
			expectedResults: []*TreeEntry{
				{Mode: "100644", Type: Blob, OID: blobID, Path: "blob"},
				{Mode: "040000", Type: Tree, OID: treeWithSubtree, Path: "subdir"},
				{Mode: "040000", Type: Tree, OID: emptyTreeID, Path: "subdir/subdir"},
			},
		},
		{
			desc:    "listing blob fails",
			treeish: blobID.Revision(),
			// We get a NotExist error here because it's invalid to suffix an object ID
			// which resolves to a blob with a colon (":") given that it's not possible
			// to resolve a subpath.
			expectedErr: git.ErrReferenceNotFound,
		},
		{
			desc:    "valid revision with invalid path",
			treeish: treeWithSubtree.Revision(),
			cfg: &getTreeConfig{
				relativePath: "does-not-exist",
			},
			expectedErr: git.ErrReferenceNotFound,
		},
		{
			desc:    "valid revision with path pointing to blob",
			treeish: treeWithSubtreeAndBlob.Revision(),
			cfg: &getTreeConfig{
				relativePath: "blob",
			},
			expectedErr: ErrNotTreeish,
		},
		{
			desc:        "listing nonexistent object fails",
			treeish:     "does-not-exist",
			expectedErr: git.ErrReferenceNotFound,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var options []GetTreeOption
			if tc.cfg != nil && tc.cfg.recursive {
				options = append(options, WithRecursive())
			}
			if tc.cfg != nil && tc.cfg.relativePath != "" {
				options = append(options, WithRelativePath(tc.cfg.relativePath))
			}
			tree, err := repo.GetTree(ctx, tc.treeish, options...)
			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				return
			}

			results := tree.Entries
			require.Equal(t, tc.expectedResults, results)
		})
	}
}

func TestGetTree_WithWalk(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	testCases := []struct {
		desc        string
		setupTree   func(t *testing.T, repoPath string) (git.ObjectID, TreeEntry)
		expectedErr error
	}{
		{
			desc: "flat tree",
			setupTree: func(t *testing.T, repoPath string) (git.ObjectID, TreeEntry) {
				blobA := gittest.WriteBlob(t, cfg, repoPath, []byte("a"))
				blobB := gittest.WriteBlob(t, cfg, repoPath, []byte("b"))

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode: "100644",
						Path: "fileA",
						OID:  blobA,
					},
					{
						Mode: "100644",
						Path: "fileB",
						OID:  blobB,
					},
				})
				return gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"),
						gittest.WithTree(treeID)),
					TreeEntry{
						OID:  treeID,
						Mode: "040000",
						Type: Tree,
						Entries: []*TreeEntry{
							{
								Mode: "100644",
								Path: "fileA",
								OID:  blobA,
								Type: Blob,
							},
							{
								Mode: "100644",
								Path: "fileB",
								OID:  blobB,
								Type: Blob,
							},
						},
					}
			},
		},
		{
			desc: "nested tree",
			setupTree: func(t *testing.T, repoPath string) (git.ObjectID, TreeEntry) {
				blobA := gittest.WriteBlob(t, cfg, repoPath, []byte("a"))
				blobB := gittest.WriteBlob(t, cfg, repoPath, []byte("b"))
				blobC := gittest.WriteBlob(t, cfg, repoPath, []byte("c"))
				dirATree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode: "100644",
						Path: "file1InDirA",
						OID:  blobA,
					},
				})

				treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode: "100644",
						Path: "fileB",
						OID:  blobB,
					},
					{
						Mode: "100644",
						Path: "fileC",
						OID:  blobC,
					},
					{
						Mode: "040000",
						Path: "dirA",
						OID:  dirATree,
					},
				})

				return gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"),
						gittest.WithTree(treeID)),
					TreeEntry{
						OID:  treeID,
						Mode: "040000",
						Type: Tree,
						Entries: []*TreeEntry{
							{
								Mode: "040000",
								Path: "dirA",
								Type: Tree,
								OID:  dirATree,
								Entries: []*TreeEntry{
									{
										Mode: "100644",
										Path: "file1InDirA",
										OID:  blobA,
										Type: Blob,
									},
								},
							},
							{
								Mode: "100644",
								Path: "fileB",
								OID:  blobB,
								Type: Blob,
							},
							{
								Mode: "100644",
								Path: "fileC",
								OID:  blobC,
								Type: Blob,
							},
						},
					}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(
				t,
				ctx,
				cfg,
				gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
			repo := NewTestRepo(t, cfg, repoProto)

			commitID, expectedTree := tc.setupTree(t, repoPath)

			tree, err := repo.GetTree(
				ctx,
				git.Revision(commitID),
				WithWalk(),
			)
			require.NoError(t, err)
			require.Equal(t, expectedTree, *tree)
		})
	}
}
