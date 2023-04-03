package localrepo

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
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
					require.Contains(t, err.Error(), tc.expectedErrString)
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

func TestWriteTreeRecursively(t *testing.T) {
	cfg := testcfg.Build(t)

	testCases := []struct {
		desc      string
		setupTree func(*testing.T, string) *TreeEntry
	}{
		{
			desc: "every level has a new tree",
			setupTree: func(t *testing.T, repoPath string) *TreeEntry {
				blobA := gittest.WriteBlob(t, cfg, repoPath, []byte("a"))

				return &TreeEntry{
					OID:  "",
					Type: Tree,
					Mode: "040000",
					Entries: []*TreeEntry{
						{
							OID:  "",
							Type: Tree,
							Mode: "040000",
							Path: "dirA",
							Entries: []*TreeEntry{
								{
									OID:  "",
									Mode: "040000",
									Type: Tree,
									Path: "dirB",
									Entries: []*TreeEntry{
										{
											OID:  blobA,
											Type: Blob,
											Mode: "100644",
											Path: "file1",
										},
									},
								},
							},
						},
					},
				}
			},
		},
		{
			desc: "only some new trees",
			setupTree: func(t *testing.T, repoPath string) *TreeEntry {
				blobA := gittest.WriteBlob(t, cfg, repoPath, []byte("a"))
				blobB := gittest.WriteBlob(t, cfg, repoPath, []byte("b"))
				blobC := gittest.WriteBlob(t, cfg, repoPath, []byte("c"))
				blobD := gittest.WriteBlob(t, cfg, repoPath, []byte("d"))
				dirDTree := gittest.WriteTree(
					t,
					cfg,
					repoPath,
					[]gittest.TreeEntry{
						{
							OID:  blobC,
							Mode: "100644",
							Path: "file3",
						},
						{
							OID:  blobD,
							Mode: "100644",
							Path: "file4",
						},
					})
				dirCTree := gittest.WriteTree(
					t,
					cfg,
					repoPath,
					[]gittest.TreeEntry{
						{
							OID:  dirDTree,
							Mode: "040000",
							Path: "dirD",
						},
					},
				)

				return &TreeEntry{
					OID:  "",
					Type: Tree,
					Mode: "040000",
					Entries: []*TreeEntry{
						{
							OID:  "",
							Type: Tree,
							Mode: "040000",
							Path: "dirA",
							Entries: []*TreeEntry{
								{
									OID:  "",
									Mode: "040000",
									Type: Tree,
									Path: "dirB",
									Entries: []*TreeEntry{
										{
											OID:  blobA,
											Type: Blob,
											Mode: "100644",
											Path: "file1",
										},
										{
											OID:  blobB,
											Type: Blob,
											Mode: "100644",
											Path: "file2",
										},
									},
								},
							},
						},
						{
							OID:  dirCTree,
							Type: Tree,
							Mode: "040000",
							Path: "dirC",
							Entries: []*TreeEntry{
								{
									OID:  dirDTree,
									Mode: "040000",
									Type: Tree,
									Path: "dirD",
									Entries: []*TreeEntry{
										{
											OID:  blobC,
											Type: Blob,
											Mode: "100644",
											Path: "file3",
										},
										{
											OID:  blobD,
											Type: Blob,
											Mode: "100644",
											Path: "file4",
										},
									},
								},
							},
						},
					},
				}
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

			treeEntry := tc.setupTree(t, repoPath)
			treeOID, err := repo.WriteTreeRecursively(
				ctx,
				treeEntry,
			)
			require.NoError(t, err)

			requireTreeEquals(t, ctx, repo, treeOID, treeEntry)
		})
	}
}

func requireTreeEquals(
	t *testing.T,
	ctx context.Context,
	repo *Repo,
	treeOID git.ObjectID,
	tree *TreeEntry,
) {
	entries, err := repo.ListEntries(ctx, git.Revision(treeOID), &ListEntriesConfig{})
	require.NoError(t, err)

	for i, entry := range entries {
		if tree.Entries[i].OID != "" {
			require.Equal(t, tree.Entries[i].Mode, entry.Mode)
			require.Equal(t, tree.Entries[i].Type, entry.Type)
			require.Equal(t, tree.Entries[i].OID, entry.OID)
			require.Equal(t, tree.Entries[i].Path, entry.Path)
		} else {
			objectInfo, err := repo.ReadObjectInfo(
				ctx,
				git.Revision(entry.OID),
			)
			require.NoError(t, err)

			require.Equal(t, tree.Entries[i].Type, ToEnum(objectInfo.Type))
		}

		if entry.Type == Tree {
			requireTreeEquals(t, ctx, repo, entry.OID, tree.Entries[i])
		}
	}
}

func TestGetFullTree(t *testing.T) {
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

			fullTree, err := repo.GetFullTree(
				ctx,
				git.Revision(commitID),
			)
			require.NoError(t, err)
			require.Equal(t, expectedTree, *fullTree)
		})
	}
}
