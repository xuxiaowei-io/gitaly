package localrepo

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
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
