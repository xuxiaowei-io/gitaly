package localrepo

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestWriteTree(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := NewTestRepo(t, cfg, repoProto)

	differentContentBlobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("different content"))
	require.NoError(t, err)

	blobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("foobar\n"))
	require.NoError(t, err)

	treeID, err := repo.WriteTree(ctx, []TreeEntry{
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
		desc            string
		entries         []TreeEntry
		expectedEntries []TreeEntry
	}{
		{
			desc: "entry with blob OID",
			entries: []TreeEntry{
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
			entries: []TreeEntry{
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
			entries: []TreeEntry{
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
			entries: []TreeEntry{
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
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			oid, err := repo.WriteTree(ctx, tc.entries)
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
