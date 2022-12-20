//go:build !gitaly_test_sha256

package tree

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestWriteTree(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := git.CreateRepository(t, ctx, cfg, git.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	differentContentBlobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("different content"))
	require.NoError(t, err)

	blobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("foobar\n"))
	require.NoError(t, err)

	treeID, err := Write(ctx, repo, []*Entry{
		{
			ObjectID: blobID,
			Mode:     []byte("100644"),
			Path:     "file",
			Type:     Blob,
		},
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		desc            string
		entries         []*Entry
		expectedEntries []*Entry
	}{
		{
			desc: "entry with blob OID",
			entries: []*Entry{
				{
					ObjectID: blobID,
					Mode:     []byte("100644"),
					Path:     "file",
					Type:     Blob,
				},
			},
			expectedEntries: []*Entry{
				{
					ObjectID: blobID,
					Mode:     []byte("100644"),
					Path:     "file",
					Type:     Blob,
				},
			},
		},
		{
			desc: "entry with tree OID",
			entries: []*Entry{
				{
					ObjectID: treeID,
					Mode:     []byte("040000"),
					Path:     "dir",
					Type:     Tree,
				},
			},
			expectedEntries: []*Entry{
				{
					ObjectID: blobID,
					Mode:     []byte("100644"),
					Path:     "dir/file",
					Type:     Blob,
				},
			},
		},
		{
			desc: "mixed tree and blob entries",
			entries: []*Entry{
				{
					ObjectID: treeID,
					Mode:     []byte("040000"),
					Path:     "dir",
					Type:     Tree,
				},
				{
					ObjectID: blobID,
					Mode:     []byte("100644"),
					Path:     "file1",
					Type:     Blob,
				},
				{
					ObjectID: differentContentBlobID,
					Mode:     []byte("100644"),
					Path:     "file2",
					Type:     Blob,
				},
			},
			expectedEntries: []*Entry{
				{
					ObjectID: blobID,
					Mode:     []byte("100644"),
					Path:     "dir/file",
					Type:     Blob,
				},
				{
					ObjectID: blobID,
					Mode:     []byte("100644"),
					Path:     "file1",
					Type:     Blob,
				},
				{
					ObjectID: differentContentBlobID,
					Mode:     []byte("100644"),
					Path:     "file2",
					Type:     Blob,
				},
			},
		},
		{
			desc: "two entries with nonexistant objects",
			entries: []*Entry{
				{
					ObjectID: git.ObjectID(strings.Repeat("1", git.ObjectHashSHA1.Hash().Size()*2)),
					Mode:     []byte("100644"),
					Path:     "file",
					Type:     Blob,
				},
				{
					ObjectID: git.ObjectHashSHA1.ZeroOID,
					Mode:     []byte("100644"),
					Path:     "file",
					Type:     Blob,
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := Write(ctx, repo, tc.entries)
			require.NoError(t, err)

			if tc.expectedEntries == nil {
				return
			}

			output := text.ChompBytes(git.Exec(t, cfg, "-C", repoPath, "ls-tree", "-r", string(oid)))

			if len(output) > 0 {
				var actualEntries []*Entry
				for _, line := range bytes.Split([]byte(output), []byte("\n")) {
					// Format: <mode> SP <type> SP <object> TAB <file>
					tabSplit := bytes.Split(line, []byte("\t"))
					require.Len(t, tabSplit, 2)

					spaceSplit := bytes.Split(tabSplit[0], []byte(" "))
					require.Len(t, spaceSplit, 3)

					path := string(tabSplit[1])

					objectID, err := git.ObjectHashSHA1.FromHex(string(spaceSplit[2]))
					require.NoError(t, err)

					var objectType ObjectType
					switch string(spaceSplit[1]) {
					case "blob":
						objectType = Blob
					case "tree":
						objectType = Tree
					case "commit":
						objectType = Submodule
					default:
						t.Errorf("unknowr type %s", spaceSplit[1])
					}

					actualEntries = append(actualEntries, &Entry{
						ObjectID: objectID,
						Mode:     spaceSplit[0],
						Path:     path,
						Type:     objectType,
					})
				}

				require.Equal(t, tc.expectedEntries, actualEntries)
			}
		})
	}
}
