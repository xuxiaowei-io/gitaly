package git_test

import (
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
)

func TestWriteTree(t *testing.T) {
	cfg, repoProto, repoPath := git.Setup(t)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	differentContentBlobID := localrepo.WriteTestBlob(t, repo, "", "different content")
	blobID := localrepo.WriteTestBlob(t, repo, "", "foobar\n")
	treeID := localrepo.WriteTestTree(t, repo, []git.TreeEntry{
		{
			OID:  blobID,
			Mode: "100644",
			Path: "file",
		},
	})

	for _, tc := range []struct {
		desc            string
		entries         []git.TreeEntry
		expectedEntries []git.TreeEntry
	}{
		{
			desc: "entry with blob OID",
			entries: []git.TreeEntry{
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file",
				},
			},
			expectedEntries: []git.TreeEntry{
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file",
				},
			},
		},
		{
			desc: "entry with blob content",
			entries: []git.TreeEntry{
				{
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file",
				},
			},
			expectedEntries: []git.TreeEntry{
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file",
				},
			},
		},
		{
			desc: "entry with tree OID",
			entries: []git.TreeEntry{
				{
					OID:  treeID,
					Mode: "040000",
					Path: "dir",
				},
			},
			expectedEntries: []git.TreeEntry{
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "dir/file",
				},
			},
		},
		{
			desc: "mixed tree and blob entries",
			entries: []git.TreeEntry{
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
					Content: "different content",
					Mode:    "100644",
					Path:    "file2",
				},
			},
			expectedEntries: []git.TreeEntry{
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "dir/file",
				},
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file1",
				},
				{
					OID:     differentContentBlobID,
					Content: "different content",
					Mode:    "100644",
					Path:    "file2",
				},
			},
		},
		{
			desc: "two entries with nonexistant objects",
			entries: []git.TreeEntry{
				{
					OID:  git.ObjectID(strings.Repeat("1", git.DefaultObjectHash.Hash().Size()*2)),
					Mode: "100644",
					Path: "file",
				},
				{
					OID:  git.DefaultObjectHash.ZeroOID,
					Mode: "100644",
					Path: "file",
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid := localrepo.WriteTestTree(t, repo, tc.entries)

			if tc.expectedEntries != nil {
				git.RequireTree(t, cfg, repoPath, oid.String(), tc.expectedEntries)
			}
		})
	}
}
