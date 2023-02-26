package lstree

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestListEntries(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

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
		cfg             *ListEntriesConfig
		expectedResults []*localrepo.TreeEntry
		expectedErr     error
	}{
		{
			desc:    "empty tree",
			treeish: emptyTreeID.Revision(),
		},
		{
			desc:    "tree with blob",
			treeish: treeWithBlob.Revision(),
			expectedResults: []*localrepo.TreeEntry{
				{Mode: "100755", Type: localrepo.Blob, OID: blobID, Path: "executable"},
				{Mode: "100644", Type: localrepo.Blob, OID: blobID, Path: "nonexecutable"},
			},
		},
		{
			desc:    "tree with subtree",
			treeish: treeWithSubtree.Revision(),
			expectedResults: []*localrepo.TreeEntry{
				{Mode: "040000", Type: localrepo.Tree, OID: emptyTreeID, Path: "subdir"},
			},
		},
		{
			desc:    "nested trees",
			treeish: treeWithNestedSubtrees.Revision(),
			expectedResults: []*localrepo.TreeEntry{
				{Mode: "040000", Type: localrepo.Tree, OID: treeWithSubtree, Path: "nested-subdir"},
			},
		},
		{
			desc:    "recursive nested trees",
			treeish: treeWithNestedSubtrees.Revision(),
			cfg: &ListEntriesConfig{
				Recursive: true,
			},
			expectedResults: []*localrepo.TreeEntry{
				{Mode: "040000", Type: localrepo.Tree, OID: treeWithSubtree, Path: "nested-subdir"},
				{Mode: "040000", Type: localrepo.Tree, OID: emptyTreeID, Path: "nested-subdir/subdir"},
			},
		},
		{
			desc:    "nested subtree",
			treeish: treeWithNestedSubtrees.Revision(),
			cfg: &ListEntriesConfig{
				RelativePath: "nested-subdir",
			},
			expectedResults: []*localrepo.TreeEntry{
				{Mode: "040000", Type: localrepo.Tree, OID: emptyTreeID, Path: "subdir"},
			},
		},
		{
			desc:    "nested recursive subtree",
			treeish: treeWithSubtreeContainingBlob.Revision(),
			cfg: &ListEntriesConfig{
				RelativePath: "subdir",
				Recursive:    true,
			},
			expectedResults: []*localrepo.TreeEntry{
				{Mode: "100644", Type: localrepo.Blob, OID: blobID, Path: "blob"},
				{Mode: "040000", Type: localrepo.Tree, OID: treeWithSubtree, Path: "subdir"},
				{Mode: "040000", Type: localrepo.Tree, OID: emptyTreeID, Path: "subdir/subdir"},
			},
		},
		{
			desc:    "recursive nested trees and blobs",
			treeish: treeWithSubtreeAndBlob.Revision(),
			cfg: &ListEntriesConfig{
				Recursive: true,
			},
			expectedResults: []*localrepo.TreeEntry{
				{Mode: "100644", Type: localrepo.Blob, OID: blobID, Path: "blob"},
				{Mode: "040000", Type: localrepo.Tree, OID: treeWithSubtree, Path: "subdir"},
				{Mode: "040000", Type: localrepo.Tree, OID: emptyTreeID, Path: "subdir/subdir"},
			},
		},
		{
			desc:    "listing blob fails",
			treeish: blobID.Revision(),
			// We get a NotExist error here because it's invalid to suffix an object ID
			// which resolves to a blob with a colon (":") given that it's not possible
			// to resolve a subpath.
			expectedErr: ErrNotExist,
		},
		{
			desc:    "valid revision with invalid path",
			treeish: treeWithSubtree.Revision(),
			cfg: &ListEntriesConfig{
				RelativePath: "does-not-exist",
			},
			expectedErr: ErrNotExist,
		},
		{
			desc:    "valid revision with path pointing to blob",
			treeish: treeWithSubtreeAndBlob.Revision(),
			cfg: &ListEntriesConfig{
				RelativePath: "blob",
			},
			expectedErr: ErrNotTreeish,
		},
		{
			desc:        "listing nonexistent object fails",
			treeish:     "does-not-exist",
			expectedErr: ErrNotExist,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			results, err := ListEntries(ctx, repo, tc.treeish, tc.cfg)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedResults, results)
		})
	}
}
