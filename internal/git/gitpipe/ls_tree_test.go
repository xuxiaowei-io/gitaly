package gitpipe

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestLsTree(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, repoPath string) (git.Revision, []RevisionResult)
		options     []LsTreeOption
		expectedErr error
	}{
		{
			desc: "initial commit",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				blobA := git.WriteBlob(t, cfg, repoPath, []byte("a"))
				blobB := git.WriteBlob(t, cfg, repoPath, []byte("b"))
				blobC := git.WriteBlob(t, cfg, repoPath, []byte("c"))

				tree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{Path: ".gitignore", Mode: "100644", OID: blobA},
					{Path: "LICENSE", Mode: "100644", OID: blobB},
					{Path: "README.md", Mode: "100644", OID: blobC},
				})

				return tree.Revision(), []RevisionResult{
					{OID: blobA, ObjectName: []byte(".gitignore")},
					{OID: blobB, ObjectName: []byte("LICENSE")},
					{OID: blobC, ObjectName: []byte("README.md")},
				}
			},
		},
		{
			desc: "includes submodule",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				blob := git.WriteBlob(t, cfg, repoPath, []byte("a"))
				commit := git.WriteTestCommit(t, cfg, repoPath)

				tree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{Path: "blob", Mode: "100644", OID: blob},
					{Path: "submodule", Mode: "160000", OID: commit},
				})

				return tree.Revision(), []RevisionResult{
					{OID: blob, ObjectName: []byte("blob")},
					{OID: commit, ObjectName: []byte("submodule")},
				}
			},
		},
		{
			desc: "filter blobs only",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				blob := git.WriteBlob(t, cfg, repoPath, []byte("a"))
				commit := git.WriteTestCommit(t, cfg, repoPath)

				tree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{
						Path: "blob",
						Mode: "100644",
						OID:  blob,
					},
					{
						Path: "submodule",
						Mode: "160000",
						OID:  commit,
					},
					{
						Path: "subtree",
						Mode: "040000",
						OID: git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
							{Path: "blob-in-subtree", Mode: "100644", Content: "something"},
						}),
					},
				})

				return tree.Revision(), []RevisionResult{
					{OID: blob, ObjectName: []byte("blob")},
				}
			},
			options: []LsTreeOption{
				LsTreeWithBlobFilter(),
			},
		},
		{
			desc: "empty tree",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				return git.DefaultObjectHash.EmptyTreeOID.Revision(), nil
			},
		},
		{
			desc: "non-recursive",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				blob := git.WriteBlob(t, cfg, repoPath, []byte("a"))
				subtree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{Path: "blob-in-subtree", Mode: "100644", Content: "something"},
				})

				tree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{Path: "blob", Mode: "100644", OID: blob},
					{Path: "subtree", Mode: "040000", OID: subtree},
				})

				return tree.Revision(), []RevisionResult{
					{OID: blob, ObjectName: []byte("blob")},
					{OID: subtree, ObjectName: []byte("subtree")},
				}
			},
		},
		{
			desc: "recursive",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				blob := git.WriteBlob(t, cfg, repoPath, []byte("a"))
				blobInSubtree := git.WriteBlob(t, cfg, repoPath, []byte("b"))

				tree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{
						Path: "blob",
						Mode: "100644",
						OID:  blob,
					},
					{
						Path: "subtree",
						Mode: "040000",
						OID: git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
							{Path: "blob-in-subtree", Mode: "100644", OID: blobInSubtree},
						}),
					},
				})

				return tree.Revision(), []RevisionResult{
					{OID: blob, ObjectName: []byte("blob")},
					{OID: blobInSubtree, ObjectName: []byte("subtree/blob-in-subtree")},
				}
			},
			options: []LsTreeOption{
				LsTreeWithRecursive(),
			},
		},
		{
			desc: "with skip function",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				blobA := git.WriteBlob(t, cfg, repoPath, []byte("a"))
				blobB := git.WriteBlob(t, cfg, repoPath, []byte("b"))
				blobC := git.WriteBlob(t, cfg, repoPath, []byte("c"))

				tree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{Path: ".gitignore", Mode: "100644", OID: blobA},
					{Path: "LICENSE", Mode: "100644", OID: blobB},
					{Path: "README.md", Mode: "100644", OID: blobC},
				})

				return tree.Revision(), []RevisionResult{
					{OID: blobB, ObjectName: []byte("LICENSE")},
					{OID: blobC, ObjectName: []byte("README.md")},
				}
			},
			options: []LsTreeOption{
				LsTreeWithSkip(func(r *RevisionResult) (bool, error) {
					return bytes.Equal(r.ObjectName, []byte(".gitignore")), nil
				}),
			},
		},
		{
			desc: "with skip failure",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				tree := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
					{Path: "README.md", Mode: "100644", Content: "Hello world"},
				})

				return tree.Revision(), nil
			},
			options: []LsTreeOption{
				LsTreeWithSkip(func(r *RevisionResult) (bool, error) {
					return true, errors.New("broken")
				}),
			},
			expectedErr: errors.New(`ls-tree skip: "broken"`),
		},
		{
			desc: "invalid revision",
			setup: func(t *testing.T, repoPath string) (git.Revision, []RevisionResult) {
				return "refs/heads/does-not-exist", nil
			},
			expectedErr: errors.New("ls-tree pipeline command: exit status 128, stderr: " +
				"\"fatal: Not a valid object name refs/heads/does-not-exist\\n\""),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := git.CreateRepository(t, ctx, cfg, git.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			revision, expectedResults := tc.setup(t, repoPath)

			it := LsTree(ctx, repo, revision.String(), tc.options...)

			var results []RevisionResult
			for it.Next() {
				results = append(results, it.Result())
			}

			// We're converting the error here to a plain un-nested error such that we
			// don't have to replicate the complete error's structure.
			err := it.Err()
			if err != nil {
				err = errors.New(err.Error())
			}

			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, expectedResults, results)
		})
	}
}
