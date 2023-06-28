//go:build !gitaly_test_sha256

package git2go

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func defaultCommitAuthorSignature() Signature {
	return NewSignature(
		gittest.DefaultCommitterName,
		gittest.DefaultCommitterMail,
		gittest.DefaultCommitTime,
	)
}

func TestExecutor_Apply(t *testing.T) {
	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
	).Run(t, testExecutorApply)
}

func testExecutorApply(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyGit2Go(t, cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	executor := NewExecutor(cfg, gittest.NewCommandFactory(t, cfg), config.NewLocator(cfg))

	oidBase, err := repo.WriteBlob(ctx, "file", strings.NewReader("base"))
	require.NoError(t, err)

	oidA, err := repo.WriteBlob(ctx, "file", strings.NewReader("a"))
	require.NoError(t, err)

	oidB, err := repo.WriteBlob(ctx, "file", strings.NewReader("b"))
	require.NoError(t, err)

	author := defaultCommitAuthorSignature()
	committer := defaultCommitAuthorSignature()

	treeEntry := &localrepo.TreeEntry{
		Mode: "040000",
		Type: localrepo.Tree,
	}

	require.NoError(t, treeEntry.Add("file", localrepo.TreeEntry{
		Path: "file",
		Mode: "100644",
		Type: localrepo.Blob,
		OID:  oidBase,
	}))
	require.NoError(t, treeEntry.Write(ctx, repo))

	parentCommitSHA, err := repo.WriteCommit(
		ctx,
		localrepo.WriteCommitConfig{
			AuthorName:     gittest.DefaultCommitterName,
			AuthorEmail:    gittest.DefaultCommitterMail,
			AuthorDate:     gittest.DefaultCommitTime,
			CommitterName:  gittest.DefaultCommitterName,
			CommitterEmail: gittest.DefaultCommitterMail,
			CommitterDate:  gittest.DefaultCommitTime,
			Message:        "base commit",
			TreeID:         treeEntry.OID,
		},
	)
	require.NoError(t, err)

	treeEntry = &localrepo.TreeEntry{
		Mode: "040000",
		Type: localrepo.Tree,
	}

	require.NoError(t, treeEntry.Add("file", localrepo.TreeEntry{
		Path: "file",
		Mode: "100644",
		Type: localrepo.Blob,
		OID:  oidA,
	}))
	require.NoError(t, treeEntry.Write(ctx, repo))

	noCommonAncestor, err := repo.WriteCommit(
		ctx,
		localrepo.WriteCommitConfig{
			AuthorName:     gittest.DefaultCommitterName,
			AuthorEmail:    gittest.DefaultCommitterMail,
			AuthorDate:     gittest.DefaultCommitTime,
			CommitterName:  gittest.DefaultCommitterName,
			CommitterEmail: gittest.DefaultCommitterMail,
			CommitterDate:  gittest.DefaultCommitTime,
			Message:        "base commit",
			TreeID:         treeEntry.OID,
		},
	)
	require.NoError(t, err)

	treeEntry, err = repo.ReadTree(ctx, git.Revision(parentCommitSHA.String()))
	require.NoError(t, err)

	require.NoError(t, treeEntry.Modify("file", func(e *localrepo.TreeEntry) error {
		e.OID = oidA
		return nil
	}))
	require.NoError(t, treeEntry.Write(ctx, repo))

	updateToA, err := repo.WriteCommit(
		ctx,
		localrepo.WriteCommitConfig{
			AuthorName:     gittest.DefaultCommitterName,
			AuthorEmail:    gittest.DefaultCommitterMail,
			AuthorDate:     gittest.DefaultCommitTime,
			CommitterName:  gittest.DefaultCommitterName,
			CommitterEmail: gittest.DefaultCommitterMail,
			CommitterDate:  gittest.DefaultCommitTime,
			Message:        "commit with a",
			TreeID:         treeEntry.OID,
			Parents:        []git.ObjectID{parentCommitSHA},
		},
	)
	require.NoError(t, err)

	treeEntry, err = repo.ReadTree(ctx, git.Revision(parentCommitSHA.String()))
	require.NoError(t, err)

	require.NoError(t, treeEntry.Modify("file", func(e *localrepo.TreeEntry) error {
		e.OID = oidB
		return nil
	}))
	require.NoError(t, treeEntry.Write(ctx, repo))

	updateToB, err := repo.WriteCommit(
		ctx,
		localrepo.WriteCommitConfig{
			AuthorName:     gittest.DefaultCommitterName,
			AuthorEmail:    gittest.DefaultCommitterMail,
			AuthorDate:     gittest.DefaultCommitTime,
			CommitterName:  gittest.DefaultCommitterName,
			CommitterEmail: gittest.DefaultCommitterMail,
			CommitterDate:  gittest.DefaultCommitTime,
			Message:        "commit with b",
			TreeID:         treeEntry.OID,
			Parents:        []git.ObjectID{parentCommitSHA},
		},
	)
	require.NoError(t, err)

	treeEntry, err = repo.ReadTree(ctx, git.Revision(updateToA.String()))
	require.NoError(t, err)

	require.NoError(t, treeEntry.Modify("file", func(e *localrepo.TreeEntry) error {
		e.OID = oidB
		return nil
	}))
	require.NoError(t, treeEntry.Write(ctx, repo))

	updateFromAToB, err := repo.WriteCommit(
		ctx,
		localrepo.WriteCommitConfig{
			AuthorName:     gittest.DefaultCommitterName,
			AuthorEmail:    gittest.DefaultCommitterMail,
			AuthorDate:     gittest.DefaultCommitTime,
			CommitterName:  gittest.DefaultCommitterName,
			CommitterEmail: gittest.DefaultCommitterMail,
			CommitterDate:  gittest.DefaultCommitTime,
			Message:        "commit a -> b",
			TreeID:         treeEntry.OID,
			Parents:        []git.ObjectID{updateToA},
		},
	)
	require.NoError(t, err)

	treeEntry = &localrepo.TreeEntry{
		Mode: "040000",
		Type: localrepo.Tree,
	}

	require.NoError(t, treeEntry.Add("other-file", localrepo.TreeEntry{
		Path: "other-file",
		Mode: "100644",
		Type: localrepo.Blob,
		OID:  oidA,
	}))
	require.NoError(t, treeEntry.Write(ctx, repo))

	otherFile, err := repo.WriteCommit(
		ctx,
		localrepo.WriteCommitConfig{
			AuthorName:     gittest.DefaultCommitterName,
			AuthorEmail:    gittest.DefaultCommitterMail,
			AuthorDate:     gittest.DefaultCommitTime,
			CommitterName:  gittest.DefaultCommitterName,
			CommitterEmail: gittest.DefaultCommitterMail,
			CommitterDate:  gittest.DefaultCommitTime,
			Message:        "commit with other-file",
			TreeID:         treeEntry.OID,
		},
	)
	require.NoError(t, err)

	diffBetween := func(tb testing.TB, fromCommit, toCommit git.ObjectID) []byte {
		tb.Helper()
		return gittest.Exec(tb, cfg, "-C", repoPath, "format-patch", "--stdout", fromCommit.String()+".."+toCommit.String())
	}

	for _, tc := range []struct {
		desc         string
		patches      []Patch
		parentCommit git.ObjectID
		tree         []gittest.TreeEntry
		error        error
	}{
		{
			desc: "patch applies cleanly",
			patches: []Patch{
				{
					Author:  author,
					Message: "test commit message",
					Diff:    diffBetween(t, parentCommitSHA, updateToA),
				},
			},
			parentCommit: parentCommitSHA,
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "a"},
			},
		},
		{
			desc: "multiple patches apply cleanly",
			patches: []Patch{
				{
					Author:  author,
					Message: "commit with a",
					Diff:    diffBetween(t, parentCommitSHA, updateToA),
				},
				{
					Author:  author,
					Message: "commit with a -> b",
					Diff:    diffBetween(t, updateToA, updateFromAToB),
				},
			},
			parentCommit: updateToA,
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "b"},
			},
		},
		{
			desc: "three way merged",
			patches: []Patch{
				{
					Author:  author,
					Message: "three-way merged files",
					Diff:    diffBetween(t, parentCommitSHA, otherFile),
				},
			},
			parentCommit: parentCommitSHA,
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "base"},
				{Path: "other-file", Mode: "100644", Content: "a"},
			},
		},
		{
			// This test asserts incorrect behavior due to a bug in libgit2's
			// git_apply_to_tree function. The correct behavior would be to
			// return an error about merge conflict but we currently concatenate
			// the blobs of the two trees together. This test will fail once the
			// issue is fixed.
			//
			// See: https://gitlab.com/gitlab-org/gitaly/-/issues/3325
			desc: "no common ancestor",
			patches: []Patch{
				{
					Author:  author,
					Message: "three-way merged file",
					Diff:    diffBetween(t, parentCommitSHA, noCommonAncestor),
				},
			},
			parentCommit: parentCommitSHA,
			// error: ErrMergeConflict, <- correct output
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "abase"},
			},
		},
		{
			desc: "merge conflict",
			patches: []Patch{
				{
					Author:  author,
					Message: "applies cleanly",
					Diff:    diffBetween(t, parentCommitSHA, updateToA),
				},
				{
					Author:  author,
					Message: "conflicts",
					Diff:    diffBetween(t, parentCommitSHA, updateToB),
				},
			},
			error: ErrMergeConflict,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			commitID, err := executor.Apply(ctx, repo, ApplyParams{
				Repository:   repoPath,
				Committer:    committer,
				ParentCommit: parentCommitSHA.String(),
				Patches:      NewSlicePatchIterator(tc.patches),
			})
			if tc.error != nil {
				require.True(t, errors.Is(err, tc.error), err)
				return
			}

			commit, err := repo.ReadCommit(ctx, commitID.Revision())
			require.NoError(t, err)

			require.Equal(t, []string{string(tc.parentCommit)}, commit.ParentIds)
			require.Equal(t, gittest.DefaultCommitAuthor, commit.Author)
			require.Equal(t, gittest.DefaultCommitAuthor, commit.Committer)
			require.Equal(t, []byte(tc.patches[len(tc.patches)-1].Message), commit.Body)

			gittest.RequireTree(t, cfg, repoPath, commitID.String(), tc.tree)
		})
	}
}
