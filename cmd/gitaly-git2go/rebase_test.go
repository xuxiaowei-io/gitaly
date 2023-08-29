//go:build static && system_libgit2

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/cmd/gitaly-git2go/git2goutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

var masterRevision = "1e292f8fedd741b75372e19097c76d327140c312"

func TestRebase_validation(t *testing.T) {
	gittest.SkipWithSHA256(t)

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	testcfg.BuildGitalyGit2Go(t, cfg)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	committer := git2go.NewSignature("Foo", "foo@example.com", time.Now())
	executor := buildExecutor(t, cfg)

	testcases := []struct {
		desc        string
		request     git2go.RebaseCommand
		expectedErr string
	}{
		{
			desc:        "no arguments",
			expectedErr: "rebase: missing repository",
		},
		{
			desc:        "missing repository",
			request:     git2go.RebaseCommand{Committer: committer, BranchName: "feature", UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing repository",
		},
		{
			desc:        "missing committer name",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: git2go.Signature{Email: "foo@example.com"}, BranchName: "feature", UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing committer name",
		},
		{
			desc:        "missing committer email",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: git2go.Signature{Name: "Foo"}, BranchName: "feature", UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing committer email",
		},
		{
			desc:        "missing branch name",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: committer, UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing branch name",
		},
		{
			desc:        "missing upstream branch",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: committer, BranchName: "feature"},
			expectedErr: "rebase: missing upstream revision",
		},
		{
			desc:        "both branch name and commit ID",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: committer, BranchName: "feature", CommitID: "a"},
			expectedErr: "rebase: both branch name and commit ID",
		},
		{
			desc:        "both upstream revision and upstream commit ID",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: committer, BranchName: "feature", UpstreamRevision: "a", UpstreamCommitID: "a"},
			expectedErr: "rebase: both upstream revision and upstream commit ID",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := executor.Rebase(ctx, repo, tc.request)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}

func TestRebase_rebase(t *testing.T) {
	gittest.SkipWithSHA256(t)

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyGit2Go(t, cfg)
	executor := buildExecutor(t, cfg)

	committer := git2go.NewSignature(
		string(gittest.TestUser.Name),
		string(gittest.TestUser.Email),
		time.Date(2021, 3, 1, 13, 45, 50, 0, time.FixedZone("", +2*60*60)),
	)

	type setup struct {
		base, upstream, downstream git.ObjectID
		expecetedCommitsAhead      int
		expectedObjectID           git.ObjectID
		expectedErr                string
	}

	testcases := []struct {
		desc  string
		setup func(testing.TB, string) setup
	}{
		{
			desc: "Single commit rebase",
			setup: func(tb testing.TB, repoPath string) setup {
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ng\n"},
				))
				upstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("upstream"), gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "upstream\nb\nc\nd\ne\nf\ng\n"},
				))
				downstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("downstream"), gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream\n"},
				))

				return setup{
					base:                  base,
					upstream:              upstream,
					downstream:            downstream,
					expectedObjectID:      "ef018adb419cd97453a0624c28271fafe622b83e",
					expecetedCommitsAhead: 1,
				}
			},
		},
		{
			desc: "Multiple commits",
			setup: func(tb testing.TB, repoPath string) setup {
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ng\n"},
				))
				upstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("upstream"), gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "upstream\nb\nc\nd\ne\nf\ng\n"},
				))
				downstream1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("downstream-1"), gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream-1\n"},
				))
				downstream2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("downstream-2"), gittest.WithParents(downstream1), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream-2\n"},
				))
				downstream3 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("downstream-3"), gittest.WithParents(downstream2), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream-3\n"},
				))

				return setup{
					base:                  base,
					upstream:              upstream,
					downstream:            downstream3,
					expectedObjectID:      "d3c737fdb3a0c4da3a371fc01de6df4cbb5bc3e4",
					expecetedCommitsAhead: 3,
				}
			},
		},
		{
			desc: "Branch zero commits behind",
			setup: func(tb testing.TB, repoPath string) setup {
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ng\n"},
				))
				downstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("downstream"), gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream\n"},
				))

				return setup{
					base:                  base,
					upstream:              base,
					downstream:            downstream,
					expectedObjectID:      downstream,
					expecetedCommitsAhead: 1,
				}
			},
		},
		{
			desc: "Merged branch",
			setup: func(tb testing.TB, repoPath string) setup {
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ng\n"},
				))
				downstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream\n"},
				))
				merge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(base, downstream), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream\n"},
				))

				return setup{
					base: base, upstream: merge, downstream: downstream,
					expectedObjectID: merge,
				}
			},
		},
		{
			desc: "Partially merged branch",
			setup: func(tb testing.TB, repoPath string) setup {
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ng\n"},
				))
				downstream1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream-1\n"},
				))
				downstream2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(downstream1), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream-2\n"},
				))
				merge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(base, downstream1), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream-1\n"},
				))

				return setup{
					base:                  base,
					upstream:              merge,
					downstream:            downstream2,
					expectedObjectID:      "721e8bd36a394a7cc243b8c3960b44c5520c6246",
					expecetedCommitsAhead: 1,
				}
			},
		},
		{
			desc: "With upstream merged into",
			setup: func(tb testing.TB, repoPath string) setup {
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ng\n"},
				))
				upstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("upstream"), gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "upstream\nb\nc\nd\ne\nf\ng\n"},
				))
				downstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("downstream"), gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "a\nb\nc\nd\ne\nf\ndownstream\n"},
				))
				downstreamMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(downstream, upstream), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "upstream\nb\nc\nd\ne\nf\ndownstream\n"},
				))

				return setup{
					base:                  base,
					upstream:              upstream,
					downstream:            downstreamMerge,
					expectedObjectID:      "aa375bc059fa8830d9489d89af1278632722407d",
					expecetedCommitsAhead: 2,
				}
			},
		},
		{
			desc: "Rebase with conflict",
			setup: func(tb testing.TB, repoPath string) setup {
				base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "base\n"},
				))
				upstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "upstream\n"},
				))
				downstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(base), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "downstream\n"},
				))

				return setup{
					upstream:    upstream,
					downstream:  downstream,
					expectedErr: fmt.Sprintf("rebase: commit %q: there are conflicting files", downstream),
				}
			},
		},
		{
			desc: "Orphaned branch",
			setup: func(tb testing.TB, repoPath string) setup {
				upstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "upstream\n"},
				))
				downstream := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "path", Mode: "100644", Content: "downstream\n"},
				))

				return setup{
					upstream:    upstream,
					downstream:  downstream,
					expectedErr: "rebase: find merge base: no merge base found",
				}
			},
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			setup := tc.setup(t, repoPath)

			gittest.WriteRef(t, cfg, repoPath, "refs/heads/upstream", setup.upstream)
			gittest.WriteRef(t, cfg, repoPath, "refs/heads/downstream", setup.downstream)

			repo, err := git2goutil.OpenRepository(repoPath)
			require.NoError(t, err)

			for desc, request := range map[string]git2go.RebaseCommand{
				"with branch and upstream": {
					Repository:       repoPath,
					Committer:        committer,
					BranchName:       "downstream",
					UpstreamRevision: setup.upstream.String(),
				},
				"with branch and upstream commit ID": {
					Repository:       repoPath,
					Committer:        committer,
					BranchName:       "downstream",
					UpstreamCommitID: setup.upstream,
				},
				"with commit ID and upstream": {
					Repository:       repoPath,
					Committer:        committer,
					CommitID:         setup.downstream,
					UpstreamRevision: setup.upstream.String(),
				},
				"with commit ID and upstream commit ID": {
					Repository:       repoPath,
					Committer:        committer,
					CommitID:         setup.downstream,
					UpstreamCommitID: setup.upstream,
				},
			} {
				t.Run(desc, func(t *testing.T) {
					response, err := executor.Rebase(ctx, repoProto, request)
					if setup.expectedErr != "" {
						require.EqualError(t, err, setup.expectedErr)
					} else {
						require.NoError(t, err)
						require.Equal(t, setup.expectedObjectID, response)

						commit, err := lookupCommit(repo, response.String())
						require.NoError(t, err)

						for i := setup.expecetedCommitsAhead; i > 0; i-- {
							commit = commit.Parent(0)
						}
						baseCommit, err := lookupCommit(repo, setup.base.String())
						require.NoError(t, err)
						require.Equal(t, baseCommit, commit)
					}
				})
			}
		})
	}
}

func TestRebase_skipEmptyCommit(t *testing.T) {
	gittest.SkipWithSHA256(t)

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	testcfg.BuildGitalyGit2Go(t, cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	// Set up history with two diverging lines of branches, where both sides have implemented
	// the same changes. During rebase, the diff will thus become empty.
	base := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(gittest.TreeEntry{
			Path: "a", Content: "base", Mode: "100644",
		}),
	)
	theirs := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("theirs"),
		gittest.WithParents(base), gittest.WithTreeEntries(gittest.TreeEntry{
			Path: "a", Content: "changed", Mode: "100644",
		}),
	)
	ours := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("ours"),
		gittest.WithParents(base), gittest.WithTreeEntries(gittest.TreeEntry{
			Path: "a", Content: "changed", Mode: "100644",
		}),
	)

	for _, tc := range []struct {
		desc             string
		skipEmptyCommits bool
		expectedErr      string
		expectedResponse git.ObjectID
	}{
		{
			desc:             "do not skip empty commit",
			skipEmptyCommits: false,
			expectedErr:      fmt.Sprintf("rebase: commit %q: this patch has already been applied", ours),
		},
		{
			desc:             "skip empty commit",
			skipEmptyCommits: true,
			expectedResponse: theirs,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := buildExecutor(t, cfg).Rebase(ctx, repoProto, git2go.RebaseCommand{
				Repository:       repoPath,
				Committer:        git2go.NewSignature("Foo", "foo@example.com", time.Now()),
				CommitID:         ours,
				UpstreamCommitID: theirs,
				SkipEmptyCommits: tc.skipEmptyCommits,
			})
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedResponse, response)
		})
	}
}
