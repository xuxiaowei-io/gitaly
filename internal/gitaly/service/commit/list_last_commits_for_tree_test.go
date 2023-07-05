package commit

import (
	"errors"
	"io"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestListLastCommitsForTree(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	childCommitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("unchanged"),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "changed", Mode: "100644", Content: "not-yet-changed"},
			gittest.TreeEntry{Path: "unchanged", Mode: "100644", Content: "unchanged"},
			gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
				{Path: "subdir-changed", Mode: "100644", Content: "not-yet-changed"},
				{Path: "subdir-unchanged", Mode: "100644", Content: "unchanged"},
			})},
		),
	)
	parentCommitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("changed"),
		gittest.WithParents(childCommitID), gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "changed", Mode: "100644", Content: "changed"},
			gittest.TreeEntry{Path: "unchanged", Mode: "100644", Content: "unchanged"},
			gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
				{Path: "subdir-changed", Mode: "100644", Content: "changed"},
				{Path: "subdir-unchanged", Mode: "100644", Content: "unchanged"},
			})},
		),
	)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	childCommit, err := repo.ReadCommit(ctx, childCommitID.Revision())
	require.NoError(t, err)
	parentCommit, err := repo.ReadCommit(ctx, parentCommitID.Revision())
	require.NoError(t, err)

	commitResponse := func(path string, commit *gitalypb.GitCommit) *gitalypb.ListLastCommitsForTreeResponse_CommitForTree {
		return &gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
			PathBytes: []byte(path),
			Commit:    commit,
		}
	}

	type setupData struct {
		request         *gitalypb.ListLastCommitsForTreeRequest
		expectedErr     error
		expectedCommits []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "missing revision",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Path:       []byte("/"),
						Revision:   "",
					},
					expectedErr: structerr.NewInvalidArgument("empty revision"),
				}
			},
		},
		{
			desc: "invalid repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: &gitalypb.Repository{
							StorageName:  "broken",
							RelativePath: "does-not-exist",
						},
						Revision: parentCommitID.String(),
					},
					expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
						"%w", storage.NewStorageNotFoundError("broken"),
					)),
				}
			},
		},
		{
			desc: "unset repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: nil,
						Path:       []byte("/"),
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "ambiguous revision",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   "a",
					},
					expectedErr: structerr.NewInternal("exit status 128"),
				}
			},
		},
		{
			desc: "invalid revision",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   "--output=/meow",
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "negative offset",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   parentCommitID.String(),
						Offset:     -1,
						Limit:      25,
					},
					expectedErr: structerr.NewInvalidArgument("offset negative"),
				}
			},
		},
		{
			desc: "negative limit",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   parentCommitID.String(),
						Offset:     0,
						Limit:      -1,
					},
					expectedErr: structerr.NewInvalidArgument("limit negative"),
				}
			},
		},
		{
			desc: "root directory",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   parentCommitID.String(),
						Path:       []byte("/"),
						Limit:      5,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("subdir", parentCommit),
						commitResponse("changed", parentCommit),
						commitResponse("unchanged", childCommit),
					},
				}
			},
		},
		{
			desc: "subdirectory",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   parentCommitID.String(),
						Path:       []byte("subdir/"),
						Limit:      5,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("subdir/subdir-changed", parentCommit),
						commitResponse("subdir/subdir-unchanged", childCommit),
					},
				}
			},
		},
		{
			desc: "offset higher than number of paths",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   parentCommitID.String(),
						Path:       []byte("/"),
						Offset:     14,
					},
					expectedCommits: nil,
				}
			},
		},
		{
			desc: "limit restricts returned commits",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   parentCommitID.String(),
						Path:       []byte("/"),
						Limit:      1,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("subdir", parentCommit),
					},
				}
			},
		},
		{
			desc: "offset allows printing tail",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   parentCommitID.String(),
						Path:       []byte("/"),
						Limit:      25,
						Offset:     2,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("unchanged", childCommit),
					},
				}
			},
		},
		{
			desc: "path with leading dash",
			setup: func(t *testing.T) setupData {
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: "-test", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
						{Path: "file", Mode: "100644", Content: "something"},
					}),
				}))

				commit, err := repo.ReadCommit(ctx, commitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   commitID.String(),
						Path:       []byte("-test/"),
						Limit:      25,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse("-test/file", commit),
					},
				}
			},
		},
		{
			desc: "glob with literal pathspec",
			setup: func(t *testing.T) setupData {
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: ":wq", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
						{Path: "file", Mode: "100644", Content: "something"},
					}),
				}))

				commit, err := repo.ReadCommit(ctx, commitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   commitID.String(),
						Path:       []byte(":wq"),
						Limit:      25,
						GlobalOptions: &gitalypb.GlobalOptions{
							LiteralPathspecs: true,
						},
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse(":wq", commit),
					},
				}
			},
		},
		{
			desc: "glob without literal pathspec",
			setup: func(t *testing.T) setupData {
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
					Path: ":wq", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
						{Path: "file", Mode: "100644", Content: "something"},
					}),
				}))

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   commitID.String(),
						Path:       []byte(":wq"),
						Limit:      25,
						GlobalOptions: &gitalypb.GlobalOptions{
							LiteralPathspecs: false,
						},
					},
					expectedCommits: nil,
				}
			},
		},
		{
			desc: "non-utf8 filename",
			setup: func(t *testing.T) setupData {
				path := "hello\x80world"
				require.False(t, utf8.ValidString(path))

				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: path, Content: "something"},
				))

				commit, err := repo.ReadCommit(ctx, commitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.ListLastCommitsForTreeRequest{
						Repository: repoProto,
						Revision:   commitID.String(),
						Limit:      25,
					},
					expectedCommits: []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
						commitResponse(path, commit),
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t)

			stream, err := client.ListLastCommitsForTree(ctx, setup.request)
			require.NoError(t, err)

			var commits []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree
			for {
				var response *gitalypb.ListLastCommitsForTreeResponse

				response, err = stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}
					break
				}

				commits = append(commits, response.Commits...)
			}

			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedCommits, commits)
		})
	}
}
