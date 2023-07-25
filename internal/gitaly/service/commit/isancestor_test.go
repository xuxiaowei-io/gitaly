//go:build !gitaly_test_sha256

package commit

import (
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommitIsAncestor(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	type setupData struct {
		request          *gitalypb.CommitIsAncestorRequest
		expectedResponse *gitalypb.CommitIsAncestorResponse
		expectedErr      error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "direct ancestor",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)
				child := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(ancestor))

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: ancestor.String(),
						ChildId:    child.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: true,
					},
				}
			},
		},
		{
			desc: "not an ancestor",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// We add different messages here to ensure that the commit IDs are different.
				ancestor := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("ancestor"))
				child := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("child"))

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: ancestor.String(),
						ChildId:    child.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: false,
					},
				}
			},
		},
		{
			desc: "invalid ancestor",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				child := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: gittest.DefaultObjectHash.EmptyTreeOID.String(),
						ChildId:    child.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: false,
					},
				}
			},
		},
		{
			desc: "invalid child",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: ancestor.String(),
						ChildId:    gittest.DefaultObjectHash.EmptyTreeOID.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: false,
					},
				}
			},
		},
		{
			desc: "indirect ancestor",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)
				midCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(ancestor))
				child := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(midCommit))

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: ancestor.String(),
						ChildId:    child.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: true,
					},
				}
			},
		},
		{
			desc: "with revisions",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(ancestor), gittest.WithBranch("feature"))

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: "master",
						ChildId:    "feature",
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: true,
					},
				}
			},
		},
		{
			desc: "with tags",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1.0.0"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(ancestor), gittest.WithReference("refs/tags/v1.0.1"))

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: "refs/tags/v1.0.0",
						ChildId:    "refs/tags/v1.0.1",
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: true,
					},
				}
			},
		},
		{
			desc: "ancestor as child",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)
				child := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(ancestor))

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: child.String(),
						ChildId:    ancestor.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: false,
					},
				}
			},
		},
		{
			desc: "with alternates directory",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)

				altObjectsDir := "./alt-objects"
				child := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(ancestor),
					gittest.WithAlternateObjectDirectory(filepath.Join(repoPath, altObjectsDir)),
				)
				repo.GitAlternateObjectDirectories = []string{altObjectsDir}

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: ancestor.String(),
						ChildId:    child.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: true,
					},
				}
			},
		},
		{
			desc: "with alternates directory, but not set on repo",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)

				altObjectsDir := "./alt-objects"
				child := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(ancestor),
					gittest.WithAlternateObjectDirectory(filepath.Join(repoPath, altObjectsDir)),
				)

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: ancestor.String(),
						ChildId:    child.String(),
					},
					expectedResponse: &gitalypb.CommitIsAncestorResponse{
						Value: false,
					},
				}
			},
		},
		{
			desc: "repo not set",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						AncestorId: gittest.DefaultObjectHash.EmptyTreeOID.String(),
						ChildId:    gittest.DefaultObjectHash.EmptyTreeOID.String(),
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "empty ancestor",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)
				child := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(ancestor))

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: "",
						ChildId:    child.String(),
					},
					expectedErr: status.Error(codes.InvalidArgument, "empty ancestor sha"),
				}
			},
		},
		{
			desc: "empty child",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				ancestor := gittest.WriteCommit(t, cfg, repoPath)

				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: repo,
						AncestorId: ancestor.String(),
						ChildId:    "",
					},
					expectedErr: status.Error(codes.InvalidArgument, "empty child sha"),
				}
			},
		},
		{
			desc: "invalid repository storage",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.CommitIsAncestorRequest{
						Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "fake-path"},
						AncestorId: gittest.DefaultObjectHash.EmptyTreeOID.String(),
						ChildId:    gittest.DefaultObjectHash.EmptyTreeOID.String(),
					},
					expectedErr: testhelper.ToInterceptedMetadata(
						structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "fake-path")),
					),
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t)

			resp, err := client.CommitIsAncestor(ctx, setup.request)
			testhelper.ProtoEqual(t, setup.expectedResponse, resp)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
		})
	}
}
