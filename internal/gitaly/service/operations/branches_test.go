//go:build !gitaly_test_sha256

package operations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	called int
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	s.called++
	return &gitalypb.VoteTransactionResponse{
		State: gitalypb.VoteTransactionResponse_COMMIT,
	}, nil
}

func TestUserCreateBranch_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	startPoint := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	startPointCommit, err := repo.ReadCommit(ctx, git.Revision(startPoint))
	require.NoError(t, err)

	testCases := []struct {
		desc           string
		branchName     string
		startPoint     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:       "valid branch",
			branchName: "new-branch",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("new-branch"),
				TargetCommit: startPointCommit,
			},
		},
		// On input like heads/foo and refs/heads/foo we don't
		// DWYM and map it to refs/heads/foo and
		// refs/heads/foo, respectively. Instead we always
		// prepend refs/heads/*, so you get
		// refs/heads/heads/foo and refs/heads/refs/heads/foo
		{
			desc:       "valid branch",
			branchName: "heads/new-branch",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("heads/new-branch"),
				TargetCommit: startPointCommit,
			},
		},
		{
			desc:       "valid branch",
			branchName: "refs/heads/new-branch",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("refs/heads/new-branch"),
				TargetCommit: startPointCommit,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			branchName := testCase.branchName
			request := &gitalypb.UserCreateBranchRequest{
				Repository: repoProto,
				BranchName: []byte(branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       gittest.TestUser,
			}

			response, err := client.UserCreateBranch(ctx, request)
			if testCase.expectedBranch != nil {
				defer gittest.Exec(t, cfg, "-C", repoPath, "branch", "-D", branchName)
			}

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch)
			//nolint:staticcheck
			require.Empty(t, response.PreReceiveError)

			branches := gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref", "--", "refs/heads/"+branchName)
			require.Contains(t, string(branches), "refs/heads/"+branchName)
		})
	}
}

func TestUserCreateBranch_Transactions(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})

	transactionServer := &testTransactionServer{}

	cfg.ListenAddr = "127.0.0.1:0" // runs gitaly on the TCP address
	addr := testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterOperationServiceServer(srv, NewServer(
			deps.GetHookManager(),
			deps.GetTxManager(),
			deps.GetLocator(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetUpdaterWithHooks(),
		))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache(), deps.GetPackObjectsConcurrencyTracker(), deps.GetPackObjectsLimiter()))
		// Praefect proxy execution disabled as praefect runs only on the UNIX socket, but
		// the test requires a TCP listening address.
	}, testserver.WithDisablePraefect())

	testcases := []struct {
		desc    string
		address string
	}{
		{
			desc:    "TCP address",
			address: addr,
		},
		{
			desc:    "Unix socket",
			address: "unix://" + cfg.InternalSocketPath(),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			defer gittest.Exec(t, cfg, "-C", repoPath, "branch", "-D", "new-branch")

			ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
			require.NoError(t, err)
			ctx = metadata.IncomingToOutgoing(ctx)

			client := newMuxedOperationClient(t, ctx, tc.address, cfg.Auth.Token,
				backchannel.NewClientHandshaker(
					testhelper.NewDiscardingLogEntry(t),
					func() backchannel.Server {
						srv := grpc.NewServer()
						gitalypb.RegisterRefTransactionServer(srv, transactionServer)
						return srv
					},
					backchannel.DefaultConfiguration(),
				),
			)

			request := &gitalypb.UserCreateBranchRequest{
				Repository: repo,
				BranchName: []byte("new-branch"),
				StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
				User:       gittest.TestUser,
			}

			transactionServer.called = 0
			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)
			//nolint:staticcheck
			require.Empty(t, response.PreReceiveError)
			require.Equal(t, 2, transactionServer.called)
		})
	}
}

func TestUserCreateBranch_hook(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	branchName := "new-branch"
	request := &gitalypb.UserCreateBranchRequest{
		Repository: repo,
		BranchName: []byte(branchName),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       gittest.TestUser,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			defer gittest.Exec(t, cfg, "-C", repoPath, "branch", "-D", branchName)

			hookOutputTempPath := gittest.WriteEnvToCustomHook(t, repoPath, hookName)

			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)
			//nolint:staticcheck
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+gittest.TestUser.GlUsername)
		})
	}
}

func TestUserCreateBranch_startPoint(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	testCases := []struct {
		desc             string
		branchName       string
		startPoint       string
		startPointCommit string
		user             *gitalypb.User
	}{
		// Similar to prefixing branchName in
		// TestSuccessfulCreateBranchRequest() above:
		// Unfortunately (and inconsistently), the StartPoint
		// reference does have DWYM semantics. See
		// https://gitlab.com/gitlab-org/gitaly/-/issues/3331
		{
			desc:             "the StartPoint parameter does DWYM references (boo!)",
			branchName:       "topic",
			startPoint:       "heads/master",
			startPointCommit: "9a944d90955aaf45f6d0c88f30e27f8d2c41cec0", // TODO: see below
			user:             gittest.TestUser,
		},
		{
			desc:             "the StartPoint parameter does DWYM references (boo!) 2",
			branchName:       "topic2",
			startPoint:       "refs/heads/master",
			startPointCommit: "c642fe9b8b9f28f9225d7ea953fe14e74748d53b", // TODO: see below
			user:             gittest.TestUser,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/"+testCase.startPoint,
				testCase.startPointCommit,
				git.ObjectHashSHA1.ZeroOID.String(),
			)
			request := &gitalypb.UserCreateBranchRequest{
				Repository: repoProto,
				BranchName: []byte(testCase.branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       testCase.user,
			}

			// BEGIN TODO: Uncomment if StartPoint started behaving sensibly
			// like BranchName. See
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3331
			//
			//targetCommitOK, err := repo.ReadCommit(ctx, testCase.startPointCommit)
			// END TODO
			targetCommitOK, err := repo.ReadCommit(ctx, "1e292f8fedd741b75372e19097c76d327140c312")
			require.NoError(t, err)

			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)
			responseOk := &gitalypb.UserCreateBranchResponse{
				Branch: &gitalypb.Branch{
					Name:         []byte(testCase.branchName),
					TargetCommit: targetCommitOK,
				},
			}
			testhelper.ProtoEqual(t, responseOk, response)
			branches := gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref", "--", "refs/heads/"+testCase.branchName)
			require.Contains(t, string(branches), "refs/heads/"+testCase.branchName)
		})
	}
}

func TestUserCreateBranch_hookFailure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

	request := &gitalypb.UserCreateBranchRequest{
		Repository: repo,
		BranchName: []byte("new-branch"),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       gittest.TestUser,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	expectedObject := "GL_ID=" + gittest.TestUser.GlId

	for _, hookName := range gitlabPreHooks {
		gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

		_, err := client.UserCreateBranch(ctx, request)

		testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("creation denied by custom hooks").WithDetail(
			&gitalypb.UserCreateBranchError{
				Error: &gitalypb.UserCreateBranchError_CustomHook{
					CustomHook: &gitalypb.CustomHookError{
						HookType: gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
						Stdout:   []byte(expectedObject + "\n"),
					},
				},
			},
		), err)

	}
}

func TestUserCreateBranch_Failure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, _, repo, _, client := setupOperationsService(t, ctx)

	testCases := []struct {
		desc       string
		repo       *gitalypb.Repository
		branchName string
		startPoint string
		user       *gitalypb.User
		err        error
	}{
		{
			desc:       "repository not provided",
			repo:       nil,
			branchName: "shiny-new-branch",
			startPoint: "",
			user:       gittest.TestUser,
			err: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:       "empty start_point",
			repo:       repo,
			branchName: "shiny-new-branch",
			startPoint: "",
			user:       gittest.TestUser,
			err:        status.Error(codes.InvalidArgument, "empty start point"),
		},
		{
			desc:       "empty user",
			repo:       repo,
			branchName: "shiny-new-branch",
			startPoint: "master",
			user:       nil,
			err:        status.Error(codes.InvalidArgument, "empty user"),
		},
		{
			desc:       "non-existing starting point",
			repo:       repo,
			branchName: "new-branch",
			startPoint: "i-dont-exist",
			user:       gittest.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", "i-dont-exist"),
		},
		{
			desc:       "branch exists",
			repo:       repo,
			branchName: "master",
			startPoint: "master",
			user:       gittest.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update refs/heads/master. Please refresh and try again."),
		},
		{
			desc:       "conflicting with refs/heads/improve/awesome",
			repo:       repo,
			branchName: "improve",
			startPoint: "master",
			user:       gittest.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update refs/heads/improve. Please refresh and try again."),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserCreateBranchRequest{
				Repository: testCase.repo,
				BranchName: []byte(testCase.branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       testCase.user,
			}

			response, err := client.UserCreateBranch(ctx, request)
			testhelper.RequireGrpcError(t, testCase.err, err)
			require.Empty(t, response)
		})
	}
}

func TestUserDeleteBranch(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	type setupResponse struct {
		request          *gitalypb.UserDeleteBranchRequest
		expectedResponse *gitalypb.UserDeleteBranchResponse
		repoPath         string
		expectedErr      error
		expectedRefs     []string
	}

	testCases := []struct {
		desc  string
		setup func() setupResponse
	}{
		{
			desc: "simple successful deletion without ExpectedOldOID",
			setup: func() setupResponse {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit))

				gittest.Exec(t, cfg, "-C", repoPath, "branch", "to-attempt-to-delete-soon-branch", "master")

				return setupResponse{
					request: &gitalypb.UserDeleteBranchRequest{
						User:       gittest.TestUser,
						Repository: repoProto,
						BranchName: []byte("to-attempt-to-delete-soon-branch"),
					},
					repoPath:         repoPath,
					expectedResponse: &gitalypb.UserDeleteBranchResponse{},
					expectedRefs:     []string{"master"},
				}
			},
		},
		{
			desc: "simple successful deletion with ExpectedOldOID",
			setup: func() setupResponse {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				headCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit))

				gittest.Exec(t, cfg, "-C", repoPath, "branch", "to-attempt-to-delete-soon-branch", "master")

				return setupResponse{
					request: &gitalypb.UserDeleteBranchRequest{
						User:           gittest.TestUser,
						Repository:     repoProto,
						BranchName:     []byte("to-attempt-to-delete-soon-branch"),
						ExpectedOldOid: headCommit.String(),
					},
					repoPath:         repoPath,
					expectedResponse: &gitalypb.UserDeleteBranchResponse{},
					expectedRefs:     []string{"master"},
				}
			},
		},
		{
			desc: "partially prefixed successful deletion",
			setup: func() setupResponse {
				branchName := "heads/to-attempt-to-delete-soon-branch"

				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit))

				gittest.Exec(t, cfg, "-C", repoPath, "branch", branchName, "master")

				return setupResponse{
					request: &gitalypb.UserDeleteBranchRequest{
						User:       gittest.TestUser,
						Repository: repoProto,
						BranchName: []byte(branchName),
					},
					repoPath:         repoPath,
					expectedResponse: &gitalypb.UserDeleteBranchResponse{},
					expectedRefs:     []string{"master"},
				}
			},
		},
		{
			desc: "branch with refs/heads/ prefix",
			setup: func() setupResponse {
				branchName := "refs/heads/branch"

				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit))

				gittest.Exec(t, cfg, "-C", repoPath, "branch", branchName, "master")

				return setupResponse{
					request: &gitalypb.UserDeleteBranchRequest{
						User:       gittest.TestUser,
						Repository: repoProto,
						BranchName: []byte(branchName),
					},
					repoPath:         repoPath,
					expectedResponse: &gitalypb.UserDeleteBranchResponse{},
					expectedRefs:     []string{"master"},
				}
			},
		},
		{
			desc: "invalid ExpectedOldOID",
			setup: func() setupResponse {
				branchName := "random-branch"

				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit))

				gittest.Exec(t, cfg, "-C", repoPath, "branch", branchName, "master")

				return setupResponse{
					request: &gitalypb.UserDeleteBranchRequest{
						User:           gittest.TestUser,
						Repository:     repoProto,
						BranchName:     []byte(branchName),
						ExpectedOldOid: "foobar",
					},
					repoPath: repoPath,
					expectedErr: structerr.NewInvalidArgument(fmt.Sprintf("invalid expected old object ID: invalid object ID: \"foobar\", expected length %v, got 6", gittest.DefaultObjectHash.EncodedLen())).
						WithInterceptedMetadata("old_object_id", "foobar"),
					expectedRefs: []string{"master", branchName},
				}
			},
		},
		{
			desc: "valid ExpectedOldOID but not present in repo",
			setup: func() setupResponse {
				branchName := "random-branch"

				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit))

				gittest.Exec(t, cfg, "-C", repoPath, "branch", branchName, "master")

				return setupResponse{
					request: &gitalypb.UserDeleteBranchRequest{
						User:           gittest.TestUser,
						Repository:     repoProto,
						BranchName:     []byte(branchName),
						ExpectedOldOid: gittest.DefaultObjectHash.ZeroOID.String(),
					},
					repoPath: repoPath,
					expectedErr: structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found").
						WithInterceptedMetadata("old_object_id", gittest.DefaultObjectHash.ZeroOID),
					expectedRefs: []string{"master", branchName},
				}
			},
		},
		{
			desc: "valid but incorrect ExpectedOldOID",
			setup: func() setupResponse {
				branchName := "random-branch"

				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit))

				gittest.Exec(t, cfg, "-C", repoPath, "branch", branchName, "master")

				return setupResponse{
					request: &gitalypb.UserDeleteBranchRequest{
						User:           gittest.TestUser,
						Repository:     repoProto,
						BranchName:     []byte(branchName),
						ExpectedOldOid: firstCommit.String(),
					},
					repoPath: repoPath,
					expectedErr: structerr.NewFailedPrecondition("reference update failed: Could not update refs/heads/random-branch. Please refresh and try again.").
						WithDetail(&gitalypb.UserDeleteBranchError{
							Error: &gitalypb.UserDeleteBranchError_ReferenceUpdate{
								ReferenceUpdate: &gitalypb.ReferenceUpdateError{
									ReferenceName: []byte("refs/heads/" + branchName),
									OldOid:        firstCommit.String(),
									NewOid:        gittest.DefaultObjectHash.ZeroOID.String(),
								},
							},
						}),
					expectedRefs: []string{"master", branchName},
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			data := tc.setup()

			response, err := client.UserDeleteBranch(ctx, data.request)
			testhelper.RequireGrpcError(t, data.expectedErr, err)
			testhelper.ProtoEqual(t, data.expectedResponse, response)

			refs := text.ChompBytes(gittest.Exec(t, cfg, "-C", data.repoPath, "for-each-ref", "--format=%(refname:short)", "--", "refs/heads/"))
			require.ElementsMatch(t, strings.Split(refs, "\n"), data.expectedRefs)
		})
	}
}

func TestUserDeleteBranch_allowed(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc             string
		allowed          func(context.Context, gitlab.AllowedParams) (bool, string, error)
		expectedErr      error
		expectedResponse *gitalypb.UserDeleteBranchResponse
	}{
		{
			desc: "allowed",
			allowed: func(context.Context, gitlab.AllowedParams) (bool, string, error) {
				return true, "", nil
			},
			expectedResponse: &gitalypb.UserDeleteBranchResponse{},
		},
		{
			desc: "not allowed",
			allowed: func(context.Context, gitlab.AllowedParams) (bool, string, error) {
				return false, "something something", nil
			},
			expectedErr: structerr.NewPermissionDenied("deletion denied by access checks: running pre-receive hooks: GitLab: something something").WithDetail(
				&gitalypb.UserDeleteBranchError{
					Error: &gitalypb.UserDeleteBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							Protocol:     "web",
							UserId:       "user-123",
							ErrorMessage: "something something",
							Changes: []byte(fmt.Sprintf(
								"%s %s refs/heads/branch\n", "549090fbeacc6607bc70648d3ba554c355e670c5", git.ObjectHashSHA1.ZeroOID,
							)),
						},
					},
				},
			),
		},
		{
			desc: "error",
			allowed: func(context.Context, gitlab.AllowedParams) (bool, string, error) {
				return false, "something something", errors.New("something else")
			},
			expectedErr: structerr.NewPermissionDenied("deletion denied by access checks: running pre-receive hooks: GitLab: something else").WithDetail(
				&gitalypb.UserDeleteBranchError{
					Error: &gitalypb.UserDeleteBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							Protocol:     "web",
							UserId:       "user-123",
							ErrorMessage: "something else",
							Changes: []byte(fmt.Sprintf(
								"%s %s refs/heads/branch\n", "549090fbeacc6607bc70648d3ba554c355e670c5", git.ObjectHashSHA1.ZeroOID,
							)),
						},
					},
				},
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx, testserver.WithGitLabClient(
				gitlab.NewMockClient(t, tc.allowed, gitlab.MockPreReceive, gitlab.MockPostReceive),
			))

			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))

			response, err := client.UserDeleteBranch(ctx, &gitalypb.UserDeleteBranchRequest{
				Repository: repo,
				BranchName: []byte("branch"),
				User:       gittest.TestUser,
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}

func TestUserDeleteBranch_concurrentUpdate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("concurrent-update"))

	// Create a git-update-ref(1) process that's locking the "concurrent-update" branch. We do
	// not commit the update yet though to keep the reference locked to simulate concurrent
	// writes to the same reference.
	updater, err := updateref.New(ctx, localrepo.NewTestRepo(t, cfg, repo))
	require.NoError(t, err)
	defer testhelper.MustClose(t, updater)

	require.NoError(t, updater.Start())
	require.NoError(t, updater.Delete("refs/heads/concurrent-update"))
	require.NoError(t, updater.Prepare())

	response, err := client.UserDeleteBranch(ctx, &gitalypb.UserDeleteBranchRequest{
		Repository: repo,
		BranchName: []byte("concurrent-update"),
		User:       gittest.TestUser,
	})
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("reference update failed: Could not update refs/heads/concurrent-update. Please refresh and try again.").WithDetail(
		&gitalypb.UserDeleteBranchError{
			Error: &gitalypb.UserDeleteBranchError_ReferenceUpdate{
				ReferenceUpdate: &gitalypb.ReferenceUpdateError{
					OldOid:        commitID.String(),
					NewOid:        git.ObjectHashSHA1.ZeroOID.String(),
					ReferenceName: []byte("refs/heads/concurrent-update"),
				},
			},
		},
	), err)
	require.Nil(t, response)
}

func TestUserDeleteBranch_hooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	branchNameInput := "to-be-deleted-soon-branch"

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: repo,
		BranchName: []byte(branchNameInput),
		User:       gittest.TestUser,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.Exec(t, cfg, "-C", repoPath, "branch", branchNameInput)

			hookOutputTempPath := gittest.WriteEnvToCustomHook(t, repoPath, hookName)

			_, err := client.UserDeleteBranch(ctx, request)
			require.NoError(t, err)

			output := testhelper.MustReadFile(t, hookOutputTempPath)
			require.Contains(t, string(output), "GL_USERNAME="+gittest.TestUser.GlUsername)
		})
	}
}

func TestUserDeleteBranch_transaction(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})

	// This creates a new branch "delete-me" which exists both in the packed-refs file and as a
	// loose reference. Git will create two reference transactions for this: one transaction to
	// delete the packed-refs reference, and one to delete the loose ref. But given that we want
	// to be independent of how well-packed refs are, we expect to get a single transactional
	// vote, only.
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/delete-me", "master~")
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/delete-me", "master")

	transactionServer := &testTransactionServer{}

	testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterOperationServiceServer(srv, NewServer(
			deps.GetHookManager(),
			deps.GetTxManager(),
			deps.GetLocator(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetUpdaterWithHooks(),
		))
	})

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	client := newMuxedOperationClient(t, ctx, fmt.Sprintf("unix://"+cfg.InternalSocketPath()), cfg.Auth.Token,
		backchannel.NewClientHandshaker(
			testhelper.NewDiscardingLogEntry(t),
			func() backchannel.Server {
				srv := grpc.NewServer()
				gitalypb.RegisterRefTransactionServer(srv, transactionServer)
				return srv
			},
			backchannel.DefaultConfiguration(),
		),
	)

	_, err = client.UserDeleteBranch(ctx, &gitalypb.UserDeleteBranchRequest{
		Repository: repo,
		BranchName: []byte("delete-me"),
		User:       gittest.TestUser,
	})
	require.NoError(t, err)
	require.Equal(t, 2, transactionServer.called)
}

func TestUserDeleteBranch_invalidArgument(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, _, repo, _, client := setupOperationsService(t, ctx)

	testCases := []struct {
		desc     string
		request  *gitalypb.UserDeleteBranchRequest
		response *gitalypb.UserDeleteBranchResponse
		err      error
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: repo,
				BranchName: []byte("does-matter-the-name-if-user-is-empty"),
			},
			response: nil,
			err:      status.Error(codes.InvalidArgument, "bad request: empty user"),
		},
		{
			desc: "empty branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: repo,
				User:       gittest.TestUser,
			},
			response: nil,
			err:      status.Error(codes.InvalidArgument, "bad request: empty branch name"),
		},
		{
			desc: "non-existent branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: repo,
				User:       gittest.TestUser,
				BranchName: []byte("i-do-not-exist"),
			},
			response: nil,
			err:      status.Errorf(codes.FailedPrecondition, "branch not found: %q", "i-do-not-exist"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			response, err := client.UserDeleteBranch(ctx, testCase.request)
			testhelper.RequireGrpcError(t, testCase.err, err)
			testhelper.ProtoEqual(t, testCase.response, response)
		})
	}
}

func TestUserDeleteBranch_hookFailure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	branchNameInput := "to-be-deleted-soon-branch"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", branchNameInput)

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: repo,
		BranchName: []byte(branchNameInput),
		User:       gittest.TestUser,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, tc := range []struct {
		hookName string
		hookType gitalypb.CustomHookError_HookType
	}{
		{
			hookName: "pre-receive",
			hookType: gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
		},
		{
			hookName: "update",
			hookType: gitalypb.CustomHookError_HOOK_TYPE_UPDATE,
		},
	} {
		t.Run(tc.hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, tc.hookName, hookContent)

			response, err := client.UserDeleteBranch(ctx, request)
			testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("deletion denied by custom hooks: running %s hooks: %s\n", tc.hookName, "GL_ID=user-123").WithDetail(
				&gitalypb.UserDeleteBranchError{
					Error: &gitalypb.UserDeleteBranchError_CustomHook{
						CustomHook: &gitalypb.CustomHookError{
							HookType: tc.hookType,
							Stdout:   []byte("GL_ID=user-123\n"),
						},
					},
				},
			), err)

			require.Nil(t, response)

			branches := gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref", "--", "refs/heads/"+branchNameInput)
			require.Contains(t, string(branches), branchNameInput, "branch name does not exist in branches list")
		})
	}
}

func TestBranchHookOutput(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	testCases := []struct {
		desc           string
		hookContent    string
		output         func(hookPath string) string
		expectedStderr string
		expectedStdout string
	}{
		{
			desc:        "empty stdout and empty stderr",
			hookContent: "#!/bin/sh\nexit 1",
			output: func(hookPath string) string {
				return fmt.Sprintf("executing custom hooks: error executing %q: exit status 1", hookPath)
			},
		},
		{
			desc:           "empty stdout and some stderr",
			hookContent:    "#!/bin/sh\necho stderr >&2\nexit 1",
			output:         func(string) string { return "stderr\n" },
			expectedStderr: "stderr\n",
		},
		{
			desc:           "some stdout and empty stderr",
			hookContent:    "#!/bin/sh\necho stdout\nexit 1",
			output:         func(string) string { return "stdout\n" },
			expectedStdout: "stdout\n",
		},
		{
			desc:           "some stdout and some stderr",
			hookContent:    "#!/bin/sh\necho stdout\necho stderr >&2\nexit 1",
			output:         func(string) string { return "stderr\n" },
			expectedStderr: "stderr\n",
			expectedStdout: "stdout\n",
		},
		{
			desc:           "whitespace stdout and some stderr",
			hookContent:    "#!/bin/sh\necho '   '\necho stderr >&2\nexit 1",
			output:         func(string) string { return "stderr\n" },
			expectedStderr: "stderr\n",
			expectedStdout: "   \n",
		},
		{
			desc:           "some stdout and whitespace stderr",
			hookContent:    "#!/bin/sh\necho stdout\necho '   ' >&2\nexit 1",
			output:         func(string) string { return "stdout\n" },
			expectedStderr: "   \n",
			expectedStdout: "stdout\n",
		},
	}

	for _, hookTestCase := range []struct {
		hookName string
		hookType gitalypb.CustomHookError_HookType
	}{
		{
			hookName: "pre-receive",
			hookType: gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
		},
		{
			hookName: "update",
			hookType: gitalypb.CustomHookError_HOOK_TYPE_UPDATE,
		},
	} {
		for _, testCase := range testCases {
			t.Run(hookTestCase.hookName+"/"+testCase.desc, func(t *testing.T) {
				branchNameInput := "some-branch"
				createRequest := &gitalypb.UserCreateBranchRequest{
					Repository: repo,
					BranchName: []byte(branchNameInput),
					StartPoint: []byte("master"),
					User:       gittest.TestUser,
				}
				deleteRequest := &gitalypb.UserDeleteBranchRequest{
					Repository: repo,
					BranchName: []byte(branchNameInput),
					User:       gittest.TestUser,
				}

				hookFilename := gittest.WriteCustomHook(t, repoPath, hookTestCase.hookName, []byte(testCase.hookContent))
				expectedError := testCase.output(hookFilename)

				_, err := client.UserCreateBranch(ctx, createRequest)

				testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("creation denied by custom hooks").WithDetail(
					&gitalypb.UserCreateBranchError{
						Error: &gitalypb.UserCreateBranchError_CustomHook{
							CustomHook: &gitalypb.CustomHookError{
								HookType: hookTestCase.hookType,
								Stdout:   []byte(testCase.expectedStdout),
								Stderr:   []byte(testCase.expectedStderr),
							},
						},
					},
				), err)

				gittest.Exec(t, cfg, "-C", repoPath, "branch", branchNameInput)
				defer gittest.Exec(t, cfg, "-C", repoPath, "branch", "-d", branchNameInput)

				deleteResponse, err := client.UserDeleteBranch(ctx, deleteRequest)
				testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("deletion denied by custom hooks: running %s hooks: %s", hookTestCase.hookName, expectedError).WithDetail(
					&gitalypb.UserDeleteBranchError{
						Error: &gitalypb.UserDeleteBranchError_CustomHook{
							CustomHook: &gitalypb.CustomHookError{
								HookType: hookTestCase.hookType,
								Stdout:   []byte(testCase.expectedStdout),
								Stderr:   []byte(testCase.expectedStderr),
							},
						},
					},
				), err)
				require.Nil(t, deleteResponse)
			})
		}
	}
}
