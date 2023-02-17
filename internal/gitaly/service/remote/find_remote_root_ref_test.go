package remote

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindRemoteRootRef(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRemoteService(t, ctx)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	// Even though FindRemoteRootRef does theoretically not require a local repository it is
	// still bound to one right now. We thus create an empty repository up front that we can
	// reuse.
	localRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	type setupData struct {
		request          *gitalypb.FindRemoteRootRefRequest
		expectedErr      error
		expectedResponse *gitalypb.FindRemoteRootRefResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "invalid repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
						RemoteUrl:  "remote-url",
					},
					expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
						`GetStorageByName: no such storage: "fake"`,
						"repo scoped: invalid Repository",
					)),
				}
			},
		},
		{
			desc: "missing repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						RemoteUrl: "remote-url",
					},
					expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
						"empty Repository",
						"repo scoped: empty Repository",
					)),
				}
			},
		},
		{
			desc: "missing remote URL",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: localRepo,
					},
					expectedErr: structerr.NewInvalidArgument("missing remote URL"),
				}
			},
		},
		{
			desc: "successful",
			setup: func(t *testing.T) setupData {
				host := "example.com"
				secret := "mysecret"

				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))

				port := gittest.HTTPServer(t, ctx, gitCmdFactory, remoteRepoPath, newGitRequestValidationMiddleware(host, secret))
				originURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(remoteRepoPath))

				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository:              localRepo,
						RemoteUrl:               originURL,
						HttpAuthorizationHeader: secret,
						HttpHost:                host,
					},
					expectedResponse: &gitalypb.FindRemoteRootRefResponse{
						Ref: "main",
					},
				}
			},
		},
		{
			desc: "unborn HEAD",
			setup: func(t *testing.T) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: localRepo,
						RemoteUrl:  "file://" + remoteRepoPath,
					},
					expectedErr: status.Error(codes.NotFound, "no remote HEAD found"),
				}
			},
		},
		{
			desc: "invalid remote URL",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.FindRemoteRootRefRequest{
						Repository: localRepo,
						RemoteUrl:  "file://" + testhelper.TempDir(t),
					},
					expectedErr: structerr.New("exit status 128"),
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			response, err := client.FindRemoteRootRef(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, response)
		})
	}
}

func newGitRequestValidationMiddleware(host, secret string) func(http.ResponseWriter, *http.Request, http.Handler) {
	return func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		if r.Host != host {
			http.Error(w, "No Host", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Authorization") != secret {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func TestServer_findRemoteRootRefCmd(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	port := gittest.HTTPServer(t, ctx, gitCmdFactory, repoPath, nil)

	s := server{gitCmdFactory: gitCmdFactory}

	originalURL := fmt.Sprintf("http://example.com:%d/%s", port, filepath.Base(repoPath))
	resolvedURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(repoPath))

	for _, tc := range []struct {
		desc           string
		request        *gitalypb.FindRemoteRootRefRequest
		expectedErr    error
		expectedConfig []string
	}{
		{
			desc: "no resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       resolvedURL,
				ResolvedAddress: "",
				Repository:      repo,
			},
			expectedConfig: []string{
				"GIT_CONFIG_KEY_0=remote.inmemory.url",
				"GIT_CONFIG_VALUE_0=" + resolvedURL,
			},
		},
		{
			desc: "resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       originalURL,
				ResolvedAddress: "127.0.0.1",
				Repository:      repo,
			},
			expectedConfig: []string{
				"GIT_CONFIG_KEY_0=http.curloptResolve",
				fmt.Sprintf("GIT_CONFIG_VALUE_0=example.com:%d:127.0.0.1", port),
				"GIT_CONFIG_KEY_1=remote.inmemory.url",
				"GIT_CONFIG_VALUE_1=" + originalURL,
			},
		},
		{
			desc: "corrupt resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       originalURL,
				ResolvedAddress: "foo/bar",
				Repository:      repo,
			},
			expectedErr: structerr.NewInvalidArgument("couldn't get curloptResolve config: %w", fmt.Errorf("resolved address has invalid IPv4/IPv6 address")),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd, err := s.findRemoteRootRefCmd(ctx, tc.request)
			require.Equal(t, tc.expectedErr, err)
			if err == nil {
				require.NoError(t, cmd.Wait())
				require.Subset(t, cmd.Env(), tc.expectedConfig)
			}
		})
	}
}
