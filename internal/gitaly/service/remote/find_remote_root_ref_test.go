//go:build !gitaly_test_sha256

package remote

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindRemoteRootRefSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRemoteService(ctx, t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	const (
		host   = "example.com"
		secret = "mysecret"
	)

	port, stopGitServer := gittest.HTTPServer(ctx, t, gitCmdFactory, repoPath, newGitRequestValidationMiddleware(host, secret))
	defer func() { require.NoError(t, stopGitServer()) }()

	originURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(repoPath))

	for _, tc := range []struct {
		desc    string
		request *gitalypb.FindRemoteRootRefRequest
	}{
		{
			desc: "with remote URL",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository:              repo,
				RemoteUrl:               originURL,
				HttpAuthorizationHeader: secret,
				HttpHost:                host,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.FindRemoteRootRef(ctx, tc.request)
			require.NoError(t, err)
			require.Equal(t, "master", response.Ref)
		})
	}
}

func TestFindRemoteRootRefWithUnbornRemoteHead(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, remoteRepo, remoteRepoPath, client := setupRemoteService(ctx, t)

	// We're creating an empty repository. Empty repositories do have a HEAD set up, but they
	// point to an unborn branch because the default branch hasn't yet been created.
	_, clientRepoPath := gittest.CreateRepository(ctx, t, cfg)
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "remote", "add", "foo", "file://"+clientRepoPath)
	response, err := client.FindRemoteRootRef(ctx, &gitalypb.FindRemoteRootRefRequest{
		Repository: remoteRepo,
		RemoteUrl:  "file://" + clientRepoPath,
	},
	)
	testhelper.RequireGrpcError(t, status.Error(codes.NotFound, "no remote HEAD found"), err)
	require.Nil(t, response)
}

func TestFindRemoteRootRefFailedDueToValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	// We're running tests with Praefect disabled given that we don't want to exercise
	// Praefect's validation, but Gitaly's.
	_, repo, _, client := setupRemoteService(ctx, t, testserver.WithDisablePraefect())

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc        string
		request     *gitalypb.FindRemoteRootRefRequest
		expectedErr error
	}{
		{
			desc: "Invalid repository",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: invalidRepo,
				RemoteUrl:  "remote-url",
			},
			expectedErr: helper.ErrInvalidArgumentf("GetStorageByName: no such storage: \"fake\""),
		},
		{
			desc: "Repository is nil",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl: "remote-url",
			},
			expectedErr: helper.ErrInvalidArgumentf("missing repository"),
		},
		{
			desc: "Remote URL is empty",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: repo,
			},
			expectedErr: helper.ErrInvalidArgumentf("missing remote URL"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			_, err := client.FindRemoteRootRef(ctx, testCase.request)
			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}

func TestFindRemoteRootRefFailedDueToInvalidRemote(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRemoteService(ctx, t)

	t.Run("invalid remote URL", func(t *testing.T) {
		fakeRepoDir := testhelper.TempDir(t)

		// We're using a nonexistent filepath remote URL so we avoid hitting the internet.
		request := &gitalypb.FindRemoteRootRefRequest{
			Repository: repo, RemoteUrl: "file://" + fakeRepoDir,
		}

		_, err := client.FindRemoteRootRef(ctx, request)
		testhelper.RequireGrpcCode(t, err, codes.Internal)
	})
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

	repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	s := server{gitCmdFactory: gittest.NewCommandFactory(t, cfg)}

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.FindRemoteRootRefRequest
		expectedErrorStr string
		expectedConfig   []string
	}{
		{
			desc: "no resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       "git@gitlab.com:foo/bar.git",
				ResolvedAddress: "",
				Repository:      repo,
			},
			expectedConfig: []string{
				"GIT_CONFIG_KEY_0=remote.inmemory.url",
				"GIT_CONFIG_VALUE_0=git@gitlab.com:foo/bar.git",
			},
		},
		{
			desc: "resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       "https://gitlab.com/foo/bar.git",
				ResolvedAddress: "192.168.0.1",
				Repository:      repo,
			},
			expectedConfig: []string{
				"GIT_CONFIG_KEY_0=http.curloptResolve",
				"GIT_CONFIG_VALUE_0=*:443:192.168.0.1",
			},
		},
		{
			desc: "corrupt resolved address is present",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl:       "git@gitlab.com:foo/bar.git",
				ResolvedAddress: "foo/bar",
				Repository:      repo,
			},
			expectedErrorStr: "resolved address has invalid IPv4/IPv6 address",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd, err := s.findRemoteRootRefCmd(ctx, tc.request)
			if err == nil {
				defer func() {
					err := cmd.Wait()
					require.Error(t, err)
				}()
			}

			if tc.expectedErrorStr != "" {
				require.Error(t, err, tc.expectedErrorStr)
			} else {
				require.NoError(t, err)
				require.Subset(t, cmd.Env(), tc.expectedConfig)
			}
		})
	}
}
