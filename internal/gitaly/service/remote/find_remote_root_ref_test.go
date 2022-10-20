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
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindRemoteRootRefSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRemoteService(t, ctx)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	const (
		host   = "example.com"
		secret = "mysecret"
	)

	port := gittest.HTTPServer(t, ctx, gitCmdFactory, repoPath, newGitRequestValidationMiddleware(host, secret))

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
	cfg, remoteRepo, remoteRepoPath, client := setupRemoteService(t, ctx)

	// We're creating an empty repository. Empty repositories do have a HEAD set up, but they
	// point to an unborn branch because the default branch hasn't yet been created.
	_, clientRepoPath := gittest.CreateRepository(t, ctx, cfg)
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
	_, repo, _, client := setupRemoteService(t, ctx)

	testCases := []struct {
		desc        string
		request     *gitalypb.FindRemoteRootRefRequest
		expectedErr error
	}{
		{
			desc: "Invalid repository",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
				RemoteUrl:  "remote-url",
			},
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefect(
				`GetStorageByName: no such storage: "fake"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "Repository is nil",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl: "remote-url",
			},
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
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
	_, repo, _, client := setupRemoteService(t, ctx)

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
				fmt.Sprintf("GIT_CONFIG_VALUE_0=*:%d:127.0.0.1", port),
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
			expectedErr: helper.ErrInvalidArgumentf("couldn't get curloptResolve config: %w", fmt.Errorf("resolved address has invalid IPv4/IPv6 address")),
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
