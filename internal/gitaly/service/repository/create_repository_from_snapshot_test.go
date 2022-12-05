//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/praefectutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	secret       = "Magic secret"
	host         = "example.com"
	redirectPath = "/redirecting-snapshot.tar"
	tarPath      = "/snapshot.tar"
)

type tarTesthandler struct {
	tarData io.Reader
	host    string
	secret  string
}

func (h *tarTesthandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.host != "" && r.Host != h.host {
		http.Error(w, "No Host", http.StatusBadRequest)
		return
	}
	if r.Header.Get("Authorization") != h.secret {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	switch r.RequestURI {
	case redirectPath:
		http.Redirect(w, r, tarPath, http.StatusFound)
	case tarPath:
		if _, err := io.Copy(w, h.tarData); err != nil {
			panic(err)
		}
	default:
		http.Error(w, "Not found", 404)
	}
}

// Create a tar file for the repo in memory, without relying on TarBuilder
func generateTarFile(t *testing.T, path string) ([]byte, []string) {
	var data []byte
	if runtime.GOOS == "darwin" {
		data = testhelper.MustRunCommand(t, nil, "tar", "-C", path, "--no-mac-metadata", "-cf", "-", ".")
	} else {
		data = testhelper.MustRunCommand(t, nil, "tar", "-C", path, "-cf", "-", ".")
	}

	entries, err := archive.TarEntries(bytes.NewReader(data))
	require.NoError(t, err)

	return data, entries
}

func TestCreateRepositoryFromSnapshot_success(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	client, socketPath := runRepositoryService(t, cfg, nil)
	cfg.SocketPath = socketPath

	_, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	// Ensure these won't be in the archive
	require.NoError(t, os.Remove(filepath.Join(sourceRepoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(sourceRepoPath, "hooks")))

	data, entries := generateTarFile(t, sourceRepoPath)

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&tarTesthandler{tarData: bytes.NewReader(data), secret: secret, host: host})
	defer srv.Close()

	repoRelativePath := filepath.Join("non-existing-parent", "repository")

	repo := &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: repoRelativePath,
	}
	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repo,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
		HttpHost:   host,
	}

	rsp, err := client.CreateRepositoryFromSnapshot(ctx, req)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, rsp, &gitalypb.CreateRepositoryFromSnapshotResponse{})

	repoAbsolutePath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, repo))
	require.DirExists(t, repoAbsolutePath)
	for _, entry := range entries {
		if strings.HasSuffix(entry, "/") {
			require.DirExists(t, filepath.Join(repoAbsolutePath, entry), "directory %q not unpacked", entry)
		} else {
			require.FileExists(t, filepath.Join(repoAbsolutePath, entry), "file %q not unpacked", entry)
		}
	}

	// hooks/ and config were excluded, but the RPC should create them
	require.FileExists(t, filepath.Join(repoAbsolutePath, "config"), "Config file not created")
}

func TestCreateRepositoryFromSnapshot_repositoryExists(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	client, socketPath := runRepositoryService(t, cfg, nil)
	cfg.SocketPath = socketPath

	// This creates the first repository on the server. As this test can run with Praefect in front of it,
	// we'll use the next replica path Praefect will assign in order to ensure this repository creation
	// conflicts even with Praefect in front of it.
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		RelativePath: praefectutil.DeriveReplicaPath(1),
		Seed:         gittest.SeedGitLabTest,
	})

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{Repository: repo}
	rsp, err := client.CreateRepositoryFromSnapshot(ctx, req)
	testhelper.RequireGrpcCode(t, err, codes.AlreadyExists)
	if testhelper.IsPraefectEnabled() {
		require.Contains(t, err.Error(), "route repository creation: reserve repository id: repository already exists")
	} else {
		require.Contains(t, err.Error(), "creating repository: repository exists already")
	}
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshot_badURL(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	client, socketPath := runRepositoryService(t, cfg, nil)
	cfg.SocketPath = socketPath

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: gittest.NewRepositoryName(t),
		},
		HttpUrl: "invalid!scheme://invalid.invalid",
	}

	rsp, err := client.CreateRepositoryFromSnapshot(ctx, req)
	testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
	require.Contains(t, err.Error(), "Bad HTTP URL")
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshot_invalidArguments(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	testCases := []struct {
		desc            string
		url             string
		auth            string
		resolvedAddress string
		code            codes.Code
		errContains     string
	}{
		{
			desc:        "http bad auth",
			url:         tarPath,
			auth:        "Bad authentication",
			code:        codes.Internal,
			errContains: "HTTP server: 401 ",
		},
		{
			desc:        "http not found",
			url:         tarPath + ".does-not-exist",
			auth:        secret,
			code:        codes.Internal,
			errContains: "HTTP server: 404 ",
		},
		{
			desc:        "http do not follow redirects",
			url:         redirectPath,
			auth:        secret,
			code:        codes.Internal,
			errContains: "HTTP server: 302 ",
		},
		{
			desc:            "resolved address not in expected format",
			url:             tarPath,
			auth:            secret,
			resolvedAddress: "foo/bar",
			code:            codes.InvalidArgument,
			errContains:     "creating resolved HTTP client: invalid resolved address",
		},
	}

	srv := httptest.NewServer(&tarTesthandler{secret: secret, host: host})
	defer srv.Close()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)
			client, socketPath := runRepositoryService(t, cfg, nil)
			cfg.SocketPath = socketPath

			req := &gitalypb.CreateRepositoryFromSnapshotRequest{
				Repository: &gitalypb.Repository{
					StorageName:  cfg.Storages[0].Name,
					RelativePath: gittest.NewRepositoryName(t),
				},
				HttpUrl:         srv.URL + tc.url,
				HttpAuth:        tc.auth,
				HttpHost:        host,
				ResolvedAddress: tc.resolvedAddress,
			}

			rsp, err := client.CreateRepositoryFromSnapshot(ctx, req)
			testhelper.RequireGrpcCode(t, err, tc.code)
			require.Nil(t, rsp)
			require.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestCreateRepositoryFromSnapshot_malformedResponse(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	client, socketPath := runRepositoryService(t, cfg, nil)
	cfg.SocketPath = socketPath

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	require.NoError(t, os.Remove(filepath.Join(repoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "hooks")))

	data, _ := generateTarFile(t, repoPath)
	// Only serve half of the tar file
	dataReader := io.LimitReader(bytes.NewReader(data), int64(len(data)/2))

	srv := httptest.NewServer(&tarTesthandler{tarData: dataReader, secret: secret, host: host})
	defer srv.Close()

	// Delete the repository so we can re-use the path
	require.NoError(t, os.RemoveAll(repoPath))

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repo,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
		HttpHost:   host,
	}

	rsp, err := client.CreateRepositoryFromSnapshot(ctx, req)
	require.Error(t, err)
	require.Nil(t, rsp)

	// Ensure that a partial result is not left in place
	require.NoFileExists(t, repoPath)
}

func TestCreateRepositoryFromSnapshot_resolvedAddressSuccess(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	client, socketPath := runRepositoryService(t, cfg, nil)
	cfg.SocketPath = socketPath

	_, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)

	// Ensure these won't be in the archive
	require.NoError(t, os.Remove(filepath.Join(sourceRepoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(sourceRepoPath, "hooks")))

	data, entries := generateTarFile(t, sourceRepoPath)

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&tarTesthandler{tarData: bytes.NewReader(data), secret: secret, host: host})
	defer srv.Close()

	repoRelativePath := gittest.NewRepositoryName(t)

	repo := &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: repoRelativePath,
	}

	// Any URL should be resolved to the provided IP in resolved address
	// so provide a random URL and it should work as long as the resolved
	// address is correct (here we're utilizing the fact that HTTP doesn't
	// have SSL verification).
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	randomHostname := fmt.Sprintf("http://www.example.com:%s%s", u.Port(), tarPath)

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository:      repo,
		HttpUrl:         randomHostname,
		HttpAuth:        secret,
		ResolvedAddress: u.Hostname(),
		HttpHost:        host,
	}

	rsp, err := client.CreateRepositoryFromSnapshot(ctx, req)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, rsp, &gitalypb.CreateRepositoryFromSnapshotResponse{})

	repoAbsolutePath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, repo))
	require.DirExists(t, repoAbsolutePath)
	for _, entry := range entries {
		if strings.HasSuffix(entry, "/") {
			require.DirExists(t, filepath.Join(repoAbsolutePath, entry), "directory %q not unpacked", entry)
		} else {
			require.FileExists(t, filepath.Join(repoAbsolutePath, entry), "file %q not unpacked", entry)
		}
	}

	// hooks/ and config were excluded, but the RPC should create them
	require.FileExists(t, filepath.Join(repoAbsolutePath, "config"), "Config file not created")
}

func TestServer_CreateRepositoryFromSnapshot_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, _, _, client := setupRepositoryService(t, ctx)

	testCases := []struct {
		desc        string
		req         *gitalypb.CreateRepositoryFromSnapshotRequest
		expectedErr error
	}{
		{
			desc: "no repository provided",
			req:  &gitalypb.CreateRepositoryFromSnapshotRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CreateRepositoryFromSnapshot(ctx, tc.req)
			require.Error(t, err)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
