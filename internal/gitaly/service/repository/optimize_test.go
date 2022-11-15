//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func getNewestPackfileModtime(t *testing.T, repoPath string) time.Time {
	t.Helper()

	packFiles, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
	require.NoError(t, err)
	if len(packFiles) == 0 {
		t.Error("no packfiles exist")
	}

	var newestPackfileModtime time.Time

	logrus.Warnf("=== getNewestPackfileModtime")
	for _, packFile := range packFiles {
		info, err := os.Stat(packFile)
		require.NoError(t, err)
		if info.ModTime().After(newestPackfileModtime) {
			newestPackfileModtime = info.ModTime()
		}
		logrus.Warnf("%s %v", packFile, info.ModTime())
	}

	return newestPackfileModtime
}

func TestOptimizeRepository(t *testing.T) {
	t.Parallel()

	logrus.SetOutput(os.Stdout)

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, client := setupRepositoryService(t, ctx)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-b")
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--size-multiple=4", "--split=replace", "--reachable", "--changed-paths")

	hasBitmap, err := stats.HasBitmap(repoPath)
	require.NoError(t, err)
	require.True(t, hasBitmap, "expect a bitmap since we just repacked with -b")

	missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
	require.NoError(t, err)
	require.False(t, missingBloomFilters)

	// get timestamp of latest packfile
	newestsPackfileTime := getNewestPackfileModtime(t, repoPath)

	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "randomStart-http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git.extraHeader-randomEnd", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "hTTp.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.ExtrAheaDeR", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "https.https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.extraHeader", "Authorization: Basic secret-password")
	confFileData := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: repoProto})
	require.NoError(t, err)

	confFileData = testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	logrus.Warn("=== Before comparison")
	require.Equal(t, getNewestPackfileModtime(t, repoPath), newestsPackfileTime, "there should not have been a new packfile created")

	testRepoProto, testRepoPath := gittest.CreateRepository(t, ctx, cfg)

	blobs := 10
	blobIDs := gittest.WriteBlobs(t, cfg, testRepoPath, blobs)

	for _, blobID := range blobIDs {
		gittest.WriteCommit(t, cfg, testRepoPath,
			gittest.WithTreeEntries(gittest.TreeEntry{
				OID: git.ObjectID(blobID), Mode: "100644", Path: "blob",
			}),
			gittest.WithBranch(blobID),
		)
	}

	// Write a blob whose OID is known to have a "17" prefix, which is required such that
	// OptimizeRepository would try to repack at all.
	blobOIDWith17Prefix := gittest.WriteBlob(t, cfg, testRepoPath, []byte("32"))
	require.True(t, strings.HasPrefix(blobOIDWith17Prefix.String(), "17"))

	bitmaps, err := filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.Empty(t, bitmaps)

	mrRefs := filepath.Join(testRepoPath, "refs/merge-requests")
	emptyRef := filepath.Join(mrRefs, "1")
	require.NoError(t, os.MkdirAll(emptyRef, 0o755))
	require.DirExists(t, emptyRef, "sanity check for empty ref dir existence")

	// optimize repository on a repository without a bitmap should call repack full
	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepoProto})
	require.NoError(t, err)

	bitmaps, err = filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.NotEmpty(t, bitmaps)

	missingBloomFilters, err = stats.IsMissingBloomFilters(testRepoPath)
	require.NoError(t, err)
	require.False(t, missingBloomFilters)

	// Empty directories should exist because they're too recent.
	require.DirExists(t, emptyRef)
	require.DirExists(t, mrRefs)
	require.FileExists(t,
		filepath.Join(testRepoPath, "refs/heads", blobIDs[0]),
		"unpacked refs should never be removed",
	)

	// Change the modification time to me older than a day and retry the call. Empty
	// directories must now be deleted.
	oneDayAgo := time.Now().Add(-24 * time.Hour)
	require.NoError(t, os.Chtimes(emptyRef, oneDayAgo, oneDayAgo))
	require.NoError(t, os.Chtimes(mrRefs, oneDayAgo, oneDayAgo))

	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepoProto})
	require.NoError(t, err)

	require.NoFileExists(t, emptyRef)
	require.NoFileExists(t, mrRefs)
}

type mockHousekeepingManager struct {
	housekeeping.Manager
	cfgCh chan housekeeping.OptimizeRepositoryConfig
}

func (m mockHousekeepingManager) OptimizeRepository(_ context.Context, _ *localrepo.Repo, opts ...housekeeping.OptimizeRepositoryOption) error {
	var cfg housekeeping.OptimizeRepositoryConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	m.cfgCh <- cfg
	return nil
}

func TestOptimizeRepository_strategy(t *testing.T) {
	t.Parallel()

	housekeepingManager := mockHousekeepingManager{
		cfgCh: make(chan housekeeping.OptimizeRepositoryConfig, 1),
	}

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t, testserver.WithHousekeepingManager(housekeepingManager))

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.OptimizeRepositoryRequest
		expectedCfg housekeeping.OptimizeRepositoryConfig
	}{
		{
			desc: "no strategy",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: repoProto,
			},
			expectedCfg: housekeeping.OptimizeRepositoryConfig{
				Strategy: housekeeping.HeuristicalOptimizationStrategy{},
			},
		},
		{
			desc: "heuristical strategy",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: repoProto,
				Strategy:   gitalypb.OptimizeRepositoryRequest_STRATEGY_HEURISTICAL,
			},
			expectedCfg: housekeeping.OptimizeRepositoryConfig{
				Strategy: housekeeping.HeuristicalOptimizationStrategy{},
			},
		},
		{
			desc: "eager strategy",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: repoProto,
				Strategy:   gitalypb.OptimizeRepositoryRequest_STRATEGY_EAGER,
			},
			expectedCfg: housekeeping.OptimizeRepositoryConfig{
				Strategy: housekeeping.EagerOptimizationStrategy{},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.OptimizeRepository(ctx, tc.request)
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.OptimizeRepositoryResponse{}, response)

			require.Equal(t, tc.expectedCfg, <-housekeepingManager.cfgCh)
		})
	}
}

func TestOptimizeRepository_validation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, _, client := setupRepositoryService(t, ctx)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.OptimizeRepositoryRequest
		expectedErr error
	}{
		{
			desc:    "empty repository",
			request: &gitalypb.OptimizeRepositoryRequest{},
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefectMessage(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "invalid repository storage",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "non-existent",
					RelativePath: repo.GetRelativePath(),
				},
			},
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefectMessage(
				`GetStorageByName: no such storage: "non-existent"`,
				"repo scoped: invalid Repository"),
			),
		},
		{
			desc: "invalid repository path",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.GetStorageName(),
					RelativePath: "path/not/exist",
				},
			},
			expectedErr: helper.ErrNotFoundf(testhelper.GitalyOrPraefectMessage(
				fmt.Sprintf(`GetRepoPath: not a git repository: "%s/path/not/exist"`, cfg.Storages[0].Path),
				`routing repository maintenance: getting repository metadata: repository not found`,
			)),
		},
		{
			desc: "invalid optimization strategy",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: repo,
				Strategy:   12,
			},
			expectedErr: helper.ErrInvalidArgumentf("unsupported optimization strategy 12"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.OptimizeRepository(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
