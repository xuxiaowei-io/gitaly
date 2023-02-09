//go:build !gitaly_test_sha256

package repository

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestMidxWrite(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	//nolint:staticcheck
	_, err := client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{Repository: repo})
	assert.NoError(t, err)

	require.FileExists(t,
		filepath.Join(repoPath, MidxRelPath),
		"multi-pack-index should exist after running MidxRepack",
	)

	configEntries := gittest.Exec(t, cfg, "-C", repoPath, "config", "--local", "--list")
	require.NotContains(t, configEntries, "core.muiltipackindex")
}

func TestMidxRewrite(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	midxPath := filepath.Join(repoPath, MidxRelPath)

	// Create an invalid multi-pack-index file
	// with mtime update being the basis for comparison
	require.NoError(t, os.WriteFile(midxPath, nil, perm.SharedFile))
	require.NoError(t, os.Chtimes(midxPath, time.Time{}, time.Time{}))
	info, err := os.Stat(midxPath)
	require.NoError(t, err)
	mt := info.ModTime()

	//nolint:staticcheck
	_, err = client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{Repository: repo})
	require.NoError(t, err)

	require.FileExists(t,
		filepath.Join(repoPath, MidxRelPath),
		"multi-pack-index should exist after running MidxRepack",
	)

	assertModTimeAfter(t, mt, midxPath)
}

func TestMidxRepack(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	// add some pack files with different sizes
	packsAdded := uint64(5)
	addPackFiles(t, ctx, cfg, repoPath, packsAdded, true)

	// record pack count
	actualCount, err := stats.PackfilesCount(repo)
	require.NoError(t, err)
	require.Equal(t,
		packsAdded+1, // expect
		actualCount,  // actual
		"New pack files should have been created",
	)

	//nolint:staticcheck
	_, err = client.MidxRepack(
		ctx,
		&gitalypb.MidxRepackRequest{
			Repository: repoProto,
		},
	)
	require.NoError(t, err)

	actualCount, err = stats.PackfilesCount(repo)
	require.NoError(t, err)
	require.Equal(t,
		packsAdded+2, // expect
		actualCount,  // actual
		"At least 1 pack file should have been created",
	)

	_, newPackFileInfo := findNewestPackFile(t, repoPath)
	assert.True(t, newPackFileInfo.ModTime().After(time.Time{}))
}

func TestMidxRepack_transactional(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	txManager := transaction.NewTrackingManager()

	cfg, client := setupRepositoryServiceWithoutRepo(t, testserver.WithTransactionManager(txManager))
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")

	// Reset the votes after creating the test repository.
	txManager.Reset()

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})
	ctx = metadata.IncomingToOutgoing(ctx)

	//nolint:staticcheck
	_, err = client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{
		Repository: repo,
	})
	require.NoError(t, err)

	require.Equal(t, 2, len(txManager.Votes()))

	multiPackIndex := gittest.Exec(t, cfg, "-C", repoPath, "config", "core.multiPackIndex")
	require.Equal(t, "true", text.ChompBytes(multiPackIndex))
}

func TestMidxRepackExpire(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	for _, packsAdded := range []uint64{3, 5, 11, 20} {
		t.Run(fmt.Sprintf("Test repack expire with %d added packs", packsAdded),
			func(t *testing.T) {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				// add some pack files with different sizes
				addPackFiles(t, ctx, cfg, repoPath, packsAdded, false)

				// record pack count
				actualCount, err := stats.PackfilesCount(repo)
				require.NoError(t, err)
				require.Equal(t,
					packsAdded+1, // expect
					actualCount,  // actual
					"New pack files should have been created",
				)

				// here we assure that for n packCount
				// we should need no more than n interation(s)
				// for the pack files to be consolidated into
				// a new second biggest pack
				var i uint64
				packCount := packsAdded + 1
				for {
					if i > packsAdded+1 {
						break
					}
					i++

					//nolint:staticcheck
					_, err := client.MidxRepack(
						ctx,
						&gitalypb.MidxRepackRequest{
							Repository: repoProto,
						},
					)
					require.NoError(t, err)

					packCount, err = stats.PackfilesCount(repo)
					require.NoError(t, err)

					if packCount == 2 {
						break
					}
				}

				require.Equal(t,
					uint64(2), // expect
					packCount, // actual
					fmt.Sprintf(
						"all small packs should be consolidated to a second biggest pack "+
							"after at most %d iterations (actual %d))",
						packCount,
						i,
					),
				)
			})
	}
}

// findNewestPackFile returns the latest created pack file in repo's odb
func findNewestPackFile(t *testing.T, repoPath string) (fs.DirEntry, fs.FileInfo) {
	t.Helper()

	files, err := getPackfiles(repoPath)
	require.NoError(t, err)

	var newestPack fs.DirEntry
	var newestPackInfo fs.FileInfo
	for _, f := range files {
		info, err := f.Info()
		require.NoError(t, err)

		if newestPack == nil || info.ModTime().After(newestPackInfo.ModTime()) {
			newestPack = f
			newestPackInfo = info
		}
	}
	require.NotNil(t, newestPack)

	return newestPack, newestPackInfo
}

// addPackFiles creates some packfiles by
// creating some commits objects and repack them.
func addPackFiles(
	t *testing.T,
	ctx context.Context,
	cfg config.Cfg,
	repoPath string,
	packCount uint64,
	resetModTime bool,
) {
	t.Helper()

	// Do a full repack to ensure we start with 1 pack.
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")

	randomReader := rand.New(rand.NewSource(1))

	// Create some pack files with different sizes.
	for i := uint64(0); i < packCount; i++ {
		buf := make([]byte, (packCount+1)*100)
		_, err := io.ReadFull(randomReader, buf)
		require.NoError(t, err)

		gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithMessage(hex.EncodeToString(buf)),
			gittest.WithBranch(fmt.Sprintf("branch-%d", i)),
		)

		gittest.Exec(t, cfg, "-C", repoPath, "repack", "-d")
	}

	// Reset mtime of packfile to mark them separately for comparison purpose.
	if resetModTime {
		packDir := filepath.Join(repoPath, "objects/pack/")

		files, err := getPackfiles(repoPath)
		require.NoError(t, err)

		for _, f := range files {
			require.NoError(t, os.Chtimes(filepath.Join(packDir, f.Name()), time.Time{}, time.Time{}))
		}
	}
}

func TestMidxRepack_validationChecks(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.MidxRepackRequest
		expectedErr error
	}{
		{
			desc: "no repository",
			req:  &gitalypb.MidxRepackRequest{},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "invalid storage",
			req:  &gitalypb.MidxRepackRequest{Repository: &gitalypb.Repository{RelativePath: "stub", StorageName: "invalid"}},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				`setting config: GetStorageByName: no such storage: "invalid"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "not existing repository",
			req:  &gitalypb.MidxRepackRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "invalid"}},
			expectedErr: status.Error(codes.NotFound, testhelper.GitalyOrPraefect(
				fmt.Sprintf(`setting config: GetRepoPath: not a git repository: "%s/invalid"`, cfg.Storages[0].Path),
				"routing repository maintenance: getting repository metadata: repository not found",
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.MidxRepack(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
