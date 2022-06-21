package localrepo

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestRepo(t *testing.T) {
	cfg := testcfg.Build(t)

	gittest.TestRepository(t, cfg, func(ctx context.Context, t testing.TB, seeded bool) (git.Repository, string) {
		t.Helper()

		var (
			pbRepo   *gitalypb.Repository
			repoPath string
		)

		if seeded {
			pbRepo, repoPath = gittest.CloneRepo(t, cfg, cfg.Storages[0])
		} else {
			pbRepo, repoPath = gittest.InitRepo(t, cfg, cfg.Storages[0])
		}

		gitCmdFactory := gittest.NewCommandFactory(t, cfg)
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)
		return New(config.NewLocator(cfg), gitCmdFactory, catfileCache, pbRepo), repoPath
	})
}

func TestSize(t *testing.T) {
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	testCases := []struct {
		desc         string
		setup        func(t *testing.T) *gitalypb.Repository
		expectedSize int64
	}{
		{
			desc:         "empty repository",
			expectedSize: 0,
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
				return repoProto
			},
		},
		{
			desc: "referenced commit",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: strings.Repeat("a", 1000)},
					),
					gittest.WithBranch("main"),
				)

				return repoProto
			},
			expectedSize: 203,
		},
		{
			desc: "unreferenced commit",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: strings.Repeat("a", 1000)},
					),
				)

				return repoProto
			},
			expectedSize: 0,
		},
		{
			desc: "modification to blob without repack",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

				rootCommitID := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: strings.Repeat("a", 1000)},
					),
				)

				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(rootCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: strings.Repeat("a", 1001)},
					),
					gittest.WithMessage("modification"),
					gittest.WithBranch("main"),
				)

				return repoProto
			},
			expectedSize: 439,
		},
		{
			desc: "modification to blob after repack",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

				rootCommitID := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: strings.Repeat("a", 1000)},
					),
				)

				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithParents(rootCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: strings.Repeat("a", 1001)},
					),
					gittest.WithMessage("modification"),
					gittest.WithBranch("main"),
				)

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-a", "-d")

				return repoProto
			},
			expectedSize: 398,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto := tc.setup(t)
			repo := New(config.NewLocator(cfg), gitCmdFactory, catfileCache, repoProto)

			ctx := testhelper.Context(t)
			size, err := repo.Size(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

func TestSize_excludeRefs(t *testing.T) {
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	pbRepo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	blob := bytes.Repeat([]byte("a"), 1000)
	blobOID := gittest.WriteBlob(t, cfg, repoPath, blob)
	treeOID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{
			OID:  blobOID,
			Mode: "100644",
			Path: "1kbblob",
		},
	})
	commitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeOID))

	repo := New(config.NewLocator(cfg), gitCmdFactory, catfileCache, pbRepo)

	ctx := testhelper.Context(t)
	sizeBeforeKeepAround, err := repo.Size(ctx)
	require.NoError(t, err)

	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName("refs/keep-around/keep1"), commitOID)

	sizeWithKeepAround, err := repo.Size(ctx)
	require.NoError(t, err)
	assert.Less(t, sizeBeforeKeepAround, sizeWithKeepAround)

	sizeWithoutKeepAround, err := repo.Size(ctx, WithExcludeRefs("refs/keep-around/*"))
	require.NoError(t, err)

	assert.Equal(t, sizeBeforeKeepAround, sizeWithoutKeepAround)
}

func TestSize_excludeAlternates(t *testing.T) {
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	locator := config.NewLocator(cfg)

	pbRepo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	_, altRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	require.NoError(t, os.WriteFile(
		filepath.Join(repoPath, "objects", "info", "alternates"),
		[]byte(filepath.Join(altRepoPath, "objects")),
		os.ModePerm,
	))

	repo := New(locator, gitCmdFactory, catfileCache, pbRepo)

	ctx := testhelper.Context(t)

	gittest.Exec(t, cfg, "-C", repoPath, "gc")

	sizeIncludingAlternates, err := repo.Size(ctx)
	require.NoError(t, err)
	assert.Greater(t, sizeIncludingAlternates, int64(0))

	sizeExcludingAlternates, err := repo.Size(ctx, WithoutAlternates())
	require.NoError(t, err)
	assert.Equal(t, int64(0), sizeExcludingAlternates)
}
