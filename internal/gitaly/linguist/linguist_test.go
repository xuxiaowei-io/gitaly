package linguist

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestInstance_Stats(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	linguist, err := New(cfg, gitCmdFactory)
	require.NoError(t, err)

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	commitID := git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312")

	for _, tc := range []struct {
		desc          string
		setup         func(t *testing.T) (*gitalypb.Repository, string, git.ObjectID)
		expectedStats ByteCountPerLanguage
		expectedErr   string
	}{
		{
			desc: "successful",
			setup: func(t *testing.T) (*gitalypb.Repository, string, git.ObjectID) {
				repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				return repoProto, repoPath, commitID
			},
			expectedStats: map[string]uint64{
				"CoffeeScript": 107,
				"HTML":         349,
				"JavaScript":   1014,
				"Ruby":         2943,
			},
		},
		{
			desc: "preexisting cache",
			setup: func(t *testing.T) (*gitalypb.Repository, string, git.ObjectID) {
				repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				repo := localrepo.NewTestRepo(t, cfg, repoProto)

				// We simply run the linguist once before so that it can already
				// write the cache.
				_, err := linguist.Stats(ctx, repo, commitID.String(), catfileCache)
				require.NoError(t, err)
				require.FileExists(t, filepath.Join(repoPath, "language-stats.cache"))

				return repoProto, repoPath, commitID
			},
			expectedStats: map[string]uint64{
				"CoffeeScript": 107,
				"HTML":         349,
				"JavaScript":   1014,
				"Ruby":         2943,
			},
		},
		{
			desc: "corrupted cache",
			setup: func(t *testing.T) (*gitalypb.Repository, string, git.ObjectID) {
				repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "language-stats.cache"), []byte("garbage"), 0o644))

				return repoProto, repoPath, commitID
			},
			expectedStats: map[string]uint64{
				"CoffeeScript": 107,
				"HTML":         349,
				"JavaScript":   1014,
				"Ruby":         2943,
			},
		},
		{
			desc: "old cache",
			setup: func(t *testing.T) (*gitalypb.Repository, string, git.ObjectID) {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
				repo := localrepo.NewTestRepo(t, cfg, repoProto)

				oldCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "main.rb", Content: "require 'fileutils'", Mode: "100644"},
				))
				newCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommitID), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "main.go", Content: "package main", Mode: "100644"},
				))

				// Precreate the cache with the old commit. This ensures that
				// linguist knows to update the cache.
				stats, err := linguist.Stats(ctx, repo, oldCommitID.String(), catfileCache)
				require.NoError(t, err)
				require.FileExists(t, filepath.Join(repoPath, "language-stats.cache"))
				require.Equal(t, ByteCountPerLanguage{
					"Ruby": 19,
				}, stats)

				return repoProto, repoPath, newCommitID
			},
			expectedStats: map[string]uint64{
				"Go": 12,
			},
		},
		{
			desc: "missing repository",
			setup: func(t *testing.T) (*gitalypb.Repository, string, git.ObjectID) {
				repoPath := filepath.Join(testhelper.TempDir(t), "nonexistent")
				repoProto := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "nonexistent"}

				return repoProto, repoPath, commitID
			},
			expectedErr: "GetRepoPath: not a git repository",
		},
		{
			desc: "missing commit",
			setup: func(t *testing.T) (*gitalypb.Repository, string, git.ObjectID) {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
				return repoProto, repoPath, commitID
			},
			expectedErr: "waiting for linguist: exit status 1",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath, objectID := tc.setup(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			stats, err := linguist.Stats(ctx, repo, objectID.String(), catfileCache)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedStats, stats)
				require.FileExists(t, filepath.Join(repoPath, "language-stats.cache"))
			} else {
				require.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

func TestInstance_Stats_unmarshalJSONError(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	repo := localrepo.New(config.NewLocator(cfg), gitCmdFactory, catfileCache, invalidRepo)

	ling, err := New(cfg, gitCmdFactory)
	require.NoError(t, err)

	// When an error occurs, this used to trigger JSON marshelling of a plain string
	// the new behaviour shouldn't do that, and return an command error
	_, err = ling.Stats(ctx, repo, "deadbeef", catfileCache)
	require.Error(t, err)

	_, ok := err.(*json.SyntaxError)
	require.False(t, ok, "expected the error not be a json Syntax Error")
}

func TestNew(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithRealLinguist())

	ling, err := New(cfg, gittest.NewCommandFactory(t, cfg))
	require.NoError(t, err)

	require.Equal(t, "#701516", ling.Color("Ruby"), "color value for 'Ruby'")
}

func TestNew_loadLanguagesCustomPath(t *testing.T) {
	jsonPath, err := filepath.Abs("testdata/fake-languages.json")
	require.NoError(t, err)

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{Ruby: config.Ruby{LinguistLanguagesPath: jsonPath}}))

	ling, err := New(cfg, gittest.NewCommandFactory(t, cfg))
	require.NoError(t, err)

	require.Equal(t, "foo color", ling.Color("FooBar"))
}
