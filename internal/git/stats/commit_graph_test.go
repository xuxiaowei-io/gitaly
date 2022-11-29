package stats

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestCommitGraphInfoForRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc         string
		args         []string
		expectedErr  error
		expectedInfo CommitGraphInfo
	}{
		{
			desc:         "no commit graph filter",
			args:         nil,
			expectedInfo: CommitGraphInfo{},
		},
		{
			desc: "single commit graph without bloom filter",
			args: []string{"commit-graph", "write", "--reachable"},
			expectedInfo: CommitGraphInfo{
				Exists: true,
			},
		},
		{
			desc: "single commit graph with bloom filter",
			args: []string{"commit-graph", "write", "--reachable", "--changed-paths"},
			expectedInfo: CommitGraphInfo{
				Exists:          true,
				HasBloomFilters: true,
			},
		},
		{
			desc: "split commit graph without bloom filter",
			args: []string{"commit-graph", "write", "--reachable", "--split"},
			expectedInfo: CommitGraphInfo{
				Exists:                 true,
				CommitGraphChainLength: 1,
			},
		},
		{
			desc: "split commit graph with bloom filter",
			args: []string{"commit-graph", "write", "--reachable", "--split", "--changed-paths"},
			expectedInfo: CommitGraphInfo{
				Exists:                 true,
				HasBloomFilters:        true,
				CommitGraphChainLength: 1,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

			if len(tc.args) > 0 {
				gittest.Exec(t, cfg, append([]string{"-C", repoPath}, tc.args...)...)
			}

			info, err := CommitGraphInfoForRepository(repoPath)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedInfo, info)
		})
	}
}
