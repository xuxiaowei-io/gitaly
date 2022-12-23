package housekeeping

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestWriteCommitGraphConfigForRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		setup          func(t *testing.T, repo *localrepo.Repo)
		expectedErr    error
		expectedConfig WriteCommitGraphConfig
	}{
		{
			desc: "without commit-graph",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "monolithic commit-graph without bloom filter",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))
				git.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable")
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "monolithic commit-graph with bloom filter",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))
				git.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--changed-paths")
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "split commit-graph without bloom filter",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))
				git.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split")
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "split commit-graph with bloom filter",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))
				git.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split", "--changed-paths")
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: false,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := git.CreateRepository(t, ctx, cfg, git.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.setup(t, repo)

			config, err := WriteCommitGraphConfigForRepository(ctx, repo)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedConfig, config)
		})
	}
}
