package housekeeping

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestWriteCommitGraphConfigForRepository(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UseCommitGraphGenerationData).Run(t, testWriteCommitGraphConfigForRepository)
}

func testWriteCommitGraphConfigForRepository(t *testing.T, ctx context.Context) {
	t.Parallel()

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

				repoPath, err := repo.Path()
				require.NoError(t, err)
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable")
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "monolithic commit-graph with bloom filter",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))

				repoPath, err := repo.Path()
				require.NoError(t, err)
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--changed-paths")
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "split commit-graph without bloom filter",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))

				repoPath, err := repo.Path()
				require.NoError(t, err)
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split")
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "split commit-graph with bloom filter without generation data",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))

				repoPath, err := repo.Path()
				require.NoError(t, err)

				gittest.Exec(t, cfg, "-C", repoPath,
					"-c", "commitGraph.generationVersion=1",
					"commit-graph", "write", "--reachable", "--split", "--changed-paths",
				)
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: featureflag.UseCommitGraphGenerationData.IsEnabled(ctx),
			},
		},
		{
			desc: "split commit-graph with bloom filter with generation data",
			setup: func(t *testing.T, repo *localrepo.Repo) {
				localrepo.WriteTestCommit(t, repo, localrepo.WithBranch("main"))

				repoPath, err := repo.Path()
				require.NoError(t, err)

				gittest.Exec(t, cfg, "-C", repoPath,
					"-c", "commitGraph.generationVersion=2",
					"commit-graph", "write", "--reachable", "--split", "--changed-paths",
				)
			},
			expectedConfig: WriteCommitGraphConfig{
				ReplaceChain: false,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
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
