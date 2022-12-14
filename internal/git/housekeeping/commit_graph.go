package housekeeping

import (
	"bytes"
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
)

// WriteCommitGraphConfig contains configuration that can be passed to WriteCommitGraph to alter its
// default behaviour.
type WriteCommitGraphConfig struct {
	// ReplaceChain causes WriteCommitGraph to rewrite the complete commit-graph chain. This is
	// a lot more expensive than the default, incremental update of the commit-graph chains but
	// may be required in certain cases to fix up commit-graphs.
	ReplaceChain bool
}

// WriteCommitGraphConfigForRepository returns the optimal default-configuration for the given repo.
// By default, the configuration will ask for an incremental commit-graph update. If the preexisting
// commit-graph is missing bloom filters though then the whole commit-graph chain will be rewritten.
func WriteCommitGraphConfigForRepository(ctx context.Context, repo *localrepo.Repo) (WriteCommitGraphConfig, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return WriteCommitGraphConfig{}, err
	}

	var replaceChain bool

	commitGraphInfo, err := stats.CommitGraphInfoForRepository(repoPath)
	if err != nil {
		return WriteCommitGraphConfig{}, helper.ErrInternalf("getting commit-graph info: %w", err)
	}

	if commitGraphInfo.CommitGraphChainLength == 0 {
		// The repository does not have a commit-graph chain. This either indicates we ain't
		// got no commit-graph at all, or that it's monolithic. In both cases we want to
		// replace the commit-graph chain.
		replaceChain = true
	} else if !commitGraphInfo.HasBloomFilters {
		// If the commit-graph-chain exists, we want to rewrite it in case we see that it
		// ain't got bloom filters enabled. This is because Git will refuse to write any
		// bloom filters as long as any of the commit-graph slices is missing this info.
		replaceChain = true
	}

	return WriteCommitGraphConfig{
		ReplaceChain: replaceChain,
	}, nil
}

// WriteCommitGraph updates the commit-graph in the given repository. The commit-graph is updated
// incrementally, except in the case where it doesn't exist yet or in case it is detected that the
// commit-graph is missing bloom filters.
func WriteCommitGraph(ctx context.Context, repo *localrepo.Repo, cfg WriteCommitGraphConfig) error {
	flags := []git.Option{
		git.Flag{Name: "--reachable"},
		git.Flag{Name: "--changed-paths"}, // enables Bloom filters
		git.ValueFlag{Name: "--size-multiple", Value: "4"},
	}

	if cfg.ReplaceChain {
		flags = append(flags, git.Flag{Name: "--split=replace"})
	} else {
		flags = append(flags, git.Flag{Name: "--split"})
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name:   "commit-graph",
		Action: "write",
		Flags:  flags,
	}, git.WithStderr(&stderr)); err != nil {
		return helper.ErrInternalf("writing commit-graph: %w: %v", err, stderr.String())
	}

	return nil
}
