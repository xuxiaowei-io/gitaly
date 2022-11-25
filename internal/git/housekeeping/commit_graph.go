package housekeeping

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

	// Check whether the commit-graph-chain exists. If it doesn't, the repository either has no
	// commit-graph at all, or it has a monolithic commit-graph. In both cases we'd need to
	// rewrite the complete commit-graph, so it's not worth checking whether the monolithic
	// commit-graph exists.
	if _, err := os.Stat(filepath.Join(repoPath, stats.CommitGraphChainRelPath)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return WriteCommitGraphConfig{}, helper.ErrInternal(fmt.Errorf("statting commit-graph-chain: %w", err))
		}

		// The commit-graph-chain doesn't exist, so we want to rewrite any potentially
		// existing monolithic commit-graph.
		replaceChain = true
	} else {
		// If the commit-graph-chain exists, we want to rewrite it in case we see that it
		// ain't got bloom filters enabled. This is because Git will refuse to write any
		// bloom filters as long as any of the commit-graph slices is missing this info.
		missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
		if err != nil {
			return WriteCommitGraphConfig{}, helper.ErrInternal(fmt.Errorf("checking for missing bloom-filters: %w", err))
		}

		replaceChain = missingBloomFilters
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
	if err := repo.ExecAndWait(ctx, git.SubSubCmd{
		Name:   "commit-graph",
		Action: "write",
		Flags:  flags,
	}, git.WithStderr(&stderr)); err != nil {
		return helper.ErrInternalf("writing commit-graph: %w: %v", err, stderr.String())
	}

	return nil
}
