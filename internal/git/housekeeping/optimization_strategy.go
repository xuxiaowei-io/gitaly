package housekeeping

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
)

// OptimizationStrategy is an interface to determine which parts of a repository should be
// optimized.
type OptimizationStrategy interface{}

// HeuristicalOptimizationStrategy is an optimization strategy that is based on a set of
// heuristics.
type HeuristicalOptimizationStrategy struct{}

// NewHeuristicalOptimizationStrategy constructs a heuristicalOptimizationStrategy for the given
// repository. It derives all data from the repository so that the heuristics used by this
// repository can be decided without further disk reads.
func NewHeuristicalOptimizationStrategy(context.Context, *localrepo.Repo) (HeuristicalOptimizationStrategy, error) {
	return HeuristicalOptimizationStrategy{}, nil
}
