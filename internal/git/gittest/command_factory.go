package gittest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

// NewCommandFactory creates a new Git command factory.
func NewCommandFactory(tb testing.TB, cfg config.Cfg, opts ...git.ExecCommandFactoryOption) *git.ExecCommandFactory {
	tb.Helper()
	factory, cleanup, err := git.NewExecCommandFactory(cfg, testhelper.SharedLogger(tb), opts...)
	require.NoError(tb, err)
	tb.Cleanup(cleanup)
	return factory
}
