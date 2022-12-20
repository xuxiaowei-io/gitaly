package git

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveRevision(t *testing.T) {
	cfg, _, repoPath := setup(t)

	commitID := WriteTestCommit(t, cfg, repoPath, WithBranch(DefaultBranch))

	require.Equal(t, commitID, ResolveRevision(t, cfg, repoPath, DefaultBranch))
}
