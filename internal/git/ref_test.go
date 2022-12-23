package git_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
)

func TestResolveRevision(t *testing.T) {
	cfg, repoProto, repoPath := git.Setup(t)

	commitID := localrepo.WriteTestCommit(
		t,
		localrepo.NewTestRepo(t, cfg, repoProto),
		localrepo.WithBranch(git.DefaultBranch),
	)

	require.Equal(t, commitID, git.ResolveRevision(t, cfg, repoPath, git.DefaultBranch))
}
