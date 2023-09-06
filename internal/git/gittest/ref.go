package gittest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

// WriteRef writes a reference into the repository pointing to the given object ID.
func WriteRef(tb testing.TB, cfg config.Cfg, repoPath string, ref git.ReferenceName, oid git.ObjectID) {
	Exec(tb, cfg, "-C", repoPath, "update-ref", ref.String(), oid.String())
}

// ResolveRevision resolves the revision to an object ID.
func ResolveRevision(tb testing.TB, cfg config.Cfg, repoPath string, revision string) git.ObjectID {
	tb.Helper()
	output := Exec(tb, cfg, "-C", repoPath, "rev-parse", "--verify", revision)
	objectID, err := DefaultObjectHash.FromHex(text.ChompBytes(output))
	require.NoError(tb, err)
	return objectID
}

// GetReferencesConfig is an alias of git.ReferencesConfig and can be passed to GetReferences to influence its default
// behaviour.
type GetReferencesConfig = git.GetReferencesConfig

// GetReferences reads references in the Git repository.
func GetReferences(tb testing.TB, cfg config.Cfg, repoPath string, optionalCfg ...GetReferencesConfig) []git.Reference {
	require.Less(tb, len(optionalCfg), 2, "you must either pass no or exactly one configuration")

	var refCfg GetReferencesConfig
	if len(optionalCfg) == 1 {
		refCfg = optionalCfg[0]
	}

	refs, err := git.GetReferences(
		testhelper.Context(tb),
		NewRepositoryPathExecutor(tb, cfg, repoPath),
		refCfg,
	)
	require.NoError(tb, err)

	return refs
}

// GetSymbolicRef reads symbolic references in the Git repository.
func GetSymbolicRef(tb testing.TB, cfg config.Cfg, repoPath string, refname git.ReferenceName) git.Reference {
	symref, err := git.GetSymbolicRef(
		testhelper.Context(tb),
		NewRepositoryPathExecutor(tb, cfg, repoPath),
		refname,
	)
	require.NoError(tb, err)

	return symref
}
