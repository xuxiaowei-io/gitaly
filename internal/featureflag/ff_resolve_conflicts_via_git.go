package featureflag

// ResolveConflictsViaGit enables the usage of git-merge-tree(1) for
// the ResolveConflicts RPC.
var ResolveConflictsViaGit = NewFeatureFlag(
	"resolve_conflicts_via_git",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5395",
	false,
)
