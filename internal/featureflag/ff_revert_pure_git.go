package featureflag

// RevertPureGit will enable the UserRevert RPC to use git-merge-tree
// instead of git2go
var RevertPureGit = NewFeatureFlag(
	"revert_pure_git",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5458",
	false,
)
