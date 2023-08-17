package featureflag

// CherryPickPureGit will enable the UserCherryPick RPC to use
// git-merge-tree instead of git2go
var CherryPickPureGit = NewFeatureFlag(
	"cherry_pick_pure_git",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5421",
	false,
)
