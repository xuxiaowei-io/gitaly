package featureflag

// MergeToRefWithGit will enable the UserMergeToRef RPC to use
// git-merge-tree instead of git2go
var MergeToRefWithGit = NewFeatureFlag(
	"merge_to_ref_with_git",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5401",
	false,
)
