package featureflag

// MergeTreeMerge enables implementation of UserMergeBranch using
// `git merge-tree` instead of s.git2goExecutor.Merge()
var MergeTreeMerge = NewFeatureFlag(
	"merge_tree_merge",
	"15.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4600",
	false,
)
