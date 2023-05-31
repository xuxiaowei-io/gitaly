package featureflag

// ListConflictFilesMergeTree enables the usage of git-merge-tree(1) for
// the ListConflictFiles RPC.
var ListConflictFilesMergeTree = NewFeatureFlag(
	"list_conflict_files_merge_tree",
	"v16.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5098",
	false,
)
