package featureflag

// SynchronizeHookExecutions synchronizes execution of Git hooks across all Gitaly nodes taking part in a transaction
// in Gitaly Cluster. This is done to avoid lock contention around the `packed-refs` file.
var SynchronizeHookExecutions = NewFeatureFlag(
	"synchronize_hook_executions",
	"v16.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5359",
	true,
)
