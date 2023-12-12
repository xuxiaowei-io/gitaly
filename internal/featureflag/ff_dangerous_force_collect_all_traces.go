package featureflag

// DangerousForceCollectAllTraces enables the collection of distribtued traces via the git trace 2 API
var DangerousForceCollectAllTraces = NewFeatureFlag(
	"dangerous_force_collect_all_traces",
	"v16.7.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5700",
	false,
)
