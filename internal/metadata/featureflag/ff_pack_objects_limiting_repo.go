package featureflag

// PackObjectsLimitingRepo will enable a concurrency limiter for pack objects
// based off of the repository.
var PackObjectsLimitingRepo = NewFeatureFlag(
	"pack_objects_limiting_repo",
	"v15.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4413",
	false,
)
