package featureflag

// PackObjectsLimitingUser will enable a concurrency limiter for pack objects
// based off of the user
var PackObjectsLimitingUser = NewFeatureFlag(
	"pack_objects_limiting_user",
	"v15.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4413",
	false,
)
