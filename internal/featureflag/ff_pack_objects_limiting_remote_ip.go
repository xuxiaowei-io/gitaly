package featureflag

// PackObjectsLimitingRemoteIP will enable a concurrency limiter for pack objects
// based off of the remote IP
var PackObjectsLimitingRemoteIP = NewFeatureFlag(
	"pack_objects_limiting_remote_ip",
	"v15.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4413",
	false,
)
