package featureflag

// AlwaysLogFeatureFlags flag enables feature_flags field attachment in all gRPC logs. By default,
// the flag is off, feature_flag field is attached in gRPC failure logs only (grpc.status != ok).
// This flag could be a long-running flag. Each environment may enable/disable this flag accordingly
// to the log volume.
var AlwaysLogFeatureFlags = NewFeatureFlag(
	"always_log_feature_flags",
	"v15.7.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4627",
	false,
)
