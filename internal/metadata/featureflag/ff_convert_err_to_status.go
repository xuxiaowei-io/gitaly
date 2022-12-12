package featureflag

// ConvertErrToStatus turns on error translation into gRPC status.
var ConvertErrToStatus = NewFeatureFlag(
	"convert_err_to_status",
	"v15.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4603",
	true,
)
