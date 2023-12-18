package featureflag

// GitV243 enables the use of Git version 2.43.
var GitV243 = NewFeatureFlag(
	"git_v243",
	"v16.7.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5739",
	false,
)
