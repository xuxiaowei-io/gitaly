package featureflag

// GitV239 enables the use of Git v2.39.
var GitV239 = NewFeatureFlag(
	"git_v239",
	"v15.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4771",
	false,
)
