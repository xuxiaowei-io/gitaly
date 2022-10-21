package featureflag

// GitV238 will enable the use of Git v2.38
var GitV238 = NewFeatureFlag(
	"git_v238",
	"v15.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4553",
	false,
)
