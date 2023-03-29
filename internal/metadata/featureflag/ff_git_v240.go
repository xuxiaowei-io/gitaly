package featureflag

// GitV240 enables the use of Git v2.40.
var GitV240 = NewFeatureFlag(
	"git_v240",
	"v15.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5030",
	false,
)
