package featureflag

// GitV241 enables the use of Git v2.41.
var GitV241 = NewFeatureFlag(
	"git_v241",
	"v16.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5336",
	false,
)
