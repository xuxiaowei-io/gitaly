package featureflag

// SquashInGit enables the pure git implementation of UserSquash
var SquashInGit = NewFeatureFlag(
	"squash_in_git",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5424",
	false,
)
