package featureflag

// SubmoduleInGit will enable the use of Git v2.38
var SubmoduleInGit = NewFeatureFlag(
	"submodule_in_git",
	"v15.10.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4801",
	false,
)
