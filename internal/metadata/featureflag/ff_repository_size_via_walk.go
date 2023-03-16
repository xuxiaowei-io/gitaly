package featureflag

// RepositorySizeViaWalk starts to compute the repository size via `filepath.WalkDir()` instead of
// using du(1).
var RepositorySizeViaWalk = NewFeatureFlag(
	"repository_size_via_walk",
	"15.10.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4976",
	false,
)
