package featureflag

// GeometricRepacking will start to use geometric repacking of objects in the context of repository
// maintenance.
var GeometricRepacking = NewFeatureFlag(
	"geometric_repacking",
	"v15.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5031",
	true,
)
