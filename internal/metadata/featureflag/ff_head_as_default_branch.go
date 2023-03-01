package featureflag

// HeadAsDefaultBranch enables using HEAD as the only source of truth for the
// default branch.
var HeadAsDefaultBranch = NewFeatureFlag(
	"head_as_default_branch",
	"v15.10.0",
	"",
	false,
)
