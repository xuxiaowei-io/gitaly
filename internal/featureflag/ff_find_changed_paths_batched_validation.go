package featureflag

// FindChangedPathsBatchedValidation enables the use of batched validation in
// the FindChangedPaths RPC
var FindChangedPathsBatchedValidation = NewFeatureFlag(
	"find_changed_paths_batched_validation",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5410",
	false,
)
