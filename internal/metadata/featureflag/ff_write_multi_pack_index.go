package featureflag

// WriteMultiPackIndex determines whether Gitaly should write multi-pack-indices in
// OptimizeRepository.
var WriteMultiPackIndex = NewFeatureFlag(
	"write_multi_pack_index",
	"v15.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4741",
	true,
)
