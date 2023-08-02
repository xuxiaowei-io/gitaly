package featureflag

// AdaptiveLimitPackObjects enables adaptive concurrency limit for pack-objects
var AdaptiveLimitPackObjects = NewFeatureFlag(
	"adaptive_limit_pack_objects",
	"v16.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5379",
	false,
)
