package featureflag

// ObjectPoolDontInitOnFetch will cause FetchIntoObjectPool to not initialize the object pool when
// it does not yet exist.
var ObjectPoolDontInitOnFetch = NewFeatureFlag(
	"object_pool_dont_init_on_fetch",
	"v15.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4560",
	false,
)
