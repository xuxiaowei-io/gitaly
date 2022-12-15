package featureflag

// AtomicCreateObjectPool will use atomic semantics in the CreateObjectPool RPC call. The end result
// should be that the object pool either exists or doesn't exist, but no in-between state.
var AtomicCreateObjectPool = NewFeatureFlag(
	"atomic_create_object_pool",
	"v15.7.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4676",
	false,
)
