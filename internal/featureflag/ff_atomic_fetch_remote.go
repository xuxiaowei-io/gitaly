package featureflag

// AtomicFetchRemote enables atomic transactions for the `FetchRemote` RPC.
var AtomicFetchRemote = NewFeatureFlag(
	"atomic_fetch_remote",
	"v16.5.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5640",
	true,
)
