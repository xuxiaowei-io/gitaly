package featureflag

// InterceptReplicateRepository will enable the interception of the ReplicateRepository RPC in
// Praefect. Interception of this RPC enables Praefect to support object pool replication.
var InterceptReplicateRepository = NewFeatureFlag(
	"intercept_replicate_repository",
	"v16.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5477",
	true,
)
