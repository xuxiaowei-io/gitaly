package featureflag

// ReplicateRepositoryHooks will enable replication of custom hooks with the
// `ReplicateRepository` RPC.
var ReplicateRepositoryHooks = NewFeatureFlag(
	"replicate_repository_hooks",
	"v15.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4772",
	false,
)
