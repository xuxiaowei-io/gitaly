package featureflag

// ReplicateRepositoryObjectPool will enable replication of a repository's object pool through the
// `ReplicateRepository` RPC. This will help to preserve object deduplication between repositories
// that share an object pool.
var ReplicateRepositoryObjectPool = NewFeatureFlag(
	"replicate_repository_object_pool",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5088",
	false,
)
