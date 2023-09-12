package featureflag

// TransactionalLinkRepository enables transactions to be used when linking a repository to an
// object pool via the `LinkRepositoryToObjectPool` RPC.
var TransactionalLinkRepository = NewFeatureFlag(
	"transactional_link_repository",
	"v16.4.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5587",
	false,
)
