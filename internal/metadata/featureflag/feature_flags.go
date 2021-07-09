package featureflag

// A set of feature flags used in Gitaly and Praefect.
// In order to support coverage of combined features usage all feature flags should be marked as enabled for the test.
// NOTE: if you add a new feature flag please add it to the `All` list defined below.
var (
	// GoUpdateRemoteMirror enables the Go implementation of UpdateRemoteMirror
	GoUpdateRemoteMirror = FeatureFlag{Name: "go_update_remote_mirror", OnByDefault: true}
	// FetchInternalRemoteErrors makes FetchInternalRemote return actual errors instead of a boolean
	FetchInternalRemoteErrors = FeatureFlag{Name: "fetch_internal_remote_errors", OnByDefault: false}
	// LFSPointersPipeline enables the alternative pipeline implementation of LFS-pointer
	// related RPCs.
	LFSPointersPipeline = FeatureFlag{Name: "lfs_pointers_pipeline", OnByDefault: false}
	// GoSetConfig enables git2go implementation of SetConfig.
	GoSetConfig = FeatureFlag{Name: "go_set_config", OnByDefault: false}
	// CreateRepositoryFromBundleAtomicFetch will add the `--atomic` flag to git-fetch(1) in
	// order to reduce the number of transactional votes.
	CreateRepositoryFromBundleAtomicFetch = FeatureFlag{Name: "create_repository_from_bundle_atomic_fetch", OnByDefault: false}
	ResolveConflictsWithHooks             = FeatureFlag{Name: "resolve_conflicts_with_hooks", OnByDefault: false}
	// ReplicateRepositoryDirectFetch will cause the ReplicateRepository RPC to perform fetches
	// via a direct call instead of doing an RPC call to its own server. This fixes calls of
	// `ReplicateRepository()` in case it's invoked via Praefect with transactions enabled.
	ReplicateRepositoryDirectFetch = FeatureFlag{Name: "replicate_repository_direct_fetch", OnByDefault: false}
	// FindAllTagsPipeline enables the alternative pipeline implementation for finding
	// tags via FindAllTags.
	FindAllTagsPipeline = FeatureFlag{Name: "find_all_tags_pipeline", OnByDefault: false}
	// TxRemoveRepository enables transactionsal voting for the RemoveRepository RPC.
	TxRemoveRepository = FeatureFlag{Name: "tx_remove_repository", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	GoUpdateRemoteMirror,
	FetchInternalRemoteErrors,
	LFSPointersPipeline,
	GoSetConfig,
	CreateRepositoryFromBundleAtomicFetch,
	ResolveConflictsWithHooks,
	ReplicateRepositoryDirectFetch,
	FindAllTagsPipeline,
	TxRemoveRepository,
}
