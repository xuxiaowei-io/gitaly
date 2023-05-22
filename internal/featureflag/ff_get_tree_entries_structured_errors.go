package featureflag

// GetTreeEntriesStructuredErrors enables the usage structured errors for
// the GetTreeEntries RPC.
var GetTreeEntriesStructuredErrors = NewFeatureFlag(
	"get_tree_entries_structured_errors",
	"v16.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5157",
	false,
)
