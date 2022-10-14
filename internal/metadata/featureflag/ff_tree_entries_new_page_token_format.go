package featureflag

// TreeEntriesNewPageTokenFormat enables support for new page token format in TreeEntries RPC
var TreeEntriesNewPageTokenFormat = NewFeatureFlag(
	"tree_entries_new_page_token_format",
	"v15.5.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4542",
	false,
)
