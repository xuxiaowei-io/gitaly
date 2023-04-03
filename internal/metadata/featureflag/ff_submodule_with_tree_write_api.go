package featureflag

// SubmoduleWithTreeAPI will enable the UserUpdateSubmodule RPC to use the
// localrepo packge's TreeEntry APIs to modify an existing entry.
var SubmoduleWithTreeAPI = NewFeatureFlag(
	"submodule_with_tree_api",
	"v15.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5040",
	false,
)
