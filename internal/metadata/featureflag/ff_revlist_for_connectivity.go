package featureflag

// RevlistForConnectivity causes the connectivity check when removing alternates
// files to use git-rev-list instead of git-fsck
var RevlistForConnectivity = NewFeatureFlag(
	"revlist_for_connectivity",
	"v15.5.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4489",
	false,
)
