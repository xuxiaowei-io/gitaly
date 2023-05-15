package featureflag

// LocalrepoReadObjectCached enables the use of the catfile cache for
// localrepo.ReadObject
var LocalrepoReadObjectCached = NewFeatureFlag(
	"localrepo_read_object_cached",
	"v15.7",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4662",
	false,
)
