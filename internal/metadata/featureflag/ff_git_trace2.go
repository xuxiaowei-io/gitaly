package featureflag

// GitTrace2 enables TRACE2 on git calls
var GitTrace2 = NewFeatureFlag(
	"git_trace2",
	"v15.5",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/TODO",
	false,
)
