package featureflag

// ExportTrace2Tracing allows Gitaly enables trace2 and export collected events as distributed
// tracing spans.
var ExportTrace2Tracing = NewFeatureFlag(
	"export_trace2_tracing",
	"v15.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4843",
	false,
)
