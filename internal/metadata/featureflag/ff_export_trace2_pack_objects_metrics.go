package featureflag

// ExportTrace2PackObjectsMetrics allows Gitaly enables trace2 and export internal Git metrics
// of pack-objects commands
var ExportTrace2PackObjectsMetrics = NewFeatureFlag(
	"export_trace2_pack_objects_metrics",
	"v15.11.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4997",
	false,
)
