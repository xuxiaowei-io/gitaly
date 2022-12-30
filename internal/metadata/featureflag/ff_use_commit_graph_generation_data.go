package featureflag

// UseCommitGraphGenerationData enables reading and writing commit-graph generation data in by
// setting `commitGraph.generationVersion=2`.
var UseCommitGraphGenerationData = NewFeatureFlag(
	"use_commit_graph_generation_data",
	"v15.8.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4704",
	false,
)
