package featureflag

// CatfileBatchCommand enables the `--batch-command` mode for git-cat-file(1).
var CatfileBatchCommand = NewFeatureFlag(
	"catfile_batch_command",
	"v16.2.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4573",
	false,
)
