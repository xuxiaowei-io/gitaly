package featureflag

// KillGitProcessesOnShutdown enables the use of Git v2.42.
var KillGitProcessesOnShutdown = NewFeatureFlag(
	"kill_git_processes_on_shutdown",
	"v16.4.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5547",
	false,
)
