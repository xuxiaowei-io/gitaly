package featureflag

// CommandCloseStdout causes the `command` package to close stdout of commands when calling `Wait()` on them instead of
// discarding their output. This has two consequences:
//
//   - We notice when a command's output has not been fully read when calling `Wait()`. Previously, such a call would
//     have succeeded.
//
//   - We notice when a command has been killed when trying to consume its output. Previously, fully consuming the output
//     would succeed, which can lead to partially-read output without indication of an error.
//
// So overall, the new behaviour is more thorough and will cause us to detect errors in stdout handling more readily.
var CommandCloseStdout = NewFeatureFlag(
	"command_close_stdout",
	"v16.4.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5558",
	false,
)
