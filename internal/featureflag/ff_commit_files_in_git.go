package featureflag

// CommitFilesInGit enables implementation of UserCommitFiles using
// git plumbing commands instead of git2go
var CommitFilesInGit = NewFeatureFlag(
	"commit_files_in_git",
	"16.1.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5019",
	false,
)
