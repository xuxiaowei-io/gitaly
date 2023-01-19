package featureflag

// UserCommitFilesStructuredErrors will enable the use of structured errors in the UserCommitFiles
// RPC.
var UserCommitFilesStructuredErrors = NewFeatureFlag(
	"user_commit_files_structured_errors",
	"v15.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4472",
	true,
)
