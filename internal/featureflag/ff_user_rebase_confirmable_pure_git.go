package featureflag

// UserRebaseConfirmablePureGit will enable the UserRebaseConfirmable RPC to
// use a pure git implemented rebase instead of git2go.
var UserRebaseConfirmablePureGit = NewFeatureFlag(
	"user_rebase_confirmable_pure_git",
	"v16.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5493",
	false,
)
