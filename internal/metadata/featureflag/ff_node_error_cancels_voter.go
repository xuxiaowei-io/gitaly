package featureflag

// NodeErrorCancelsVoter enables cancellation of the voter associated
// with a failed node RPC. By canceling voters that can no longer vote,
// the transaction can fail faster if quorum becomes impossible.
var NodeErrorCancelsVoter = NewFeatureFlag(
	"node_error_cancels_voter",
	"v15.6.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4552",
	false,
)
