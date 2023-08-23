package featureflag

// TransactionalAlternatesDisconnect enables transactions to be used when disconnecting an alternate
// object database from a Git repository. If the transaction fails, the alternate disconnect is
// rolled back.
var TransactionalAlternatesDisconnect = NewFeatureFlag(
	"transactional_alternates_disconnect",
	"v16.4.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5540",
	false,
)
