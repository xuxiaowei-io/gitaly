package featureflag

// FetchSourceBranchQuarantined enables use of quarantined repository for
// `FetchSourceBranch`.
var FetchSourceBranchQuarantined = NewFeatureFlag(
	"fetch_source_branch_quarantined",
	"v15.8.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4698",
	true,
)
