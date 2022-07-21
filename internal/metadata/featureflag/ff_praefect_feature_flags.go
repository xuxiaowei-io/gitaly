package featureflag

// PraefectFeatureFlags will enable praefec to call the gitlab API for feature
// flags on replication jobs.
var PraefectFeatureFlags = NewFeatureFlag(
	"praefect_feature_flags",
	"v15.2.0",
	"",
	false)
