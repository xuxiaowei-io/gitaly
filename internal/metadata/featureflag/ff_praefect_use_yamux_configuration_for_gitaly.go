package featureflag

// PraefectUseYamuxConfigurationForGitaly switches the RPCs to be proxied over
// connections that are set up with custom Yamux configuration.
var PraefectUseYamuxConfigurationForGitaly = NewFeatureFlag(
	"praefect_use_yamux_configuration_for_gitaly",
	"v15.7.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4644",
	false,
)
