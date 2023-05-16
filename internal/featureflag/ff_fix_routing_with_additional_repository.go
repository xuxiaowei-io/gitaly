package featureflag

// FixRoutingWithAdditionalRepository fixes routing of repository-creating RPCs that have an
// additional repository set in their request that is to be rewritten. Previously, it could have
// happened that the new repository was routed to nodes that didn't have the additional repo. With
// the flag enabled, Praefect always routes repository-creating RPCs to the same node that the
// additional repository is assigned to.
var FixRoutingWithAdditionalRepository = NewFeatureFlag(
	"fix_routing_with_additional_repository",
	"v16.0.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5134",
	false,
)
