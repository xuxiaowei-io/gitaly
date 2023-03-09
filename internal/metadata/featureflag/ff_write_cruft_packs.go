package featureflag

// WriteCruftPacks enables writing cruft packs during repository housekeeping.
var WriteCruftPacks = NewFeatureFlag(
	"write_cruft_packs",
	"v15.10.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4829",
	false,
)
