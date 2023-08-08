package featureflag

// LowerBigFileThreshold lowers the `core.bigFileThreshold` from 512MB to 50MB. This has two consequences:
//
//   - Files larger than 50MB are not going to be slurped into memory anymore, but will instead use streaming interfaces.
//     This should improve memory consumption.
//
//   - Files larger than 50MB will not be diffed anymore. This should improve latency for RPCs that compute diffs when any
//     such large files are involved. The downside is that diffs for such files cannot be viewed anymore.
var LowerBigFileThreshold = NewFeatureFlag(
	"lower_big_file_threshold",
	"v16.3.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5496",
	false,
)
