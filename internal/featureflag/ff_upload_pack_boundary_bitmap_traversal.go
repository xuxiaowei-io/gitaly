package featureflag

// UploadPackBoundaryBitmapTraversal enables the new boundary bitmap traversal algorithm introduced in Git v2.42 (see
// https://github.blog/2023-08-21-highlights-from-git-2-42/#faster-object-traversals-with-bitmaps). This new algorithm
// should be faster to enumerate objects when having only sparse bitmap coverage.
var UploadPackBoundaryBitmapTraversal = NewFeatureFlag(
	"upload_pack_boundary_bitmap_traversal",
	"v16.5.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/5620",
	true,
)
