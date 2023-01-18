package featureflag

// WriteBitmapLookupTable enables writing of the lookup table extension in bitmaps. When enabled,
// the lookup table is used to defer loading individual bitmaps. This can be especially beneficial
// in large repositories that have large bitmap indexes.
//
// This feature has been introduced with Git v2.38.0.
var WriteBitmapLookupTable = NewFeatureFlag(
	"write_bitmap_lookup_table",
	"v15.9.0",
	"https://gitlab.com/gitlab-org/gitaly/-/issues/4744",
	false,
)
