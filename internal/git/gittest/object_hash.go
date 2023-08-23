package gittest

import "testing"

// SkipWithSHA256 skips the test in case the default object hash is SHA256.
func SkipWithSHA256(tb testing.TB) {
	if DefaultObjectHash.Format == "sha256" {
		tb.Skip("test is not compatible with SHA256")
	}
}
