package gittest

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// DefaultObjectHash is the default object hash used for running tests.
var DefaultObjectHash = func() git.ObjectHash {
	if _, enabled := os.LookupEnv("GITALY_TEST_WITH_SHA256"); enabled {
		return git.ObjectHashSHA256
	}

	return git.ObjectHashSHA1
}()

// SkipWithSHA256 skips the test in case the default object hash is SHA256.
func SkipWithSHA256(tb testing.TB) {
	if DefaultObjectHash.Format == "sha256" {
		tb.Skip("test is not compatible with SHA256")
	}
}
