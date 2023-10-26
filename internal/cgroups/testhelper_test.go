//go:build linux

package cgroups

import (
	"hash/crc32"
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// calcGroupID calculates the repository cgroup ID for the key provided.
func calcGroupID(key []string, ct uint) uint {
	checksum := crc32.ChecksumIEEE([]byte(strings.Join(key, "/")))
	return uint(checksum) % ct
}
