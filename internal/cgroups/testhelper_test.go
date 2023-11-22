//go:build linux

package cgroups

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

// cmdArgs are Command arguments used processes to be added to a cgroup.
var cmdArgs = []string{"ls", "-hal", "."}

func TestMain(m *testing.M) {
	testhelper.Run(m)
}
