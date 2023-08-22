//go:build linux

package cgroups

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestNewManager(t *testing.T) {
	require.IsType(t, &NoopManager{}, NewManager(cgroups.Config{}, testhelper.SharedLogger(t), 1))
}
