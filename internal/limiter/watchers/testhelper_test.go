package watchers

import (
	"os/exec"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
)

type testCgroupManager struct {
	ready      bool
	statsErr   error
	statsList  []cgroups.Stats
	statsIndex int
}

func (m *testCgroupManager) Ready() bool { return m.ready }
func (m *testCgroupManager) Stats() (cgroups.Stats, error) {
	m.statsIndex++
	return m.statsList[m.statsIndex-1], m.statsErr
}
func (m *testCgroupManager) Setup() error                        { return nil }
func (m *testCgroupManager) Cleanup() error                      { return nil }
func (m *testCgroupManager) Describe(ch chan<- *prometheus.Desc) {}
func (m *testCgroupManager) Collect(ch chan<- prometheus.Metric) {}
func (m *testCgroupManager) AddCommand(*exec.Cmd, ...cgroups.AddCommandOption) (string, error) {
	return "", nil
}
