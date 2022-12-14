package cgroups

import (
	"os/exec"

	"github.com/prometheus/client_golang/prometheus"
)

// NoopManager is a cgroups manager that does nothing
type NoopManager struct{}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Setup() error {
	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) AddCommand(*exec.Cmd, ...AddCommandOption) (string, error) {
	return "", nil
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Cleanup() error {
	return nil
}

// Describe does nothing
func (cg *NoopManager) Describe(ch chan<- *prometheus.Desc) {}

// Collect does nothing
func (cg *NoopManager) Collect(ch chan<- prometheus.Metric) {}
