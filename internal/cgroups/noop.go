package cgroups

import (
	"io"
	"os/exec"

	"github.com/prometheus/client_golang/prometheus"
)

// NoopManager is a cgroups manager that does nothing
type NoopManager struct{}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Ready() bool {
	return false
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Stats() (Stats, error) {
	return Stats{}, nil
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) Setup() error {
	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (cg *NoopManager) AddCommand(*exec.Cmd, ...AddCommandOption) (string, error) {
	return "", nil
}

// SupportsCloneIntoCgroup returns false.
func (cg *NoopManager) SupportsCloneIntoCgroup() bool {
	return false
}

// CloneIntoCgroup does nothing.
func (cg *NoopManager) CloneIntoCgroup(*exec.Cmd, ...AddCommandOption) (string, io.Closer, error) {
	return "", io.NopCloser(nil), nil
}

// Describe does nothing
func (cg *NoopManager) Describe(ch chan<- *prometheus.Desc) {}

// Collect does nothing
func (cg *NoopManager) Collect(ch chan<- prometheus.Metric) {}
