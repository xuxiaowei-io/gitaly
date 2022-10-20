//go:build !gitaly_test_sha256

package housekeeping

// mockOptimizationStrategy is a mock strategy that can be used with OptimizeRepository.
type mockOptimizationStrategy struct{}
