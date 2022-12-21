package version

import (
	"fmt"
)

var version string

// GetVersionString returns a standard version header
func GetVersionString(binary string) string {
	return fmt.Sprintf("%s, version %v", binary, version)
}

// GetVersion returns the semver compatible version number
func GetVersion() string {
	return version
}
