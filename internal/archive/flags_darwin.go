package archive

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/darwin"
)

// MacOS operating system of version lower than 13 produces extra files while creating a tarball archive.
// Then details can be found in the answer https://apple.stackexchange.com/a/76077.
// The extra flag '--no-mac-metadata' needs to be added to the tar command to overcome it.
// We can drop this hack once moved to a newer version of MacOS used in our 'test:macos' CI job.
// Also, the function 'generateTarFile()' in internal/gitaly/service/repository/create_repository_from_snapshot_test.go
// can be simplified after version upgrade.
func extraFlags() []string {
	if darwin.MajorVersion() < 13 {
		return []string{"--no-mac-metadata"}
	}

	return nil
}
