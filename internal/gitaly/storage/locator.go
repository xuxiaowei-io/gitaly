package storage

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RepositoryNotFoundError is returned when attempting to operate on a repository that does not
// exist in the virtual storage.
type RepositoryNotFoundError struct {
	virtualStorage string
	relativePath   string
}

// NewRepositoryNotFoundError returns a new repository not found error for the given repository.
func NewRepositoryNotFoundError(virtualStorage string, relativePath string) error {
	return RepositoryNotFoundError{virtualStorage: virtualStorage, relativePath: relativePath}
}

// Error returns the error message.
func (err RepositoryNotFoundError) Error() string {
	return fmt.Sprintf("repository %q/%q not found", err.virtualStorage, err.relativePath)
}

var (
	// ErrRepositoryNotFound is returned when operating on a repository that doesn't exist.
	//
	// This somewhat duplicates the above RepositoryNotFoundError but doesn't specify which
	// repository was not found. With repository IDs in use, the virtual storage and relative
	// path won't be available everywhere anymore.
	ErrRepositoryNotFound = errors.New("repository not found")

	// ErrRepositoryAlreadyExists is returned when attempting to create a repository that
	// already exists.
	ErrRepositoryAlreadyExists = errors.New("repository already exists")

	// ErrRepositoryNotValid is returned when operating on a path that is not a valid Git
	// repository.
	ErrRepositoryNotValid = errors.New("repository not valid")

	// ErrRelativePathEscapesRoot is returned when a relative path is passed that escapes the
	// storage's root directory.
	ErrRelativePathEscapesRoot = errors.New("relative path escapes root directory")
)

// Repository represents a storage-scoped repository.
type Repository interface {
	GetStorageName() string
	GetRelativePath() string
	GetGitObjectDirectory() string
	GetGitAlternateObjectDirectories() []string
}

// RepoPathEqual compares if two repositories are in the same location
func RepoPathEqual(a, b Repository) bool {
	return a.GetStorageName() == b.GetStorageName() &&
		a.GetRelativePath() == b.GetRelativePath()
}

// Locator allows to get info about location of the repository or storage at the local file system.
type Locator interface {
	// ValidateRepository validates whether the given repository is a valid Git repository.
	ValidateRepository(Repository) error
	// GetRepoPath returns the full path of the repository referenced by an RPC Repository message.
	// By default, it verifies that the path is an existing git directory. However, if invoked with
	// the `GetRepoPathOption` produced by `WithRepositoryVerificationSkipped()`, this validation
	// will be skipped. The errors returned are gRPC errors with relevant error codes and should be
	// passed back to gRPC without further decoration.
	GetRepoPath(repo Repository, opts ...GetRepoPathOption) (string, error)
	// GetStorageByName will return the path for the storage, which is fetched by
	// its key. An error is return if it cannot be found.
	GetStorageByName(storageName string) (string, error)

	// CacheDir returns the path to the cache dir for a storage.
	CacheDir(storageName string) (string, error)
	// TempDir returns the path to the temp dir for a storage.
	TempDir(storageName string) (string, error)
	// StateDir returns the path to the state dir for a storage.
	StateDir(storageName string) (string, error)
}

// GetRepoPathConfig is used to configure GetRepoPath.
type GetRepoPathConfig struct {
	// SkipRepositoryVerification will cause GetRepoPath to skip verification the verification whether the
	// computed path is an actual Git repository or not.
	SkipRepositoryVerification bool
}

// GetRepoPathOption can be passed to GetRepoPath to change its default behavior.
type GetRepoPathOption func(*GetRepoPathConfig)

// WithRepositoryVerificationSkipped skips the repository path validation that ensures the Git
// directory is valid.
func WithRepositoryVerificationSkipped() GetRepoPathOption {
	return func(cfg *GetRepoPathConfig) {
		cfg.SkipRepositoryVerification = true
	}
}

// ValidateRelativePath validates a relative path by joining it with rootDir and verifying the result
// is either rootDir or a path within rootDir. Returns clean relative path from rootDir to relativePath
// or an ErrRelativePathEscapesRoot if the resulting path is not contained within rootDir.
func ValidateRelativePath(rootDir, relativePath string) (string, error) {
	absPath := filepath.Join(rootDir, relativePath)
	if rootDir != absPath && !strings.HasPrefix(absPath, rootDir+string(os.PathSeparator)) {
		return "", ErrRelativePathEscapesRoot
	}

	return filepath.Rel(rootDir, absPath)
}

// QuarantineDirectoryPrefix returns a prefix for use in the temporary directory. The prefix is
// based on the relative repository path and will stay stable for any given repository. This allows
// us to verify that a given quarantine object directory indeed belongs to the repository at hand.
// Ideally, this function would directly be located in the quarantine module, but this is not
// possible due to cyclic dependencies.
func QuarantineDirectoryPrefix(repo Repository) string {
	hash := [20]byte{}
	if repo != nil {
		hash = sha1.Sum([]byte(repo.GetRelativePath()))
	}
	return fmt.Sprintf("quarantine-%x-", hash[:8])
}
