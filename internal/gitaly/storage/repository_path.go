package storage

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// praefectPoolPathPrefix is the prefix directory where Praefect places object pools.
const praefectPoolPathPrefix = "@cluster/pools/"

// IsPraefectPoolRepository returns whether the repository is a Praefect generated object pool repository.
func IsPraefectPoolRepository(repo Repository) bool {
	return strings.HasPrefix(repo.GetRelativePath(), praefectPoolPathPrefix)
}

// DeriveReplicaPath derives a repository's disk storage path from its repository ID. The repository ID
// is hashed with SHA256 and the first four hex digits of the hash are used as the two subdirectories to
// ensure even distribution into subdirectories. The format is @cluster/repositories/ab/cd/<repository-id>.
func DeriveReplicaPath(repositoryID int64) string {
	return deriveDiskPath("@cluster/repositories", repositoryID)
}

// DerivePoolPath derives an object pools's disk storage path from its repository ID. The repository ID
// is hashed with SHA256 and the first four hex digits of the hash are used as the two subdirectories to
// ensure even distribution into subdirectories. The format is @cluster/pools/ab/cd/<repository-id>. The pools
// have a different directory prefix from other repositories so Gitaly can identify them in OptimizeRepository
// and avoid pruning them.
func DerivePoolPath(repositoryID int64) string {
	return deriveDiskPath(praefectPoolPathPrefix, repositoryID)
}

func deriveDiskPath(prefixDir string, repositoryID int64) string {
	hasher := sha256.New()
	// String representation of the ID is used to make it easier to derive the replica paths with
	// external tools. The error is ignored as the hash.Hash interface is documented to never return
	// an error.
	hasher.Write([]byte(strconv.FormatInt(repositoryID, 10)))
	hash := hasher.Sum(nil)
	return filepath.Join(prefixDir, fmt.Sprintf("%x/%x/%d", hash[0:1], hash[1:2], repositoryID))
}
