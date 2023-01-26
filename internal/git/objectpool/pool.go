package objectpool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

var (
	// ErrInvalidPoolDir is returned when the object pool relative path is malformed.
	ErrInvalidPoolDir = errors.New("invalid object pool directory")

	// ErrInvalidPoolRepository indicates the directory the alternates file points to is not a valid git repository
	ErrInvalidPoolRepository = errors.New("object pool is not a valid git repository")

	// ErrAlternateObjectDirNotExist indicates a repository does not have an alternates file
	ErrAlternateObjectDirNotExist = errors.New("no alternates directory exists")
)

// ObjectPool are a way to de-dupe objects between repositories, where the objects
// live in a pool in a distinct repository which is used as an alternate object
// store for other repositories.
type ObjectPool struct {
	*localrepo.Repo

	gitCmdFactory       git.CommandFactory
	txManager           transaction.Manager
	housekeepingManager housekeeping.Manager
}

// FromProto returns an object pool object from its Protobuf representation. This function verifies
// that the object pool exists and is a valid pool repository.
func FromProto(
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	txManager transaction.Manager,
	housekeepingManager housekeeping.Manager,
	proto *gitalypb.ObjectPool,
) (*ObjectPool, error) {
	poolPath, err := locator.GetPath(proto.GetRepository())
	if err != nil {
		return nil, err
	}

	if !stats.IsPoolRepository(proto.GetRepository()) {
		// When creating repositories in the ObjectPool service we will first create the
		// repository in a temporary directory. So we need to check whether the path we see
		// here is in such a temporary directory and let it pass.
		tempDir, err := locator.TempDir(proto.GetRepository().GetStorageName())
		if err != nil {
			return nil, fmt.Errorf("getting temporary storage directory: %w", err)
		}

		if !strings.HasPrefix(poolPath, tempDir) {
			return nil, ErrInvalidPoolDir
		}
	}

	pool := &ObjectPool{
		Repo:                localrepo.New(locator, gitCmdFactory, catfileCache, proto.GetRepository()),
		gitCmdFactory:       gitCmdFactory,
		txManager:           txManager,
		housekeepingManager: housekeepingManager,
	}

	if !pool.IsValid() {
		return nil, ErrInvalidPoolRepository
	}

	return pool, nil
}

// ToProto returns a new struct that is the protobuf definition of the ObjectPool
func (o *ObjectPool) ToProto() *gitalypb.ObjectPool {
	return &gitalypb.ObjectPool{
		Repository: &gitalypb.Repository{
			StorageName:  o.GetStorageName(),
			RelativePath: o.GetRelativePath(),
		},
	}
}

// Exists will return true if the pool path exists and is a directory
func (o *ObjectPool) Exists() bool {
	path, err := o.Path()
	if err != nil {
		return false
	}

	fi, err := os.Stat(path)
	if os.IsNotExist(err) || err != nil {
		return false
	}

	return fi.IsDir()
}

// IsValid checks if a repository exists, and if its valid.
func (o *ObjectPool) IsValid() bool {
	path, err := o.Path()
	if err != nil {
		return false
	}

	return storage.IsGitDirectory(path)
}

// Remove will remove the pool, and all its contents without preparing and/or
// updating the repositories depending on this object pool
// Subdirectories will remain to exist, and will never be cleaned up, even when
// these are empty.
func (o *ObjectPool) Remove(ctx context.Context) (err error) {
	path, err := o.Path()
	if err != nil {
		return nil
	}

	return os.RemoveAll(path)
}

// FromRepo returns an instance of ObjectPool that the repository points to
func FromRepo(
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	txManager transaction.Manager,
	housekeepingManager housekeeping.Manager,
	repo *localrepo.Repo,
) (*ObjectPool, error) {
	dir, err := getAlternateObjectDir(repo)
	if err != nil {
		return nil, err
	}

	if dir == "" {
		return nil, nil
	}

	repoPath, err := repo.Path()
	if err != nil {
		return nil, err
	}

	altPathRelativeToStorage, err := objectPathRelativeToStorage(locator, repo.GetStorageName(), dir, repoPath)
	if err != nil {
		return nil, err
	}

	objectPoolProto := &gitalypb.ObjectPool{
		Repository: &gitalypb.Repository{
			StorageName:  repo.GetStorageName(),
			RelativePath: filepath.Dir(altPathRelativeToStorage),
		},
	}

	return FromProto(locator, gitCmdFactory, catfileCache, txManager, housekeepingManager, objectPoolProto)
}

// getAlternateObjectDir returns the entry in the objects/info/attributes file if it exists
// it will only return the first line of the file if there are multiple lines.
func getAlternateObjectDir(repo *localrepo.Repo) (string, error) {
	altPath, err := repo.InfoAlternatesPath()
	if err != nil {
		return "", err
	}

	if _, err = os.Stat(altPath); err != nil {
		if os.IsNotExist(err) {
			return "", ErrAlternateObjectDirNotExist
		}
		return "", err
	}

	altFile, err := os.Open(altPath)
	if err != nil {
		return "", err
	}
	defer altFile.Close()

	r := bufio.NewReader(altFile)
	b, err := r.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("reading alternates file: %v", err)
	}

	if err == nil {
		b = b[:len(b)-1]
	}

	if bytes.HasPrefix(b, []byte("#")) {
		return "", ErrAlternateObjectDirNotExist
	}

	return string(b), nil
}

// objectPathRelativeToStorage takes an object path that's relative to a repository's object directory
// and returns the path relative to the storage path of the repository.
func objectPathRelativeToStorage(locator storage.Locator, storageName, path, repoPath string) (string, error) {
	storagePath, err := locator.GetStorageByName(storageName)
	if err != nil {
		return "", err
	}
	objectDirPath := filepath.Join(repoPath, "objects")

	poolObjectDirFullPath := filepath.Join(objectDirPath, path)

	if !storage.IsGitDirectory(filepath.Dir(poolObjectDirFullPath)) {
		return "", ErrInvalidPoolRepository
	}

	poolRelPath, err := filepath.Rel(storagePath, poolObjectDirFullPath)
	if err != nil {
		return "", err
	}

	return poolRelPath, nil
}
