package service

import (
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// ValidateRepository checks where the Repository is provided and
// all the required fields are set.
func ValidateRepository(repository *gitalypb.Repository) error {
	if repository == nil {
		return storage.ErrRepositoryNotSet
	}
	if strings.TrimSpace(repository.GetStorageName()) == "" {
		return storage.ErrStorageNotSet
	}
	if strings.TrimSpace(repository.GetRelativePath()) == "" {
		return storage.ErrRepositoryPathNotSet
	}
	return nil
}
