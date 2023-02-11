package service

import (
	"strings"

	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
)

// ValidateRepository checks where the Repository is provided and
// all the required fields are set.
func ValidateRepository(repository *gitalypb.Repository) error {
	if repository == nil {
		return gitalyerrors.ErrEmptyRepository
	}
	if strings.TrimSpace(repository.GetStorageName()) == "" {
		return gitalyerrors.ErrEmptyStorageName
	}
	if strings.TrimSpace(repository.GetRelativePath()) == "" {
		return gitalyerrors.ErrEmptyRelativePath
	}
	return nil
}
