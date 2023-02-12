package repository

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) RemoveAll(ctx context.Context, in *gitalypb.RemoveAllRequest) (*gitalypb.RemoveAllResponse, error) {
	storagePath, err := s.locator.GetStorageByName(in.StorageName)
	if err != nil {
		return nil, structerr.NewInternal("remove all: %w", err)
	}

	if err := removeAllInside(ctx, storagePath); err != nil {
		return nil, structerr.NewInternal("remove all: %w", err).WithMetadata("storage", in.StorageName)
	}

	return &gitalypb.RemoveAllResponse{}, nil
}

// removeAllInside tries to remove everything within path, avoiding permission
// problems associated with removing a user supplied directory.
func removeAllInside(ctx context.Context, path string) error {
	children, err := os.ReadDir(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return err
	}

	for _, child := range children {
		if err := os.RemoveAll(filepath.Join(path, child.Name())); err != nil {
			return err
		}
	}

	return nil
}
