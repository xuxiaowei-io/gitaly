package internalgitaly

import (
	"context"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *server) WalkRepos(req *gitalypb.WalkReposRequest, stream gitalypb.InternalGitaly_WalkReposServer) error {
	if err := walkStorage(stream.Context(), s.locator, req.GetStorageName(), stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func walkStorage(
	ctx context.Context,
	locator storage.Locator,
	storageName string,
	stream gitalypb.InternalGitaly_WalkReposServer,
) error {
	storagePath, err := locator.GetStorageByName(storageName)
	if err != nil {
		return structerr.NewNotFound("looking up storage: %w", err)
	}

	return filepath.Walk(storagePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}

			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// keep walking
		}

		if locator.ValidateRepository(path) == nil {
			relPath, err := filepath.Rel(storagePath, path)
			if err != nil {
				return err
			}

			gitDirInfo, err := os.Stat(path)
			if err != nil {
				return err
			}

			if err := stream.Send(&gitalypb.WalkReposResponse{
				RelativePath:     relPath,
				ModificationTime: timestamppb.New(gitDirInfo.ModTime()),
			}); err != nil {
				return err
			}

			// once we know we are inside a git directory, we don't
			// want to continue walking inside since that is
			// resource intensive and unnecessary
			return filepath.SkipDir
		}

		return nil
	})
}
