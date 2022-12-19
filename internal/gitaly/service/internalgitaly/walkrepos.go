package internalgitaly

import (
	"context"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *server) WalkRepos(req *gitalypb.WalkReposRequest, stream gitalypb.InternalGitaly_WalkReposServer) error {
	sPath, err := s.storagePath(req.GetStorageName())
	if err != nil {
		return err
	}

	if err := walkStorage(stream.Context(), sPath, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) storagePath(storageName string) (string, error) {
	for _, storage := range s.storages {
		if storage.Name == storageName {
			return storage.Path, nil
		}
	}
	return "", structerr.NewNotFound("storage name %q not found", storageName)
}

func walkStorage(ctx context.Context, storagePath string, stream gitalypb.InternalGitaly_WalkReposServer) error {
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

		if storage.IsGitDirectory(path) {
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
