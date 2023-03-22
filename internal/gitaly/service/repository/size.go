package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// RepositorySize returns the size of the specified repository in kibibytes. By default, this
// calculation is performed using the disk usage command.
func (s *server) RepositorySize(ctx context.Context, in *gitalypb.RepositorySizeRequest) (*gitalypb.RepositorySizeResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	repo := s.localrepo(repository)

	path, err := repo.Path()
	if err != nil {
		return nil, err
	}

	var sizeKiB int64
	if featureflag.RepositorySizeViaWalk.IsEnabled(ctx) {
		sizeKiB, err = dirSizeInKB(path)
		if err != nil {
			return nil, fmt.Errorf("calculating directory size: %w", err)
		}
	} else {
		sizeKiB = getPathSize(ctx, path)
	}

	return &gitalypb.RepositorySizeResponse{Size: sizeKiB}, nil
}

func (s *server) GetObjectDirectorySize(ctx context.Context, in *gitalypb.GetObjectDirectorySizeRequest) (*gitalypb.GetObjectDirectorySizeResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	repo := s.localrepo(repository)

	path, err := repo.ObjectDirectoryPath()
	if err != nil {
		return nil, err
	}

	var sizeKiB int64
	if featureflag.RepositorySizeViaWalk.IsEnabled(ctx) {
		sizeKiB, err = dirSizeInKB(path)
		if err != nil {
			return nil, fmt.Errorf("calculating directory size: %w", err)
		}
	} else {
		sizeKiB = getPathSize(ctx, path)
	}

	return &gitalypb.GetObjectDirectorySizeResponse{Size: sizeKiB}, nil
}

func getPathSize(ctx context.Context, path string) int64 {
	cmd, err := command.New(ctx, []string{"du", "-sk", path})
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Warn("ignoring du command error")
		return 0
	}

	sizeLine, err := io.ReadAll(cmd)
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Warn("ignoring command read error")
		return 0
	}

	if err := cmd.Wait(); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Warn("ignoring du wait error")
		return 0
	}

	sizeParts := bytes.Split(sizeLine, []byte("\t"))
	if len(sizeParts) != 2 {
		ctxlogrus.Extract(ctx).Warn(fmt.Sprintf("ignoring du malformed output: %q", sizeLine))
		return 0
	}

	size, err := strconv.ParseInt(string(sizeParts[0]), 10, 0)
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Warn("ignoring parsing size error")
		return 0
	}

	return size
}

func dirSizeInKB(path string) (int64, error) {
	var totalSize int64

	if err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		fi, err := d.Info()
		if err != nil {
			// The file may have been concurrently removed.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}

			return fmt.Errorf("retrieving file info: %w", err)
		}

		totalSize += fi.Size()

		return nil
	}); err != nil {
		return 0, fmt.Errorf("walking directory: %w", err)
	}

	return totalSize / 1024, nil
}
