package internalgitaly

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) CleanRepos(ctx context.Context, req *gitalypb.CleanReposRequest) (*gitalypb.CleanReposResponse, error) {
	sPath, err := s.storagePath(req.GetStorageName())
	if err != nil {
		return nil, fmt.Errorf("getting storage path: %w", err)
	}

	directory := filepath.Join(
		sPath,
		"+gitaly",
		"lost+found",
		time.Now().Format("2006-01-02"),
	)

	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating lost+found directory: %w", err)
	}

	logger := ctxlogrus.Extract(ctx)
	for _, relPath := range req.GetRelativePaths() {
		sourceDir := filepath.Join(sPath, relPath)
		destDir := fmt.Sprintf("%s-%d", filepath.Join(directory, relPath), time.Now().Unix())

		entry := logger.WithField("source_dir", sourceDir).
			WithField("dest_dir", destDir)

		if err := os.MkdirAll(
			filepath.Dir(filepath.Join(directory, relPath)),
			os.ModePerm,
		); err != nil {
			return nil, fmt.Errorf("creating destination folder: %w", err)
		}

		if err := os.Rename(sourceDir, destDir); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				entry.WithError(err).Warn("source directory missing")
				continue
			}

			return nil, fmt.Errorf("moving repository: %w", err)
		}
	}

	return &gitalypb.CleanReposResponse{}, nil
}
