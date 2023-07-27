package repository

import (
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) CreateRepositoryFromBundle(stream gitalypb.RepositoryService_CreateRepositoryFromBundleServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return structerr.NewInternal("first request failed: %w", err)
	}

	repo := firstRequest.GetRepository()
	if err := s.locator.ValidateRepository(repo, storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	ctx := stream.Context()

	firstRead := false
	bundleReader := streamio.NewReader(func() ([]byte, error) {
		if !firstRead {
			firstRead = true
			return firstRequest.GetData(), nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	if err := repoutil.Create(ctx, s.locator, s.gitCmdFactory, s.txManager, s.repositoryCounter, repo, func(repo *gitalypb.Repository) error {
		opts := &localrepo.FetchBundleOpts{
			UpdateHead: true,
		}
		if err := s.localrepo(repo).FetchBundle(ctx, s.txManager, bundleReader, opts); err != nil {
			return structerr.NewInternal("%w", err)
		}

		return nil
	}); err != nil {
		return structerr.NewInternal("creating repository: %w", err)
	}

	return stream.SendAndClose(&gitalypb.CreateRepositoryFromBundleResponse{})
}

func sanitizedError(path, format string, a ...interface{}) string {
	str := fmt.Sprintf(format, a...)
	return strings.Replace(str, path, "[REPO PATH]", -1)
}
