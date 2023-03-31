package repository

import (
	"bytes"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

func (s *server) CreateBundleFromRefList(stream gitalypb.RepositoryService_CreateBundleFromRefListServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	repository := firstRequest.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	ctx := stream.Context()

	if _, err := s.Cleanup(ctx, &gitalypb.CleanupRequest{Repository: repository}); err != nil {
		return err
	}

	firstRead := true
	patterns := streamio.NewReader(func() ([]byte, error) {
		var request *gitalypb.CreateBundleFromRefListRequest
		if firstRead {
			firstRead = false
			request = firstRequest
		} else {
			var err error
			request, err = stream.Recv()
			if err != nil {
				return nil, err
			}
		}
		return append(bytes.Join(request.GetPatterns(), []byte("\n")), '\n'), nil
	})
	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.CreateBundleFromRefListResponse{Data: p})
	})

	repo := s.localrepo(repository)
	err = repo.CreateBundle(ctx, writer, &localrepo.CreateBundleOpts{Patterns: patterns})
	switch {
	case errors.Is(err, localrepo.ErrEmptyBundle):
		return structerr.NewFailedPrecondition("%w", err)
	case err != nil:
		return structerr.NewInternal("%w", err)
	}

	return nil
}
