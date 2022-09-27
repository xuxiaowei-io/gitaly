package wiki

import (
	"errors"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) WikiWritePage(stream gitalypb.WikiService_WikiWritePageServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateWikiWritePageRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()

	client, err := s.ruby.WikiServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.WikiWritePage(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	err = rubyserver.Proxy(func() error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		return rubyStream.Send(request)
	})

	if err != nil {
		return err
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return stream.SendAndClose(response)
}

func validateWikiWritePageRequest(request *gitalypb.WikiWritePageRequest) error {
	if request.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}
	if len(request.GetName()) == 0 {
		return errors.New("empty Name")
	}

	if request.GetFormat() == "" {
		return errors.New("empty Format")
	}

	return validateRequestCommitDetails(request)
}
