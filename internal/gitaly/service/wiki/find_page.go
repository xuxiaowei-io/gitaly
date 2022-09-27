package wiki

import (
	"errors"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) WikiFindPage(request *gitalypb.WikiFindPageRequest, stream gitalypb.WikiService_WikiFindPageServer) error {
	ctx := stream.Context()

	if err := validateWikiFindPage(request); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	client, err := s.ruby.WikiServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, request.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.WikiFindPage(clientCtx, request)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func validateWikiFindPage(request *gitalypb.WikiFindPageRequest) error {
	if request.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}
	if err := git.ValidateRevisionAllowEmpty(request.Revision); err != nil {
		return err
	}
	if len(request.GetTitle()) == 0 {
		return errors.New("empty Title")
	}
	return nil
}
