package commit

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) FilterShasWithSignatures(bidi gitalypb.CommitService_FilterShasWithSignaturesServer) error {
	firstRequest, err := bidi.Recv()
	if err != nil {
		return err
	}

	if err = validateFirstFilterShasWithSignaturesRequest(firstRequest); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	if err := s.filterShasWithSignatures(bidi, firstRequest); err != nil {
		return structerr.NewInternal("%w", err)
	}
	return nil
}

func validateFirstFilterShasWithSignaturesRequest(in *gitalypb.FilterShasWithSignaturesRequest) error {
	return service.ValidateRepository(in.GetRepository())
}

func (s *server) filterShasWithSignatures(bidi gitalypb.CommitService_FilterShasWithSignaturesServer, firstRequest *gitalypb.FilterShasWithSignaturesRequest) error {
	ctx := bidi.Context()
	repo := s.localrepo(firstRequest.GetRepository())

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return err
	}
	defer cancel()

	request := firstRequest
	for {
		shas, err := filterCommitShasWithSignatures(ctx, objectReader, request.GetShas())
		if err != nil {
			return err
		}

		if err := bidi.Send(&gitalypb.FilterShasWithSignaturesResponse{Shas: shas}); err != nil {
			return err
		}

		request, err = bidi.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}
	}
}

func filterCommitShasWithSignatures(ctx context.Context, objectReader catfile.ObjectContentReader, shas [][]byte) ([][]byte, error) {
	var foundShas [][]byte
	for _, sha := range shas {
		commit, err := catfile.GetCommit(ctx, objectReader, git.Revision(sha))
		if catfile.IsNotFound(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		if commit.SignatureType == gitalypb.SignatureType_NONE {
			continue
		}

		foundShas = append(foundShas, sha)
	}

	return foundShas, nil
}
