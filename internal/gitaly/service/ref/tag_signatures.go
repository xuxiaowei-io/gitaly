package ref

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func verifyGetTagSignaturesRequest(req *gitalypb.GetTagSignaturesRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if len(req.GetTagRevisions()) == 0 {
		return errors.New("missing revisions")
	}

	for _, revision := range req.GetTagRevisions() {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return fmt.Errorf("invalid revision: %q", revision)
		}
	}
	return nil
}

func (s *server) GetTagSignatures(req *gitalypb.GetTagSignaturesRequest, stream gitalypb.RefService_GetTagSignaturesServer) error {
	if err := verifyGetTagSignaturesRequest(req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	ctx := stream.Context()
	repo := s.localrepo(req.GetRepository())

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return structerr.NewInternal("creating object reader: %w", err)
	}
	defer cancel()

	chunker := chunk.New(&tagSignatureSender{
		send: func(signatures []*gitalypb.GetTagSignaturesResponse_TagSignature) error {
			return stream.Send(&gitalypb.GetTagSignaturesResponse{
				Signatures: signatures,
			})
		},
	})

	revlistOptions := []gitpipe.RevlistOption{
		gitpipe.WithObjects(),
		gitpipe.WithObjectTypeFilter(gitpipe.ObjectTypeTag),
	}

	revlistIter := gitpipe.Revlist(ctx, repo, req.GetTagRevisions(), revlistOptions...)

	catfileObjectIter, err := gitpipe.CatfileObject(ctx, objectReader, revlistIter)
	if err != nil {
		return structerr.NewInternal("creating cat-file object iterator: %w", err)
	}

	for catfileObjectIter.Next() {
		tag := catfileObjectIter.Result()

		raw, err := io.ReadAll(tag)
		if err != nil {
			return structerr.NewInternal("reading tag: %w", err)
		}

		signatureKey, tagText := catfile.ExtractTagSignature(raw)

		if err := chunker.Send(&gitalypb.GetTagSignaturesResponse_TagSignature{
			TagId:     tag.ObjectID().String(),
			Signature: signatureKey,
			Content:   tagText,
		}); err != nil {
			return structerr.NewInternal("sending tag signature chunk: %w", err)
		}
	}

	if err := catfileObjectIter.Err(); err != nil {
		return structerr.NewInternal("cat-file iterator stop: %w", err)
	}

	if err := chunker.Flush(); err != nil {
		return structerr.NewInternal("flushing chunker: %w", err)
	}

	return nil
}

type tagSignatureSender struct {
	signatures []*gitalypb.GetTagSignaturesResponse_TagSignature
	send       func([]*gitalypb.GetTagSignaturesResponse_TagSignature) error
}

func (t *tagSignatureSender) Reset() {
	t.signatures = t.signatures[:0]
}

func (t *tagSignatureSender) Append(m proto.Message) {
	t.signatures = append(t.signatures, m.(*gitalypb.GetTagSignaturesResponse_TagSignature))
}

func (t *tagSignatureSender) Send() error {
	return t.send(t.signatures)
}
