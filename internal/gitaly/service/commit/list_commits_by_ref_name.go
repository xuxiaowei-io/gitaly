package commit

import (
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func (s *server) ListCommitsByRefName(in *gitalypb.ListCommitsByRefNameRequest, stream gitalypb.CommitService_ListCommitsByRefNameServer) error {
	ctx := stream.Context()
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	repo := s.localrepo(repository)

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}
	defer cancel()

	sender := chunk.New(&commitsByRefNameSender{stream: stream})

	for _, refName := range in.RefNames {
		commit, err := catfile.GetCommit(ctx, objectReader, git.Revision(refName))
		if catfile.IsNotFound(err) {
			continue
		}
		if err != nil {
			return structerr.NewInternal("%w", err)
		}

		commitByRef := &gitalypb.ListCommitsByRefNameResponse_CommitForRef{
			Commit: commit, RefName: refName,
		}

		if err := sender.Send(commitByRef); err != nil {
			return structerr.NewInternal("%w", err)
		}
	}

	return sender.Flush()
}

type commitsByRefNameSender struct {
	response *gitalypb.ListCommitsByRefNameResponse
	stream   gitalypb.CommitService_ListCommitsByRefNameServer
}

func (c *commitsByRefNameSender) Append(m proto.Message) {
	commitByRef := m.(*gitalypb.ListCommitsByRefNameResponse_CommitForRef)

	c.response.CommitRefs = append(c.response.CommitRefs, commitByRef)
}

func (c *commitsByRefNameSender) Send() error { return c.stream.Send(c.response) }
func (c *commitsByRefNameSender) Reset()      { c.response = &gitalypb.ListCommitsByRefNameResponse{} }
