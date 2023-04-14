package ref

import (
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ListBranchNamesContainingCommit returns a maximum of in.GetLimit() Branch names which contain the
// commit ID passed as argument.
func (s *server) ListBranchNamesContainingCommit(in *gitalypb.ListBranchNamesContainingCommitRequest, stream gitalypb.RefService_ListBranchNamesContainingCommitServer) error {
	ctx := stream.Context()

	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(in.GetRepository())
	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	if err := objectHash.ValidateHex(in.GetCommitId()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	chunker := chunk.New(&branchNamesContainingCommitSender{stream: stream})

	if err := listRefNames(ctx, repo, chunker, "refs/heads", containingArgs(in)); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

type containingRequest interface {
	GetCommitId() string
	GetLimit() uint32
}

func containingArgs(req containingRequest) []string {
	args := []string{fmt.Sprintf("--contains=%s", req.GetCommitId())}
	if limit := req.GetLimit(); limit != 0 {
		args = append(args, fmt.Sprintf("--count=%d", limit))
	}
	return args
}

type branchNamesContainingCommitSender struct {
	stream      gitalypb.RefService_ListBranchNamesContainingCommitServer
	branchNames [][]byte
}

func (bs *branchNamesContainingCommitSender) Reset() { bs.branchNames = nil }
func (bs *branchNamesContainingCommitSender) Append(m proto.Message) {
	bs.branchNames = append(bs.branchNames, stripPrefix(m.(*wrapperspb.StringValue).Value, "refs/heads/"))
}

func (bs *branchNamesContainingCommitSender) Send() error {
	return bs.stream.Send(&gitalypb.ListBranchNamesContainingCommitResponse{BranchNames: bs.branchNames})
}

// ListTagNamesContainingCommit returns a maximum of in.GetLimit() Tag names which contain the
// commit ID passed as argument.
func (s *server) ListTagNamesContainingCommit(in *gitalypb.ListTagNamesContainingCommitRequest, stream gitalypb.RefService_ListTagNamesContainingCommitServer) error {
	ctx := stream.Context()

	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(in.GetRepository())
	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	if err := objectHash.ValidateHex(in.GetCommitId()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	chunker := chunk.New(&tagNamesContainingCommitSender{stream: stream})

	if err := listRefNames(ctx, repo, chunker, "refs/tags", containingArgs(in)); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

type tagNamesContainingCommitSender struct {
	stream   gitalypb.RefService_ListTagNamesContainingCommitServer
	tagNames [][]byte
}

func (ts *tagNamesContainingCommitSender) Reset() { ts.tagNames = nil }
func (ts *tagNamesContainingCommitSender) Append(m proto.Message) {
	ts.tagNames = append(ts.tagNames, stripPrefix(m.(*wrapperspb.StringValue).Value, "refs/tags/"))
}

func (ts *tagNamesContainingCommitSender) Send() error {
	return ts.stream.Send(&gitalypb.ListTagNamesContainingCommitResponse{TagNames: ts.tagNames})
}

func stripPrefix(s string, prefix string) []byte {
	return []byte(strings.TrimPrefix(s, prefix))
}
