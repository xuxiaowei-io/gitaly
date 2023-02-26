package commit

import (
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func (s *server) ListFiles(in *gitalypb.ListFilesRequest, stream gitalypb.CommitService_ListFilesServer) error {
	ctxlogrus.Extract(stream.Context()).WithFields(log.Fields{
		"Revision": in.GetRevision(),
	}).Debug("ListFiles")

	if err := validateListFilesRequest(in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(in.GetRepository())
	if _, err := repo.Path(); err != nil {
		return err
	}

	revision := string(in.GetRevision())
	if len(revision) == 0 {
		defaultBranch, err := repo.GetDefaultBranch(stream.Context())
		if err != nil {
			return structerr.NewNotFound("revision not found %q", revision)
		}

		if len(defaultBranch) == 0 {
			return structerr.NewFailedPrecondition("repository does not have a default branch")
		}

		revision = defaultBranch.String()
	}

	contained, err := s.localrepo(repo).HasRevision(stream.Context(), git.Revision(revision))
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	if !contained {
		return stream.Send(&gitalypb.ListFilesResponse{})
	}

	if err := s.listFiles(repo, revision, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func validateListFilesRequest(in *gitalypb.ListFilesRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevisionAllowEmpty(in.Revision); err != nil {
		return err
	}
	return nil
}

func (s *server) listFiles(repo git.RepositoryExecutor, revision string, stream gitalypb.CommitService_ListFilesServer) error {
	cmd, err := repo.Exec(stream.Context(), git.Command{
		Name: "ls-tree",
		Flags: []git.Option{
			git.Flag{Name: "-z"},
			git.Flag{Name: "-r"},
			git.Flag{Name: "--full-tree"},
			git.Flag{Name: "--full-name"},
		},
		Args: []string{revision},
	})
	if err != nil {
		return err
	}

	sender := chunk.New(&listFilesSender{stream: stream})

	for parser := lstree.NewParser(cmd, git.ObjectHashSHA1); ; {
		entry, err := parser.NextEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if entry.Type != localrepo.Blob {
			continue
		}

		if err := sender.Send(&gitalypb.ListFilesResponse{Paths: [][]byte{[]byte(entry.Path)}}); err != nil {
			return err
		}
	}

	return sender.Flush()
}

type listFilesSender struct {
	stream   gitalypb.CommitService_ListFilesServer
	response *gitalypb.ListFilesResponse
}

func (s *listFilesSender) Reset()      { s.response = &gitalypb.ListFilesResponse{} }
func (s *listFilesSender) Send() error { return s.stream.Send(s.response) }
func (s *listFilesSender) Append(m proto.Message) {
	s.response.Paths = append(s.response.Paths, m.(*gitalypb.ListFilesResponse).Paths...)
}
