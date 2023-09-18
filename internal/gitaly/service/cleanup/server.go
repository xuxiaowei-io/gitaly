package cleanup

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedCleanupServiceServer
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
}

// NewServer creates a new instance of a grpc CleanupServer
func NewServer(deps *service.Dependencies) gitalypb.CleanupServiceServer {
	return &server{
		locator:       deps.GetLocator(),
		gitCmdFactory: deps.GetGitCmdFactory(),
		catfileCache:  deps.GetCatfileCache(),
	}
}

func (s *server) localrepo(repo storage.Repository) *localrepo.Repo {
	return localrepo.New(s.locator, s.gitCmdFactory, s.catfileCache, repo)
}
