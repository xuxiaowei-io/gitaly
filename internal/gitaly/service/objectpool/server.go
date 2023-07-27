package objectpool

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedObjectPoolServiceServer
	locator             storage.Locator
	gitCmdFactory       git.CommandFactory
	catfileCache        catfile.Cache
	txManager           transaction.Manager
	housekeepingManager housekeeping.Manager
	repositoryCounter   *counter.RepositoryCounter
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	txManager transaction.Manager,
	housekeepingManager housekeeping.Manager,
	repositoryCounter *counter.RepositoryCounter,
) gitalypb.ObjectPoolServiceServer {
	return &server{
		locator:             locator,
		gitCmdFactory:       gitCmdFactory,
		catfileCache:        catfileCache,
		txManager:           txManager,
		housekeepingManager: housekeepingManager,
		repositoryCounter:   repositoryCounter,
	}
}

func (s *server) localrepo(repo storage.Repository) *localrepo.Repo {
	return localrepo.New(s.locator, s.gitCmdFactory, s.catfileCache, repo)
}
