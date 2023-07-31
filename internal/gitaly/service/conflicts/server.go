package conflicts

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedConflictsServiceServer
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
	pool          *client.Pool
	hookManager   hook.Manager
	updater       *updateref.UpdaterWithHooks
}

// NewServer creates a new instance of a grpc ConflictsServer
func NewServer(
	hookManager hook.Manager,
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	connsPool *client.Pool,
	updater *updateref.UpdaterWithHooks,
) gitalypb.ConflictsServiceServer {
	return &server{
		hookManager:   hookManager,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
		pool:          connsPool,
		updater:       updater,
	}
}

func (s *server) localrepo(repo storage.Repository) *localrepo.Repo {
	return localrepo.New(s.locator, s.gitCmdFactory, s.catfileCache, repo)
}

func (s *server) quarantinedRepo(
	ctx context.Context, repo *gitalypb.Repository,
) (*quarantine.Dir, *localrepo.Repo, error) {
	quarantineDir, err := quarantine.New(ctx, repo, s.locator)
	if err != nil {
		return nil, nil, structerr.NewInternal("creating object quarantine: %w", err)
	}

	quarantineRepo := s.localrepo(quarantineDir.QuarantinedRepo())

	return quarantineDir, quarantineRepo, nil
}
