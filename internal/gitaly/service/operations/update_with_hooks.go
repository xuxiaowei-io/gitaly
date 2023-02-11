package operations

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/quarantine"
)

func (s *Server) updateReferenceWithHooks(
	ctx context.Context,
	repo *gitalypb.Repository,
	user *gitalypb.User,
	quarantine *quarantine.Dir,
	reference git.ReferenceName,
	newrev, oldrev git.ObjectID,
	pushOptions ...string,
) error {
	return s.updater.UpdateReference(ctx, repo, user, quarantine, reference, newrev, oldrev, pushOptions...)
}
