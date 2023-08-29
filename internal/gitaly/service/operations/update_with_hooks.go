package operations

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab/gitlabaction"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *Server) updateReferenceWithHooks(
	ctx context.Context,
	repo *gitalypb.Repository,
	user *gitalypb.User,
	quarantine *quarantine.Dir,
	action gitlabaction.Action,
	reference git.ReferenceName,
	newrev, oldrev git.ObjectID,
	pushOptions ...string,
) error {
	return s.updater.UpdateReference(ctx, repo, user, quarantine, action, reference, newrev, oldrev, pushOptions...)
}
