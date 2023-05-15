package operations

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *Server) updateReferenceWithHooks(
	ctx context.Context,
	tx *gitaly.Transaction,
	repo *gitalypb.Repository,
	user *gitalypb.User,
	quarantine *quarantine.Dir,
	reference git.ReferenceName,
	newrev, oldrev git.ObjectID,
	pushOptions ...string,
) error {
	return s.updater.UpdateReference(ctx, tx, repo, user, quarantine, reference, newrev, oldrev, pushOptions...)
}
