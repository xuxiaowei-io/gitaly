package commit

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) LastCommitForPath(ctx context.Context, in *gitalypb.LastCommitForPathRequest) (*gitalypb.LastCommitForPathResponse, error) {
	if err := validateLastCommitForPathRequest(s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	resp, err := s.lastCommitForPath(ctx, in)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return resp, nil
}

func (s *server) lastCommitForPath(ctx context.Context, in *gitalypb.LastCommitForPathRequest) (*gitalypb.LastCommitForPathResponse, error) {
	path := string(in.GetPath())
	if len(path) == 0 || path == "/" {
		path = "."
	}

	repo := s.localrepo(in.GetRepository())
	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, err
	}
	defer cancel()

	options := in.GetGlobalOptions()

	// Preserve backwards compatibility with legacy LiteralPathspec
	// flag: These protobuf changes were not shipped in 13.1, so can be
	// remove before 13.2.
	if in.GetLiteralPathspec() {
		if options == nil {
			options = &gitalypb.GlobalOptions{}
		}

		options.LiteralPathspecs = true
	}

	commit, err := log.LastCommitForPath(ctx, s.gitCmdFactory, objectReader, repo, git.Revision(in.GetRevision()), path, options)
	if log.IsNotFound(err) {
		return &gitalypb.LastCommitForPathResponse{}, nil
	}

	return &gitalypb.LastCommitForPathResponse{Commit: commit}, err
}

func validateLastCommitForPathRequest(locator storage.Locator, in *gitalypb.LastCommitForPathRequest) error {
	if err := locator.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision(in.Revision); err != nil {
		return err
	}
	return nil
}
