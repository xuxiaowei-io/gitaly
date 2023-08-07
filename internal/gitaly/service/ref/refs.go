package ref

import (
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const (
	tagFormat = "%(objectname) %(objecttype) %(refname:lstrip=2)"
)

// FindDefaultBranchName returns the default branch name for the given repository
func (s *server) FindDefaultBranchName(ctx context.Context, in *gitalypb.FindDefaultBranchNameRequest) (*gitalypb.FindDefaultBranchNameResponse, error) {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	repo := s.localrepo(repository)

	if in.GetHeadOnly() {
		head, err := repo.HeadReference(ctx)
		if err != nil {
			return nil, structerr.NewInternal("head reference: %w", err)
		}
		return &gitalypb.FindDefaultBranchNameResponse{Name: []byte(head)}, nil
	}

	defaultBranch, err := repo.GetDefaultBranch(ctx)
	if err != nil {
		return nil, structerr.NewInternal("get default branch: %w", err)
	}
	return &gitalypb.FindDefaultBranchNameResponse{Name: []byte(defaultBranch)}, nil
}

func parseSortKey(sortKey gitalypb.FindLocalBranchesRequest_SortBy) string {
	switch sortKey {
	case gitalypb.FindLocalBranchesRequest_NAME:
		return "refname"
	case gitalypb.FindLocalBranchesRequest_UPDATED_ASC:
		return "committerdate"
	case gitalypb.FindLocalBranchesRequest_UPDATED_DESC:
		return "-committerdate"
	}

	panic("never reached") // famous last words
}

// FindLocalBranches creates a stream of branches for all local branches in the given repository
func (s *server) FindLocalBranches(in *gitalypb.FindLocalBranchesRequest, stream gitalypb.RefService_FindLocalBranchesServer) error {
	if err := s.locator.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	if err := s.findLocalBranches(in, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) findLocalBranches(in *gitalypb.FindLocalBranchesRequest, stream gitalypb.RefService_FindLocalBranchesServer) error {
	ctx := stream.Context()
	repo := s.localrepo(in.GetRepository())

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return fmt.Errorf("creating object reader: %w", err)
	}
	defer cancel()

	writer := newFindLocalBranchesWriter(stream, objectReader)
	opts := buildFindRefsOpts(ctx, in.GetPaginationParams())
	opts.cmdArgs = []git.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		git.Flag{Name: "--format=" + strings.Join(localBranchFormatFields, "%00")},
		git.Flag{Name: "--sort=" + parseSortKey(in.GetSortBy())},
	}

	if err := s.findRefs(ctx, writer, repo, []string{"refs/heads"}, opts); err != nil {
		return fmt.Errorf("finding refs: %w", err)
	}

	return nil
}

func (s *server) FindAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	if err := s.locator.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	if err := s.findAllBranches(in, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) findAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	repo := s.localrepo(in.GetRepository())

	args := []git.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		git.Flag{Name: "--format=" + strings.Join(localBranchFormatFields, "%00")},
	}

	patterns := []string{"refs/heads", "refs/remotes"}

	if in.MergedOnly {
		defaultBranch, err := repo.GetDefaultBranch(stream.Context())
		if err != nil {
			return fmt.Errorf("default branch name: %w", err)
		}

		args = append(args, git.Flag{Name: fmt.Sprintf("--merged=%s", defaultBranch.String())})

		if len(in.MergedBranches) > 0 {
			patterns = nil

			for _, mergedBranch := range in.MergedBranches {
				patterns = append(patterns, string(mergedBranch))
			}
		}
	}

	ctx := stream.Context()
	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return fmt.Errorf("creating object reader: %w", err)
	}
	defer cancel()

	opts := buildFindRefsOpts(ctx, nil)
	opts.cmdArgs = args

	writer := newFindAllBranchesWriter(stream, objectReader)

	if err = s.findRefs(ctx, writer, repo, patterns, opts); err != nil {
		return fmt.Errorf("finding refs: %w", err)
	}

	return nil
}
