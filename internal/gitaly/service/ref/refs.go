package ref

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

const (
	tagFormat = "%(objectname) %(objecttype) %(refname:lstrip=2)"
)

type paginationOpts struct {
	// Limit allows to set the maximum numbers of elements
	Limit int
	// IsPageToken allows control over which results are sent as part of the
	// response. When IsPageToken evaluates to true for the first time,
	// results will start to be sent as part of the response. This function
	// will	be called with an empty slice previous to sending the first line
	// in order to allow sending everything right from the beginning.
	IsPageToken func([]byte) bool
	// When PageTokenError is true then the response will return an error when
	// PageToken is not found.
	PageTokenError bool
}

type findRefsOpts struct {
	cmdArgs []git.Option
	delim   byte
	lines.SenderOpts
}

func (s *server) findRefs(ctx context.Context, writer lines.Sender, repo git.RepositoryExecutor, patterns []string, opts *findRefsOpts) error {
	var options []git.Option

	if len(opts.cmdArgs) == 0 {
		options = append(options, git.Flag{Name: "--format=%(refname)"}) // Default format
	} else {
		options = append(options, opts.cmdArgs...)
	}

	cmd, err := repo.Exec(ctx, git.Command{
		Name:  "for-each-ref",
		Flags: options,
		Args:  patterns,
	})
	if err != nil {
		return err
	}

	if err := lines.Send(cmd, writer, lines.SenderOpts{
		IsPageToken:    opts.IsPageToken,
		Delimiter:      opts.delim,
		Limit:          opts.Limit,
		PageTokenError: opts.PageTokenError,
	}); err != nil {
		return err
	}

	return cmd.Wait()
}

// FindDefaultBranchName returns the default branch name for the given repository
func (s *server) FindDefaultBranchName(ctx context.Context, in *gitalypb.FindDefaultBranchNameRequest) (*gitalypb.FindDefaultBranchNameResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	repo := s.localrepo(repository)

	defaultBranch, err := repo.GetDefaultBranch(ctx)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
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
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
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
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
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

func buildPaginationOpts(ctx context.Context, p *gitalypb.PaginationParameter) *paginationOpts {
	opts := &paginationOpts{}
	opts.IsPageToken = func(_ []byte) bool { return true }
	opts.Limit = math.MaxInt32

	if p == nil {
		return opts
	}

	if p.GetLimit() >= 0 {
		opts.Limit = int(p.GetLimit())
	}

	if p.GetPageToken() != "" {
		opts.IsPageToken = func(line []byte) bool {
			// Only use the first part of the line before \x00 separator
			if nullByteIndex := bytes.IndexByte(line, 0); nullByteIndex != -1 {
				line = line[:nullByteIndex]
			}

			return bytes.Equal(line, []byte(p.GetPageToken()))
		}
		opts.PageTokenError = true
	}

	return opts
}

func buildFindRefsOpts(ctx context.Context, p *gitalypb.PaginationParameter) *findRefsOpts {
	opts := buildPaginationOpts(ctx, p)

	refsOpts := &findRefsOpts{delim: '\n'}
	refsOpts.Limit = opts.Limit
	refsOpts.IsPageToken = opts.IsPageToken
	refsOpts.PageTokenError = opts.PageTokenError

	return refsOpts
}

// getTagSortField returns a field that needs to be used to sort the tags.
// If sorting is not provided the default sorting is used: by refname.
func getTagSortField(sortBy *gitalypb.FindAllTagsRequest_SortBy) (string, error) {
	if sortBy == nil {
		return "", nil
	}

	var dir string
	switch sortBy.Direction {
	case gitalypb.SortDirection_ASCENDING:
		dir = ""
	case gitalypb.SortDirection_DESCENDING:
		dir = "-"
	default:
		return "", fmt.Errorf("unsupported sorting direction: %s", sortBy.Direction)
	}

	var key string
	switch sortBy.Key {
	case gitalypb.FindAllTagsRequest_SortBy_REFNAME:
		key = "refname"
	case gitalypb.FindAllTagsRequest_SortBy_CREATORDATE:
		key = "creatordate"
	case gitalypb.FindAllTagsRequest_SortBy_VERSION_REFNAME:
		key = "version:refname"
	default:
		return "", fmt.Errorf("unsupported sorting key: %s", sortBy.Key)
	}

	return dir + key, nil
}
