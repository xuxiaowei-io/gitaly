package backup

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// remoteRepository implements git repository access over GRPC
type remoteRepository struct {
	repo *gitalypb.Repository
	conn *grpc.ClientConn
}

func newRemoteRepository(repo *gitalypb.Repository, conn *grpc.ClientConn) *remoteRepository {
	return &remoteRepository{
		repo: repo,
		conn: conn,
	}
}

// IsEmpty returns true if the repository has no branches.
func (rr *remoteRepository) IsEmpty(ctx context.Context) (bool, error) {
	client := rr.newRepoClient()
	hasLocalBranches, err := client.HasLocalBranches(ctx, &gitalypb.HasLocalBranchesRequest{
		Repository: rr.repo,
	})
	switch {
	case status.Code(err) == codes.NotFound:
		return true, nil
	case err != nil:
		return false, fmt.Errorf("remote repository: is empty: %w", err)
	}
	return !hasLocalBranches.GetValue(), nil
}

// ListRefs fetches the full set of refs and targets for the repository
func (rr *remoteRepository) ListRefs(ctx context.Context) ([]git.Reference, error) {
	refClient := rr.newRefClient()
	stream, err := refClient.ListRefs(ctx, &gitalypb.ListRefsRequest{
		Repository: rr.repo,
		Head:       true,
		Patterns:   [][]byte{[]byte("refs/")},
	})
	if err != nil {
		return nil, fmt.Errorf("remote repository: list refs: %w", err)
	}

	var refs []git.Reference

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("remote repository: list refs: %w", err)
		}
		for _, ref := range resp.GetReferences() {
			refs = append(refs, git.NewReference(git.ReferenceName(ref.GetName()), ref.GetTarget()))
		}
	}

	return refs, nil
}

// GetCustomHooks fetches the custom hooks archive.
func (rr *remoteRepository) GetCustomHooks(ctx context.Context, out io.Writer) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{Repository: rr.repo})
	if err != nil {
		return fmt.Errorf("remote repository: get custom hooks: %w", err)
	}

	hooks := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	if _, err := io.Copy(out, hooks); err != nil {
		return fmt.Errorf("remote repository: get custom hooks: %w", err)
	}

	return nil
}

// CreateBundle fetches a bundle that contains refs matching patterns.
func (rr *remoteRepository) CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.CreateBundleFromRefList(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}
	c := chunk.New(&createBundleFromRefListSender{
		stream: stream,
	})

	buf := bufio.NewReader(patterns)
	for {
		line, err := buf.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("remote repository: create bundle: %w", err)
		}

		line = bytes.TrimSuffix(line, []byte("\n"))

		if err := c.Send(&gitalypb.CreateBundleFromRefListRequest{
			Repository: rr.repo,
			Patterns:   [][]byte{line},
		}); err != nil {
			return fmt.Errorf("remote repository: create bundle: %w", err)
		}
	}
	if err := c.Flush(); err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}

	bundle := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		if structerr.GRPCCode(err) == codes.FailedPrecondition {
			err = errEmptyBundle
		}
		return resp.GetData(), err
	})

	if _, err := io.Copy(out, bundle); err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}

	return nil
}

func (rr *remoteRepository) newRepoClient() gitalypb.RepositoryServiceClient {
	return gitalypb.NewRepositoryServiceClient(rr.conn)
}

func (rr *remoteRepository) newRefClient() gitalypb.RefServiceClient {
	return gitalypb.NewRefServiceClient(rr.conn)
}

type localRepository struct {
	locator storage.Locator
	repo    *localrepo.Repo
}

func newLocalRepository(locator storage.Locator, repo *localrepo.Repo) *localRepository {
	return &localRepository{
		locator: locator,
		repo:    repo,
	}
}

// IsEmpty returns true if the repository has no branches.
func (r *localRepository) IsEmpty(ctx context.Context) (bool, error) {
	path, err := r.locator.GetPath(r.repo)
	if err != nil {
		return false, fmt.Errorf("local repository: is empty: %w", err)
	}

	if !storage.IsGitDirectory(path) {
		// Backups do not currently differentiate between non-existent and
		// empty. See https://gitlab.com/gitlab-org/gitlab/-/issues/357044
		return true, nil
	}

	hasBranches, err := r.repo.HasBranches(ctx)
	if err != nil {
		return false, fmt.Errorf("local repository: is empty: %w", err)
	}

	return !hasBranches, nil
}

// ListRefs fetches the full set of refs and targets for the repository.
func (r *localRepository) ListRefs(ctx context.Context) ([]git.Reference, error) {
	refs, err := r.repo.GetReferences(ctx, "refs/")
	if err != nil {
		return nil, fmt.Errorf("local repository: list refs: %w", err)
	}

	headOID, err := r.repo.ResolveRevision(ctx, git.Revision("HEAD"))
	switch {
	case errors.Is(err, git.ErrReferenceNotFound):
		// Nothing to add
	case err != nil:
		return nil, structerr.NewInternal("local repository: list refs: %w", err)
	default:
		head := git.NewReference("HEAD", headOID.String())
		refs = append([]git.Reference{head}, refs...)
	}

	return refs, nil
}

// GetCustomHooks fetches the custom hooks archive.
func (r *localRepository) GetCustomHooks(ctx context.Context, out io.Writer) error {
	if err := repoutil.GetCustomHooks(ctx, r.locator, out, r.repo); err != nil {
		return fmt.Errorf("local repository: list refs: %w", err)
	}
	return nil
}

// CreateBundle fetches a bundle that contains refs matching patterns.
func (r *localRepository) CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error {
	err := r.repo.CreateBundle(ctx, out, &localrepo.CreateBundleOpts{
		Patterns: patterns,
	})
	switch {
	case errors.Is(err, localrepo.ErrEmptyBundle):
		return errEmptyBundle
	case err != nil:
		return fmt.Errorf("local repository: create bundle: %w", err)
	}

	return nil
}
