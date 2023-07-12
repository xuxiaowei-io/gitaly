package backup

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
			refs = append(refs, git.NewReference(git.ReferenceName(ref.GetName()), git.ObjectID(ref.GetTarget())))
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
			err = localrepo.ErrEmptyBundle
		}
		return resp.GetData(), err
	})

	if _, err := io.Copy(out, bundle); err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}

	return nil
}

type createBundleFromRefListSender struct {
	stream gitalypb.RepositoryService_CreateBundleFromRefListClient
	chunk  gitalypb.CreateBundleFromRefListRequest
}

// Reset should create a fresh response message.
func (s *createBundleFromRefListSender) Reset() {
	s.chunk = gitalypb.CreateBundleFromRefListRequest{}
}

// Append should append the given item to the slice in the current response message
func (s *createBundleFromRefListSender) Append(msg proto.Message) {
	req := msg.(*gitalypb.CreateBundleFromRefListRequest)
	s.chunk.Repository = req.GetRepository()
	s.chunk.Patterns = append(s.chunk.Patterns, req.Patterns...)
}

// Send should send the current response message
func (s *createBundleFromRefListSender) Send() error {
	return s.stream.Send(&s.chunk)
}

// Remove removes the repository. Does not return an error if the repository
// cannot be found.
func (rr *remoteRepository) Remove(ctx context.Context) error {
	repoClient := rr.newRepoClient()
	_, err := repoClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: rr.repo,
	})
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		return fmt.Errorf("remote repository: remove: %w", err)
	}
	return nil
}

// Create creates the repository.
func (rr *remoteRepository) Create(ctx context.Context) error {
	repoClient := rr.newRepoClient()
	if _, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: rr.repo}); err != nil {
		return fmt.Errorf("remote repository: create: %w", err)
	}
	return nil
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository.
func (rr *remoteRepository) FetchBundle(ctx context.Context, reader io.Reader) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.FetchBundle(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	request := &gitalypb.FetchBundleRequest{Repository: rr.repo, UpdateHead: true}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.FetchBundleRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	return nil
}

// SetCustomHooks updates the custom hooks for the repository.
func (rr *remoteRepository) SetCustomHooks(ctx context.Context, reader io.Reader) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.SetCustomHooks(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}

	request := &gitalypb.SetCustomHooksRequest{Repository: rr.repo}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.SetCustomHooksRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
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
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	txManager     transaction.Manager
	repo          *localrepo.Repo
}

func newLocalRepository(
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	txManager transaction.Manager,
	repo *localrepo.Repo,
) *localRepository {
	return &localRepository{
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		txManager:     txManager,
		repo:          repo,
	}
}

// IsEmpty returns true if the repository has no branches.
func (r *localRepository) IsEmpty(ctx context.Context) (bool, error) {
	if err := r.locator.ValidateRepository(r.repo); err != nil {
		if errors.Is(err, storage.ErrRepositoryNotFound) {
			// Backups do not currently differentiate between non-existent and
			// empty. See https://gitlab.com/gitlab-org/gitlab/-/issues/357044
			return true, nil
		}

		return false, fmt.Errorf("local repository: verifying repository: %w", err)
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
		head := git.NewReference("HEAD", headOID)
		refs = append([]git.Reference{head}, refs...)
	}

	return refs, nil
}

// GetCustomHooks fetches the custom hooks archive.
func (r *localRepository) GetCustomHooks(ctx context.Context, out io.Writer) error {
	if err := repoutil.GetCustomHooks(ctx, r.locator, out, r.repo); err != nil {
		return fmt.Errorf("local repository: get custom hooks: %w", err)
	}
	return nil
}

// CreateBundle fetches a bundle that contains refs matching patterns.
func (r *localRepository) CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error {
	err := r.repo.CreateBundle(ctx, out, &localrepo.CreateBundleOpts{
		Patterns: patterns,
	})
	if err != nil {
		return fmt.Errorf("local repository: create bundle: %w", err)
	}

	return nil
}

// Remove removes the repository. Does not return an error if the repository
// cannot be found.
func (r *localRepository) Remove(ctx context.Context) error {
	err := repoutil.Remove(ctx, r.locator, r.txManager, r.repo)
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		return fmt.Errorf("local repository: remove: %w", err)
	}
	return nil
}

// Create creates the repository.
func (r *localRepository) Create(ctx context.Context) error {
	if err := repoutil.Create(
		ctx,
		r.locator,
		r.gitCmdFactory,
		r.txManager,
		r.repo,
		func(repository *gitalypb.Repository) error { return nil },
	); err != nil {
		return fmt.Errorf("local repository: create: %w", err)
	}
	return nil
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository.
func (r *localRepository) FetchBundle(ctx context.Context, reader io.Reader) error {
	err := r.repo.FetchBundle(ctx, r.txManager, reader, &localrepo.FetchBundleOpts{
		UpdateHead: true,
	})
	if err != nil {
		return fmt.Errorf("local repository: fetch bundle: %w", err)
	}
	return nil
}

// SetCustomHooks updates the custom hooks for the repository.
func (r *localRepository) SetCustomHooks(ctx context.Context, reader io.Reader) error {
	if err := repoutil.SetCustomHooks(
		ctx,
		r.locator,
		r.txManager,
		reader,
		r.repo,
	); err != nil {
		return fmt.Errorf("local repository: set custom hooks: %w", err)
	}
	return nil
}
