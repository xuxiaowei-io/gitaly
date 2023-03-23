package backup

import (
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
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
func (repo *remoteRepository) IsEmpty(ctx context.Context) (bool, error) {
	client := repo.newRepoClient()
	hasLocalBranches, err := client.HasLocalBranches(ctx, &gitalypb.HasLocalBranchesRequest{
		Repository: repo.repo,
	})
	switch {
	case status.Code(err) == codes.NotFound:
		return true, nil
	case err != nil:
		return false, fmt.Errorf("IsEmpty: %w", err)
	}
	return !hasLocalBranches.GetValue(), nil
}

// ListRefs fetches the full set of refs and targets for the repository
func (repo *remoteRepository) ListRefs(ctx context.Context) ([]*gitalypb.ListRefsResponse_Reference, error) {
	refClient := repo.newRefClient()
	stream, err := refClient.ListRefs(ctx, &gitalypb.ListRefsRequest{
		Repository: repo.repo,
		Head:       true,
		Patterns:   [][]byte{[]byte("refs/")},
	})
	if err != nil {
		return nil, fmt.Errorf("list refs: %w", err)
	}

	var refs []*gitalypb.ListRefsResponse_Reference

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("list refs: %w", err)
		}
		refs = append(refs, resp.GetReferences()...)
	}

	return refs, nil
}

func (repo *remoteRepository) newRepoClient() gitalypb.RepositoryServiceClient {
	return gitalypb.NewRepositoryServiceClient(repo.conn)
}

func (repo *remoteRepository) newRefClient() gitalypb.RefServiceClient {
	return gitalypb.NewRefServiceClient(repo.conn)
}
