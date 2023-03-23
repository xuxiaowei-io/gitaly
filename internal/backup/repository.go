package backup

import (
	"context"
	"fmt"

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

func (repo *remoteRepository) newRepoClient() gitalypb.RepositoryServiceClient {
	return gitalypb.NewRepositoryServiceClient(repo.conn)
}
