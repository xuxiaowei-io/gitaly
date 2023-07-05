package backup

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerSideAdapter allows calling the server-side backup RPCs `BackupRepository`
// and `RestoreRepository` through `backup.Strategy` such that server-side
// backups can be used with `backup.Pipeline`.
type ServerSideAdapter struct {
	pool *client.Pool
}

// NewServerSideAdapter creates and returns initialized *ServerSideAdapter instance.
func NewServerSideAdapter(pool *client.Pool) *ServerSideAdapter {
	return &ServerSideAdapter{
		pool: pool,
	}
}

// Create calls the BackupRepository RPC.
func (m ServerSideAdapter) Create(ctx context.Context, req *CreateRequest) error {
	if err := setContextServerInfo(ctx, &req.Server, req.Repository.GetStorageName()); err != nil {
		return fmt.Errorf("server-side create: %w", err)
	}

	client, err := m.newRepoClient(ctx, req.Server)
	if err != nil {
		return fmt.Errorf("server-side create: %w", err)
	}

	_, err = client.BackupRepository(ctx, &gitalypb.BackupRepositoryRequest{
		Repository:       req.Repository,
		VanityRepository: req.VanityRepository,
		BackupId:         req.BackupID,
	})
	if err != nil {
		st := status.Convert(err)
		if st.Code() == codes.NotFound {
			// Backups do not currently differentiate between non-existent and
			// empty. See https://gitlab.com/gitlab-org/gitlab/-/issues/357044
			return fmt.Errorf("server-side create: not found: %w", ErrSkipped)
		}
		for _, detail := range st.Details() {
			switch detail.(type) {
			case *gitalypb.BackupRepositoryResponse_SkippedError:
				return fmt.Errorf("server-side create: empty repository: %w", ErrSkipped)
			}
		}

		return fmt.Errorf("server-side create: %w", err)
	}

	return nil
}

// Restore calls the RestoreRepository RPC.
func (m ServerSideAdapter) Restore(ctx context.Context, req *RestoreRequest) error {
	if err := setContextServerInfo(ctx, &req.Server, req.Repository.GetStorageName()); err != nil {
		return fmt.Errorf("server-side restore: %w", err)
	}

	client, err := m.newRepoClient(ctx, req.Server)
	if err != nil {
		return fmt.Errorf("server-side restore: %w", err)
	}

	_, err = client.RestoreRepository(ctx, &gitalypb.RestoreRepositoryRequest{
		Repository:       req.Repository,
		VanityRepository: req.VanityRepository,
		AlwaysCreate:     req.AlwaysCreate,
		BackupId:         req.BackupID,
	})
	if err != nil {
		return structerr.New("server-side restore: %w", err)
	}

	return nil
}

func (m ServerSideAdapter) newRepoClient(ctx context.Context, server storage.ServerInfo) (gitalypb.RepositoryServiceClient, error) {
	conn, err := m.pool.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}
