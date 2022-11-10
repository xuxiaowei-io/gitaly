package praefect

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type mockRepositoryService struct {
	gitalypb.UnimplementedRepositoryServiceServer
	ReplicateRepositoryFunc func(context.Context, *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error)
}

func (m *mockRepositoryService) ReplicateRepository(ctx context.Context, r *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
	return m.ReplicateRepositoryFunc(ctx, r)
}
