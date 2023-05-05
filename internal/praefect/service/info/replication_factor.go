package info

import (
	"context"
	"errors"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/datastore"
)

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) SetReplicationFactor(ctx context.Context, req *gitalypb.SetReplicationFactorRequest) (*gitalypb.SetReplicationFactorResponse, error) {
	resp, err := s.setReplicationFactor(ctx, req)
	if err != nil {
		var invalidArg datastore.InvalidArgumentError
		if errors.As(err, &invalidArg) {
			return nil, structerr.NewInvalidArgument("%w", err)
		}

		return nil, structerr.NewInternal("%w", err)
	}

	return resp, nil
}

func (s *Server) setReplicationFactor(ctx context.Context, req *gitalypb.SetReplicationFactorRequest) (*gitalypb.SetReplicationFactorResponse, error) {
	storages, err := s.assignmentStore.SetReplicationFactor(ctx, req.VirtualStorage, req.RelativePath, int(req.ReplicationFactor))
	if err != nil {
		return nil, fmt.Errorf("set replication factor: %w", err)
	}

	return &gitalypb.SetReplicationFactorResponse{Storages: storages}, nil
}
