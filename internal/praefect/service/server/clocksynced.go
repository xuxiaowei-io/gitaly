package server

import (
	"context"
	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
)

// ClockSynced returns whether the system clock has an acceptable time drift when compared to NTP service.
func (s *Server) ClockSynced(_ context.Context, req *gitalypb.ClockSyncedRequest) (*gitalypb.ClockSyncedResponse, error) {
	if err := req.DriftThreshold.CheckValid(); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	synced, err := helper.CheckClockSync(req.NtpHost, req.DriftThreshold.AsDuration())
	if err != nil {
		return nil, err
	}
	return &gitalypb.ClockSyncedResponse{Synced: synced}, nil
}
