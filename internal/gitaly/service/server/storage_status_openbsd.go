package server

import (
	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"golang.org/x/sys/unix"
)

func getStorageStatus(shard config.Storage) (*gitalypb.DiskStatisticsResponse_StorageStatus, error) {
	var stats unix.Statfs_t
	err := unix.Statfs(shard.Path, &stats)
	if err != nil {
		return nil, err
	}

	// Redundant conversions to handle differences between unix families
	available := int64(stats.F_bavail) * int64(stats.F_bsize)
	used := (int64(stats.F_blocks) - int64(stats.F_bfree)) * int64(stats.F_bsize)

	return &gitalypb.DiskStatisticsResponse_StorageStatus{
		StorageName: shard.Name,
		Available:   available,
		Used:        used,
	}, nil
}
