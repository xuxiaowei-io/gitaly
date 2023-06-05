package internalgitaly

import (
	"errors"
	"fmt"
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s server) BackupRepos(stream gitalypb.InternalGitaly_BackupReposServer) error {
	ctx := stream.Context()

	request, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("backup repos: first request: %w", err)
	}

	if err := validateBackupReposRequest(request); err != nil {
		return structerr.NewInvalidArgument("backup repos: first request: %w", err)
	}

	header := request.GetHeader()
	backupID := header.GetBackupId()

	sink, err := backup.ResolveSink(ctx, header.GetStorageUrl())
	if err != nil {
		return structerr.NewInvalidArgument("backup repos: resolve sink: %w", err)
	}

	locator, err := backup.ResolveLocator("pointer", sink)
	if err != nil {
		return structerr.NewInvalidArgument("backup repos: resolve locator: %w", err)
	}

	manager := backup.NewManagerLocal(sink, locator, s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, backupID)
	pipeline := backup.NewLoggingPipeline(ctxlogrus.Extract(ctx))

	for {
		for _, repo := range request.GetRepositories() {
			pipeline.Handle(ctx, backup.NewCreateCommand(
				manager,
				storage.ServerInfo{},
				repo,
				false,
			))
		}

		var err error
		request, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("backup repos: receive: %w", err)
		}
	}

	if err := pipeline.Done(); err != nil {
		return fmt.Errorf("backup repos: %w", err)
	}

	if err := stream.SendAndClose(&gitalypb.BackupReposResponse{}); err != nil {
		return fmt.Errorf("backup repos: %w", err)
	}

	return nil
}

func validateBackupReposRequest(req *gitalypb.BackupReposRequest) error {
	header := req.Header
	switch {
	case header == nil:
		return fmt.Errorf("empty Header")
	case header.GetBackupId() == "":
		return fmt.Errorf("empty BackupId")
	case header.GetStorageUrl() == "":
		return fmt.Errorf("empty StorageUrl")
	}
	return nil
}
