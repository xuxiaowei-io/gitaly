package repository

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestServerBackupRepository(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	type setupData struct {
		cfg      config.Cfg
		client   gitalypb.RepositoryServiceClient
		repo     *gitalypb.Repository
		backupID string
	}

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T, ctx context.Context, backupSink backup.Sink, backupLocator backup.Locator) setupData
		expectedErr error
	}{
		{
			desc: "success",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				cfg, client := setupRepositoryServiceWithoutRepo(t,
					testserver.WithBackupSink(backupSink),
					testserver.WithBackupLocator(backupLocator),
				)

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

				return setupData{
					cfg:      cfg,
					client:   client,
					repo:     repo,
					backupID: "abc123",
				}
			},
		},
		{
			desc: "missing backup ID",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				cfg, client := setupRepositoryServiceWithoutRepo(t,
					testserver.WithBackupSink(backupSink),
					testserver.WithBackupLocator(backupLocator),
				)

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))

				return setupData{
					cfg:      cfg,
					client:   client,
					repo:     repo,
					backupID: "",
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty BackupId"),
		},
		{
			desc: "missing repository",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				cfg, client := setupRepositoryServiceWithoutRepo(t,
					testserver.WithBackupSink(backupSink),
					testserver.WithBackupLocator(backupLocator),
				)

				return setupData{
					cfg:      cfg,
					client:   client,
					repo:     nil,
					backupID: "abc123",
				}
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "repository with no branches",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				cfg, client := setupRepositoryServiceWithoutRepo(t,
					testserver.WithBackupSink(backupSink),
					testserver.WithBackupLocator(backupLocator),
				)

				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repo:     repo,
					backupID: "abc123",
				}
			},
			expectedErr: structerr.NewFailedPrecondition("backup repository: manager: repository empty: repository skipped").WithDetail(
				&gitalypb.BackupRepositoryResponse_SkippedError{},
			),
		},
		{
			desc: "missing backup sink",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				cfg, client := setupRepositoryServiceWithoutRepo(t,
					testserver.WithBackupLocator(backupLocator),
				)

				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repo:     repo,
					backupID: "abc123",
				}
			},
			expectedErr: structerr.NewFailedPrecondition("backup repository: server-side backups are not configured"),
		},
		{
			desc: "missing backup locator",
			setup: func(t *testing.T, ctx context.Context, backupSink backup.Sink, backupLocator backup.Locator) setupData {
				cfg, client := setupRepositoryServiceWithoutRepo(t,
					testserver.WithBackupSink(backupSink),
				)

				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					cfg:      cfg,
					client:   client,
					repo:     repo,
					backupID: "abc123",
				}
			},
			expectedErr: structerr.NewFailedPrecondition("backup repository: server-side backups are not configured"),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			backupRoot := testhelper.TempDir(t)
			backupSink, err := backup.ResolveSink(ctx, backupRoot)
			require.NoError(t, err)

			backupLocator, err := backup.ResolveLocator("pointer", backupSink)
			require.NoError(t, err)

			vanityRepo := &gitalypb.Repository{
				StorageName:  "does-not-exist",
				RelativePath: "@test/repo.git",
			}

			data := tc.setup(t, ctx, backupSink, backupLocator)

			response, err := data.client.BackupRepository(ctx, &gitalypb.BackupRepositoryRequest{
				Repository:       data.repo,
				VanityRepository: vanityRepo,
				BackupId:         data.backupID,
			})
			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.BackupRepositoryResponse{}, response)

			relativePath := strings.TrimSuffix(vanityRepo.GetRelativePath(), ".git")
			bundlePath := filepath.Join(relativePath, data.backupID, "001.bundle")

			bundle, err := backupSink.GetReader(ctx, bundlePath)
			require.NoError(t, err)
			testhelper.MustClose(t, bundle)
		})
	}
}
