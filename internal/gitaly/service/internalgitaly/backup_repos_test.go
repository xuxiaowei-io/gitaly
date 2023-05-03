package internalgitaly

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServerBackupRepos(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	type setupData struct {
		requests      []*gitalypb.BackupReposRequest
		expectedRepos []*gitalypb.Repository
	}

	for _, tc := range []struct {
		desc        string
		setup       func(tb testing.TB, ctx context.Context, cfg config.Cfg, storageURL string) setupData
		expectedErr error
	}{
		{
			desc: "missing header",
			setup: func(tb testing.TB, ctx context.Context, cfg config.Cfg, storageURL string) setupData {
				return setupData{requests: []*gitalypb.BackupReposRequest{{}}}
			},
			expectedErr: status.Error(codes.InvalidArgument, `backup repos: first request: empty Header`),
		},
		{
			desc: "missing backup ID",
			setup: func(tb testing.TB, ctx context.Context, cfg config.Cfg, storageURL string) setupData {
				return setupData{
					requests: []*gitalypb.BackupReposRequest{{Header: &gitalypb.BackupReposRequest_Header{}}},
				}
			},
			expectedErr: status.Error(codes.InvalidArgument, `backup repos: first request: empty BackupId`),
		},
		{
			desc: "missing storage URL",
			setup: func(tb testing.TB, ctx context.Context, cfg config.Cfg, storageURL string) setupData {
				return setupData{
					requests: []*gitalypb.BackupReposRequest{{Header: &gitalypb.BackupReposRequest_Header{
						BackupId: backupID,
					}}},
				}
			},
			expectedErr: status.Error(codes.InvalidArgument, `backup repos: first request: empty StorageUrl`),
		},
		{
			desc: "invalid storage URL",
			setup: func(tb testing.TB, ctx context.Context, cfg config.Cfg, storageURL string) setupData {
				return setupData{
					requests: []*gitalypb.BackupReposRequest{
						{
							Header: &gitalypb.BackupReposRequest_Header{
								BackupId:   backupID,
								StorageUrl: "%invalid%",
							},
						},
					},
				}
			},
			expectedErr: status.Error(codes.InvalidArgument, `backup repos: resolve sink: parse "%invalid%": invalid URL escape "%in"`),
		},
		{
			desc: "success",
			setup: func(tb testing.TB, ctx context.Context, cfg config.Cfg, storageURL string) setupData {
				var repos []*gitalypb.Repository
				for i := 0; i < 5; i++ {
					repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
					})
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch))
					repos = append(repos, repo)
				}

				return setupData{
					requests: []*gitalypb.BackupReposRequest{
						{
							Header: &gitalypb.BackupReposRequest_Header{
								BackupId:   backupID,
								StorageUrl: storageURL,
							},
							Repositories: repos[:len(repos)/2],
						},
						{
							Repositories: repos[len(repos)/2:],
						},
					},
					expectedRepos: repos,
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)
			storageURL := testhelper.TempDir(t)
			setupData := tc.setup(t, ctx, cfg, storageURL)

			catfileCache := catfile.NewCache(cfg)
			t.Cleanup(catfileCache.Stop)

			srv := NewServer(
				cfg.Storages,
				config.NewLocator(cfg),
				gittest.NewCommandFactory(t, cfg),
				catfileCache,
			)

			client := setupInternalGitalyService(t, cfg, srv)

			stream, err := client.BackupRepos(ctx)
			require.NoError(t, err)

			for _, req := range setupData.requests {
				err = stream.Send(req)
				require.NoError(t, err)
			}

			_, err = stream.CloseAndRecv()
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.expectedErr, err)
			}

			t.Log(storageURL)
			sink, err := backup.ResolveSink(ctx, storageURL)
			require.NoError(t, err)

			for _, repo := range setupData.expectedRepos {
				relativePath := strings.TrimSuffix(repo.GetRelativePath(), ".git")
				bundlePath := filepath.Join(relativePath, backupID, "001.bundle")

				r, err := sink.GetReader(ctx, bundlePath)
				require.NoError(t, err)
				testhelper.MustClose(t, r)
			}
		})
	}
}
