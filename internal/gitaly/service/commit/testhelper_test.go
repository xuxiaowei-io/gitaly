package commit

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// setupCommitService makes a basic configuration and starts the service with the client.
func setupCommitService(
	tb testing.TB,
	ctx context.Context,
	opts ...testserver.GitalyServerOpt,
) (config.Cfg, gitalypb.CommitServiceClient) {
	cfg := testcfg.Build(tb)
	cfg.SocketPath = startTestServices(tb, cfg, opts...)

	return cfg, newCommitServiceClient(tb, cfg.SocketPath)
}

// setupCommitServiceWithRepo makes a basic configuration, creates a test repository and starts the service with the client.
func setupCommitServiceWithRepo(
	tb testing.TB,
	ctx context.Context,
	opts ...testserver.GitalyServerOpt,
) (config.Cfg, *gitalypb.Repository, string, gitalypb.CommitServiceClient) {
	cfg, client := setupCommitService(tb, ctx, opts...)

	repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	return cfg, repo, repoPath, client
}

func startTestServices(tb testing.TB, cfg config.Cfg, opts ...testserver.GitalyServerOpt) string {
	tb.Helper()
	return testserver.RunGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterCommitServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
			cfg,
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetHousekeepingManager(),
			deps.GetBackupSink(),
			deps.GetBackupLocator(),
		))
	}, opts...)
}

func newCommitServiceClient(tb testing.TB, serviceSocketPath string) gitalypb.CommitServiceClient {
	tb.Helper()

	connOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.Dial(serviceSocketPath, connOpts...)
	require.NoError(tb, err)
	tb.Cleanup(func() { conn.Close() })

	return gitalypb.NewCommitServiceClient(conn)
}

func dummyCommitAuthor(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad+gitlab-test@gitlab.com"),
		Date:     &timestamppb.Timestamp{Seconds: ts},
		Timezone: []byte("+0200"),
	}
}

type gitCommitsGetter interface {
	GetCommits() []*gitalypb.GitCommit
}

func createCommits(tb testing.TB, cfg config.Cfg, repoPath, branch string, commitCount int, parent git.ObjectID) git.ObjectID {
	for i := 0; i < commitCount; i++ {
		var parents []git.ObjectID
		if parent != "" {
			parents = append(parents, parent)
		}

		parent = gittest.WriteCommit(tb, cfg, repoPath,
			gittest.WithBranch(branch),
			gittest.WithMessage(fmt.Sprintf("%s branch Empty commit %d", branch, i)),
			gittest.WithParents(parents...),
		)
	}

	return parent
}

func getAllCommits(tb testing.TB, getter func() (gitCommitsGetter, error)) []*gitalypb.GitCommit {
	tb.Helper()

	var commits []*gitalypb.GitCommit
	for {
		resp, err := getter()
		if err == io.EOF {
			return commits
		}
		require.NoError(tb, err)

		commits = append(commits, resp.GetCommits()...)
	}
}
