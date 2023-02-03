//go:build !gitaly_test_sha256

package limithandler_test

import (
	"context"
	"io"
	"net"
	"testing"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/log"
	"gitlab.com/gitlab-org/gitaly/v15/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func createNewServer(t *testing.T, cfg config.Cfg, logger *logrus.Logger) *grpc.Server {
	t.Helper()

	logrusEntry := logrus.NewEntry(logger).WithField("test", t.Name())

	concurrencyLimitHandler := limithandler.New(
		cfg,
		limithandler.LimitConcurrencyByRepo,
		limithandler.WithConcurrencyLimiters,
	)

	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpcmw.ChainStreamServer(
			limithandler.StatsStreamInterceptor,
			grpcmwlogrus.StreamServerInterceptor(logrusEntry,
				grpcmwlogrus.WithTimestampFormat(log.LogTimestampFormat),
				grpcmwlogrus.WithMessageProducer(log.MessageProducer(grpcmwlogrus.DefaultMessageProducer, limithandler.FieldsProducer))),
			concurrencyLimitHandler.StreamInterceptor(),
		)),
		grpc.UnaryInterceptor(grpcmw.ChainUnaryServer(
			limithandler.StatsUnaryInterceptor,
			grpcmwlogrus.UnaryServerInterceptor(logrusEntry,
				grpcmwlogrus.WithTimestampFormat(log.LogTimestampFormat),
				grpcmwlogrus.WithMessageProducer(log.MessageProducer(grpcmwlogrus.DefaultMessageProducer, limithandler.FieldsProducer))),
			concurrencyLimitHandler.UnaryInterceptor(),
		)),
	}

	server := grpc.NewServer(opts...)

	gitCommandFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	gitalypb.RegisterRefServiceServer(server, ref.NewServer(
		config.NewLocator(cfg),
		gitCommandFactory,
		transaction.NewManager(cfg, backchannel.NewRegistry()),
		catfileCache,
	))

	return server
}

func getBufDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, url string) (net.Conn, error) {
		return listener.Dial()
	}
}

func TestInterceptor(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	logger, hook := test.NewNullLogger()

	s := createNewServer(t, cfg, logger)
	defer s.Stop()

	bufferSize := 1024 * 1024
	listener := bufconn.Listen(bufferSize)
	go testhelper.MustServe(t, s, listener)

	tests := []struct {
		name            string
		performRPC      func(t *testing.T, ctx context.Context, client gitalypb.RefServiceClient)
		expectedLogData []string
	}{
		{
			name: "Unary",
			performRPC: func(t *testing.T, ctx context.Context, client gitalypb.RefServiceClient) {
				req := &gitalypb.RefExistsRequest{Repository: repo, Ref: []byte("refs/foo")}

				_, err := client.RefExists(ctx, req)
				require.NoError(t, err)
			},
			expectedLogData: []string{"limit.concurrency_queue_ms"},
		},
		{
			name: "Stream",
			performRPC: func(t *testing.T, ctx context.Context, client gitalypb.RefServiceClient) {
				req := &gitalypb.ListRefsRequest{Repository: repo, Patterns: [][]byte{[]byte("refs/heads/")}}

				stream, err := client.ListRefs(ctx, req)
				require.NoError(t, err)

				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
				}
			},
			expectedLogData: []string{"limit.concurrency_queue_ms"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hook.Reset()

			conn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(getBufDialer(listener)), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()

			client := gitalypb.NewRefServiceClient(conn)

			tt.performRPC(t, ctx, client)

			logEntries := hook.AllEntries()
			require.Len(t, logEntries, 1)
			for _, expectedLogKey := range tt.expectedLogData {
				require.Contains(t, logEntries[0].Data, expectedLogKey)
			}
		})
	}
}
