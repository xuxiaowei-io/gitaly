//go:build !gitaly_test_sha256

package hook

import (
	"context"
	"crypto/sha1"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	handler func(in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	if s.handler != nil {
		return s.handler(in)
	}
	return nil, nil
}

func TestReferenceTransactionHookInvalidArgument(t *testing.T) {
	cfg := testcfg.Build(t)
	serverSocketPath := runHooksServer(t, cfg, nil)

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()
	ctx := testhelper.Context(t)

	stream, err := client.ReferenceTransactionHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.ReferenceTransactionHookRequest{}))
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, status.Error(codes.InvalidArgument, "empty Repository"), err)
}

func TestReferenceTransactionHook(t *testing.T) {
	testCases := []struct {
		desc              string
		stdin             []byte
		state             gitalypb.ReferenceTransactionHookRequest_State
		voteResponse      gitalypb.VoteTransactionResponse_TransactionState
		expectedErr       error
		expectedResponse  *gitalypb.ReferenceTransactionHookResponse
		expectedReftxHash []byte
	}{
		{
			desc:         "hook triggers transaction with default state",
			stdin:        []byte("foobar"),
			voteResponse: gitalypb.VoteTransactionResponse_COMMIT,
			expectedResponse: &gitalypb.ReferenceTransactionHookResponse{
				ExitStatus: &gitalypb.ExitStatus{
					Value: 0,
				},
			},
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:         "hook triggers transaction with explicit prepared state",
			stdin:        []byte("foobar"),
			state:        gitalypb.ReferenceTransactionHookRequest_PREPARED,
			voteResponse: gitalypb.VoteTransactionResponse_COMMIT,
			expectedResponse: &gitalypb.ReferenceTransactionHookResponse{
				ExitStatus: &gitalypb.ExitStatus{
					Value: 0,
				},
			},
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:  "hook does not trigger transaction with aborted state",
			stdin: []byte("foobar"),
			state: gitalypb.ReferenceTransactionHookRequest_ABORTED,
			expectedResponse: &gitalypb.ReferenceTransactionHookResponse{
				ExitStatus: &gitalypb.ExitStatus{
					Value: 0,
				},
			},
		},
		{
			desc:  "hook triggers transaction with committed state",
			stdin: []byte("foobar"),
			state: gitalypb.ReferenceTransactionHookRequest_COMMITTED,
			expectedResponse: &gitalypb.ReferenceTransactionHookResponse{
				ExitStatus: &gitalypb.ExitStatus{
					Value: 0,
				},
			},
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:              "hook fails with failed vote",
			stdin:             []byte("foobar"),
			voteResponse:      gitalypb.VoteTransactionResponse_ABORT,
			expectedErr:       structerr.NewAborted("reference-transaction hook: error voting on transaction: transaction was aborted"),
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:              "hook fails with stopped vote",
			stdin:             []byte("foobar"),
			voteResponse:      gitalypb.VoteTransactionResponse_STOP,
			expectedErr:       structerr.NewFailedPrecondition("reference-transaction hook: error voting on transaction: transaction was stopped"),
			expectedReftxHash: []byte("foobar"),
		},
	}

	transactionServer := &testTransactionServer{}
	grpcServer := grpc.NewServer()
	gitalypb.RegisterRefTransactionServer(grpcServer, transactionServer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backchannelConn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer backchannelConn.Close()

	registry := backchannel.NewRegistry()
	backchannelID := registry.RegisterBackchannel(backchannelConn)

	errQ := make(chan error)
	go func() {
		errQ <- grpcServer.Serve(listener)
	}()
	defer func() {
		grpcServer.Stop()
		require.NoError(t, <-errQ)
	}()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)

			var reftxHash []byte
			transactionServer.handler = func(in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				reftxHash = in.ReferenceUpdatesHash
				return &gitalypb.VoteTransactionResponse{
					State: tc.voteResponse,
				}, nil
			}

			cfg.SocketPath = runHooksServer(t, cfg, nil, testserver.WithBackchannelRegistry(registry))
			ctx := testhelper.Context(t)

			repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})

			hooksPayload, err := git.NewHooksPayload(
				cfg,
				repo,
				&txinfo.Transaction{
					BackchannelID: backchannelID,
					ID:            1234,
					Node:          "node-1",
				},
				nil,
				git.ReferenceTransactionHook,
				featureflag.FromContext(ctx),
			).Env()
			require.NoError(t, err)

			environment := []string{
				hooksPayload,
			}

			client, conn := newHooksClient(t, cfg.SocketPath)
			defer conn.Close()

			stream, err := client.ReferenceTransactionHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.ReferenceTransactionHookRequest{
				Repository:           repo,
				State:                tc.state,
				EnvironmentVariables: environment,
			}))
			require.NoError(t, stream.Send(&gitalypb.ReferenceTransactionHookRequest{
				Stdin: tc.stdin,
			}))
			require.NoError(t, stream.CloseSend())

			resp, err := stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, resp)

			var expectedReftxHash []byte
			if tc.expectedReftxHash != nil {
				hash := sha1.Sum(tc.expectedReftxHash)
				expectedReftxHash = hash[:]
			}
			require.Equal(t, expectedReftxHash[:], reftxHash)
		})
	}
}
