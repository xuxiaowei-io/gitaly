package limithandler_test

import (
	"context"
	"io"
	"sync/atomic"

	"google.golang.org/grpc/test/grpc_testing"
)

type server struct {
	grpc_testing.UnimplementedTestServiceServer
	requestCount uint64
	blockCh      chan struct{}
}

func (s *server) registerRequest() {
	atomic.AddUint64(&s.requestCount, 1)
}

func (s *server) getRequestCount() int {
	return int(atomic.LoadUint64(&s.requestCount))
}

func (s *server) UnaryCall(
	ctx context.Context,
	in *grpc_testing.SimpleRequest,
) (*grpc_testing.SimpleResponse, error) {
	s.registerRequest()

	<-s.blockCh // Block to ensure concurrency

	return &grpc_testing.SimpleResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	}, nil
}

func (s *server) StreamingOutputCall(
	in *grpc_testing.StreamingOutputCallRequest,
	stream grpc_testing.TestService_StreamingOutputCallServer,
) error {
	s.registerRequest()

	<-s.blockCh // Block to ensure concurrency

	return stream.Send(&grpc_testing.StreamingOutputCallResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	})
}

func (s *server) StreamingInputCall(stream grpc_testing.TestService_StreamingInputCallServer) error {
	// Read all the input
	for {
		if _, err := stream.Recv(); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		s.registerRequest()
	}

	<-s.blockCh // Block to ensure concurrency

	return stream.SendAndClose(&grpc_testing.StreamingInputCallResponse{
		AggregatedPayloadSize: 9000,
	})
}

func (s *server) FullDuplexCall(stream grpc_testing.TestService_FullDuplexCallServer) error {
	// Read all the input
	for {
		if _, err := stream.Recv(); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		s.registerRequest()
	}

	<-s.blockCh // Block to ensure concurrency

	return stream.Send(&grpc_testing.StreamingOutputCallResponse{
		Payload: &grpc_testing.Payload{
			Body: []byte("success"),
		},
	})
}
