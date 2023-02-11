package sidechannel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/listenmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var magicBytes = []byte("sidechannel")

// sidechannelTimeout is the timeout for establishing a sidechannel
// connection. The sidechannel is supposed to be opened on the same wire with
// incoming grpc request. There won't be real handshaking involved, so it
// should be fast.
const (
	sidechannelTimeout     = 5 * time.Second
	sidechannelMetadataKey = "gitaly-sidechannel-id"
)

// OpenSidechannel opens a sidechannel connection from the stream opener
// extracted from the current peer connection.
func OpenSidechannel(ctx context.Context) (_ *ServerConn, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("sidechannel: failed to extract incoming metadata")
	}
	ids := md.Get(sidechannelMetadataKey)
	if len(ids) == 0 {
		return nil, fmt.Errorf("sidechannel: sidechannel-id not found in incoming metadata")
	}
	sidechannelID, _ := strconv.ParseInt(ids[len(ids)-1], 10, 64)

	muxSession, err := backchannel.GetYamuxSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("sidechannel: fail to extract yamux session: %w", err)
	}

	stream, err := muxSession.Open()
	if err != nil {
		return nil, fmt.Errorf("sidechannel: open stream: %w", err)
	}
	defer func() {
		if err != nil {
			stream.Close()
		}
	}()

	if err := stream.SetDeadline(time.Now().Add(sidechannelTimeout)); err != nil {
		return nil, err
	}

	if _, err := stream.Write(magicBytes); err != nil {
		return nil, fmt.Errorf("sidechannel: write magic bytes: %w", err)
	}

	if err := binary.Write(stream, binary.BigEndian, sidechannelID); err != nil {
		return nil, fmt.Errorf("sidechannel: write stream id: %w", err)
	}

	buf := make([]byte, 2)
	if _, err := io.ReadFull(stream, buf); err != nil {
		return nil, fmt.Errorf("sidechannel: receive confirmation: %w", err)
	}
	if string(buf) != "ok" {
		return nil, fmt.Errorf("sidechannel: expected ok, got %q", buf)
	}

	if err := stream.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}

	return newServerConn(stream), nil
}

// RegisterSidechannel registers the caller into the waiting list of the
// sidechannel registry and injects the sidechannel ID into outgoing metadata.
// The caller is expected to establish the request with the returned context. The
// callback is executed automatically when the sidechannel connection arrives.
// The result is pushed to the error channel of the returned waiter.
func RegisterSidechannel(ctx context.Context, registry *Registry, callback func(*ClientConn) error) (context.Context, *Waiter) {
	waiter := registry.Register(callback)
	ctxOut := metadata.AppendToOutgoingContext(ctx, sidechannelMetadataKey, fmt.Sprintf("%d", waiter.id))
	return ctxOut, waiter
}

// ServerHandshaker implements the server-side sidechannel handshake.
type ServerHandshaker struct {
	registry *Registry
}

// Magic returns the magic bytes for sidechannel
func (s *ServerHandshaker) Magic() string {
	return string(magicBytes)
}

// Handshake implements the handshaking logic for sidechannel so that
// this handshaker reads the sidechannel ID from the wire, and then delegates
// the connection to the sidechannel registry
func (s *ServerHandshaker) Handshake(conn net.Conn, authInfo credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	var sidechannelID sidechannelID
	if err := binary.Read(conn, binary.BigEndian, &sidechannelID); err != nil {
		return nil, nil, fmt.Errorf("sidechannel: fail to extract sidechannel ID: %w", err)
	}

	if err := s.registry.receive(sidechannelID, conn); err != nil {
		return nil, nil, err
	}

	// credentials.ErrConnDispatched, indicating that the connection is already
	// dispatched out of gRPC. gRPC should leave it alone and exit in peace.
	return nil, nil, credentials.ErrConnDispatched
}

// NewServerHandshaker creates a new handshaker for sidechannel to
// embed into listenmux.
func NewServerHandshaker(registry *Registry) *ServerHandshaker {
	return &ServerHandshaker{registry: registry}
}

// NewClientHandshaker is used to enable sidechannel support on outbound
// gRPC connections.
func NewClientHandshaker(logger *logrus.Entry, registry *Registry) backchannel.ClientHandshaker {
	cfg := backchannel.DefaultConfiguration()
	// If a client hangs up while the server is writing data to it then the
	// server will block for 5 minutes by default before erroring out. This
	// makes testing difficult and there is no reason to have such a long
	// timeout in the case of sidechannels. A 1 second timeout is also OK.
	cfg.StreamCloseTimeout = time.Second

	return backchannel.NewClientHandshaker(
		logger,
		func() backchannel.Server {
			lm := listenmux.New(insecure.NewCredentials())
			lm.Register(NewServerHandshaker(registry))
			return grpc.NewServer(grpc.Creds(lm))
		},
		cfg,
	)
}

// SidechannelRegistry associates sidechannel callbacks with outbound
// gRPC calls.
type SidechannelRegistry struct {
	registry *Registry
	logger   *logrus.Entry
}

// NewSidechannelRegistry returns a new registry.
func NewSidechannelRegistry(logger *logrus.Entry) *SidechannelRegistry {
	return &SidechannelRegistry{
		registry: NewRegistry(),
		logger:   logger,
	}
}

// Register registers a callback. It adds metadata to ctx and returns the
// new context. The caller must use the new context for the gRPC call.
// Caller must Close() the returned SidechannelWaiter to prevent resource
// leaks.
func (sr *SidechannelRegistry) Register(
	ctx context.Context,
	callback func(SidechannelConn) error,
) (context.Context, *SidechannelWaiter) {
	ctx, waiter := RegisterSidechannel(
		ctx,
		sr.registry,
		func(cc *ClientConn) error { return callback(cc) },
	)
	return ctx, &SidechannelWaiter{waiter: waiter}
}

// SidechannelWaiter represents a pending sidechannel and its callback.
type SidechannelWaiter struct{ waiter *Waiter }

// Close de-registers the sidechannel callback. If the callback is still
// running, Close blocks until it is done and returns the error return
// value of the callback. If the callback has not been called yet, Close
// returns an error immediately.
func (w *SidechannelWaiter) Close() error { return w.waiter.Close() }

// SidechannelConn allows a client to read and write bytes with less
// overhead than doing so via gRPC messages.
type SidechannelConn interface {
	io.ReadWriter

	// CloseWrite tells the server we won't write any more data. We can still
	// read data from the server after CloseWrite(). A typical use case is in
	// an RPC where the byte stream has a request/response pattern: the
	// client then uses CloseWrite() to signal the end of the request body.
	// When the client calls CloseWrite(), the server receives EOF.
	CloseWrite() error
}

// TestSidechannelServer allows downstream consumers of this package to
// create mock sidechannel gRPC servers.
func TestSidechannelServer(
	logger *logrus.Entry,
	creds credentials.TransportCredentials,
	handler func(interface{}, grpc.ServerStream, io.ReadWriteCloser) error,
) []grpc.ServerOption {
	return []grpc.ServerOption{
		SidechannelServer(logger, creds),
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			conn, err := OpenServerSidechannel(stream.Context())
			if err != nil {
				return err
			}
			defer conn.Close()

			return handler(srv, stream, conn)
		}),
	}
}

// SidechannelServer adds sidechannel support to a gRPC server
func SidechannelServer(logger *logrus.Entry, creds credentials.TransportCredentials) grpc.ServerOption {
	lm := listenmux.New(creds)
	lm.Register(backchannel.NewServerHandshaker(logger, backchannel.NewRegistry(), nil))
	return grpc.Creds(lm)
}

// OpenServerSidechannel opens a sidechannel on the server side. This
// only works if the server was created using SidechannelServer().
func OpenServerSidechannel(ctx context.Context) (io.ReadWriteCloser, error) {
	return OpenSidechannel(ctx)
}

// Dial configures the dialer to establish a Gitaly
// backchannel connection instead of a regular gRPC connection. It also
// injects sr as a sidechannel registry, so that Gitaly can establish
// sidechannels back to the client.
func Dial(ctx context.Context, rawAddress string, sr *SidechannelRegistry, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	clientHandshaker := NewClientHandshaker(sr.logger, sr.registry)
	return client.DialHandshaker(ctx, rawAddress, connOpts, clientHandshaker)
}
