package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/client/internal/backoff"
	"gitlab.com/gitlab-org/gitaly/v15/client/internal/dnsresolver"
	"gitlab.com/gitlab-org/gitaly/v15/client/internal/x509"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protojson"
)

// DefaultDialOpts hold the default DialOptions for connection to Gitaly over UNIX-socket
var DefaultDialOpts = []grpc.DialOption{}

// DialContext dials the Gitaly at the given address with the provided options. Valid address formats are
// 'unix:<socket path>' for Unix sockets, 'tcp://<host:port>' for insecure TCP connections and 'tls://<host:port>'
// for TCP+TLS connections.
//
// The returned ClientConns are configured with tracing and correlation id interceptors to ensure they are propagated
// correctly. They're also configured to send Keepalives with settings matching what Gitaly expects.
//
// connOpts should not contain `grpc.WithInsecure` as DialContext determines whether it is needed or not from the
// scheme. `grpc.TransportCredentials` should not be provided either as those are handled internally as well.
func DialContext(ctx context.Context, rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return DialHandshaker(ctx, rawAddress, connOpts, nil)
}

// Dial calls DialContext with the provided arguments and context.Background. Refer to DialContext's documentation
// for details.
func Dial(rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return DialContext(context.Background(), rawAddress, connOpts)
}

type connectionType int

const (
	invalidConnection connectionType = iota
	tcpConnection
	tlsConnection
	unixConnection
)

func getConnectionType(rawAddress string) connectionType {
	u, err := url.Parse(rawAddress)
	if err != nil {
		return invalidConnection
	}

	switch u.Scheme {
	case "tls":
		return tlsConnection
	case "unix":
		return unixConnection
	case "tcp":
		return tcpConnection
	default:
		return invalidConnection
	}
}

// Handshaker is an interface that allows for wrapping the transport credentials
// with a custom handshake.
type Handshaker interface {
	// ClientHandshake wraps the provided credentials and returns new credentials.
	ClientHandshake(credentials.TransportCredentials) credentials.TransportCredentials
}

// DialHandshaker dials a Gitaly node serving at the given address.
//
// If handshaker is provided, it's passed the transport credentials which would be otherwise set. The transport credentials
// returned by handshaker are then set instead.
func DialHandshaker(ctx context.Context, rawAddress string, connOpts []grpc.DialOption, handshaker Handshaker) (*grpc.ClientConn, error) {
	connOpts = cloneOpts(connOpts) // copy to avoid potentially mutating the backing array of the passed slice

	var canonicalAddress string
	var err error
	var transportCredentials credentials.TransportCredentials

	switch getConnectionType(rawAddress) {
	case invalidConnection:
		return nil, fmt.Errorf("invalid connection string: %q", rawAddress)

	case tlsConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, fmt.Errorf("failed to extract host for 'tls' connection: %w", err)
		}

		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to get system certificat pool for 'tls' connection: %w", err)
		}

		transportCredentials = credentials.NewTLS(&tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		})

	case tcpConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, fmt.Errorf("failed to extract host for 'tcp' connection: %w", err)
		}

	case unixConnection:
		canonicalAddress = rawAddress // This will be overridden by the custom dialer...
		connOpts = append(
			connOpts,
			// Use a custom dialer to ensure that we don't experience
			// issues in environments that have proxy configurations
			// https://gitlab.com/gitlab-org/gitaly/merge_requests/1072#note_140408512
			grpc.WithContextDialer(func(ctx context.Context, addr string) (conn net.Conn, err error) {
				path, err := extractPathFromSocketURL(addr)
				if err != nil {
					return nil, fmt.Errorf("failed to extract host for 'unix' connection: %w", err)
				}

				d := net.Dialer{}
				return d.DialContext(ctx, "unix", path)
			}),
		)
	}

	if handshaker != nil {
		if transportCredentials == nil {
			transportCredentials = insecure.NewCredentials()
		}

		transportCredentials = handshaker.ClientHandshake(transportCredentials)
	}

	if transportCredentials == nil {
		connOpts = append(connOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		connOpts = append(connOpts, grpc.WithTransportCredentials(transportCredentials))
	}

	connOpts = append(connOpts,
		// grpc.KeepaliveParams must be specified at least as large as what is allowed by the
		// server-side grpc.KeepaliveEnforcementPolicy
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			PermitWithoutStream: true,
		}),
		// grpc.WithDisableServiceConfig ignores the service config provided by resolvers
		// when they resolve the target. gRPC provides this feature to inject service
		// config from external sources (DNS TXT record, for example). Gitaly doesn't need
		// this feature. When we implement a custom client-side load balancer, this feature
		// can even break the balancer. So, we should better disable it.
		// For more information, please visit
		// - https://github.com/grpc/proposal/blob/master/A2-service-configs-in-dns.md
		grpc.WithDisableServiceConfig(),
		// grpc.WithDefaultServiceConfig sets the recommended client-side load balancing
		// configuration to client dial. By default, gRPC clients don't support client-side load
		// balancing. After the connection to a host is established for the first time, that
		// client always sticks to that host. In all Gitaly clients, the connection is cached
		// somehow, usually one connection per host. It means they always stick to the same
		// host until the process restarts. This is not a problem in pure Gitaly environment.
		// In a cluster with more than one Praefect node, this behavior may cause serious
		// workload skew, especially after a fail-over event.
		//
		// This option configures the load balancing strategy to `round_robin`. This is a
		// built-in strategy grpc-go provides. When combining with service discovery via DNS,
		// a client can distribute its requests to all discovered nodes in a round-robin
		// fashion. The client can detect the connectivity changes, such as a node goes
		// down/up again. It evicts subsequent requests accordingly.
		//
		// For more information:
		// https://gitlab.com/groups/gitlab-org/-/epics/8971#note_1207008162
		grpc.WithDefaultServiceConfig(defaultServiceConfig()),
	)

	conn, err := grpc.DialContext(ctx, canonicalAddress, connOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %q connection: %w", canonicalAddress, err)
	}

	return conn, nil
}

// FailOnNonTempDialError helps to identify if remote listener is ready to accept new connections.
func FailOnNonTempDialError() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
	}
}

// HealthCheckDialer uses provided dialer as an actual dialer, but issues a health check request to the remote
// to verify the connection was set properly and could be used with no issues.
func HealthCheckDialer(base Dialer) Dialer {
	return func(ctx context.Context, address string, dialOptions []grpc.DialOption) (*grpc.ClientConn, error) {
		cc, err := base(ctx, address, dialOptions)
		if err != nil {
			return nil, err
		}

		if _, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			_ = cc.Close()
			return nil, err
		}

		return cc, nil
	}
}

// DNSResolverBuilderConfig exposes the DNS resolver builder option. It is used to build Gitaly
// custom DNS resolver.
type DNSResolverBuilderConfig dnsresolver.BuilderConfig

// DefaultDNSResolverBuilderConfig returns the default options for building DNS resolver.
func DefaultDNSResolverBuilderConfig() *DNSResolverBuilderConfig {
	return &DNSResolverBuilderConfig{
		RefreshRate:     5 * time.Minute,
		Logger:          logrus.StandardLogger(),
		Backoff:         backoff.NewDefaultExponential(rand.New(rand.NewSource(time.Now().UnixNano()))),
		DefaultGrpcPort: "443",
	}
}

// WithGitalyDNSResolver defines a gRPC dial option for injecting Gitaly's custom DNS resolver. This
// resolver watches for the changes of target URL periodically and update the target subchannels
// accordingly.
func WithGitalyDNSResolver(opts *DNSResolverBuilderConfig) grpc.DialOption {
	return grpc.WithResolvers(dnsresolver.NewBuilder((*dnsresolver.BuilderConfig)(opts)))
}

func cloneOpts(opts []grpc.DialOption) []grpc.DialOption {
	clone := make([]grpc.DialOption, len(opts))
	copy(clone, opts)
	return clone
}

func defaultServiceConfig() string {
	serviceConfig := &gitalypb.ServiceConfig{
		LoadBalancingConfig: []*gitalypb.LoadBalancingConfig{{
			Policy: &gitalypb.LoadBalancingConfig_RoundRobin{},
		}},
	}
	configJSON, err := protojson.Marshal(serviceConfig)
	if err != nil {
		panic("fail to convert service config from protobuf to json")
	}

	return string(configJSON)
}
