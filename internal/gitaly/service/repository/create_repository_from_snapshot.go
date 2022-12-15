package repository

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/tracing"
)

// httpTransport defines a http.Transport with values that are more restrictive
// than for http.DefaultTransport.
//
// They define shorter TLS Handshake, and more aggressive connection closing
// to prevent the connection hanging and reduce FD usage.
var httpTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 10 * time.Second,
	}).DialContext,
	MaxIdleConns:          2,
	IdleConnTimeout:       30 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 10 * time.Second,
	ResponseHeaderTimeout: 30 * time.Second,
}

// httpClient defines a http.Client that uses the specialized httpTransport
// (above). It also disables following redirects, as we don't expect this to be
// required for this RPC.
var httpClient = &http.Client{
	Transport: correlation.NewInstrumentedRoundTripper(tracing.NewRoundTripper(httpTransport)),
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// newResolvedHTTPClient is a modified version of the httpClient variable but here we resolve the
// URL to predefined IP:PORT. This is to avoid DNS rebinding.
func newResolvedHTTPClient(httpAddress, resolvedAddress string) (*http.Client, error) {
	url, err := url.ParseRequestURI(httpAddress)
	if err != nil {
		return nil, structerr.NewInvalidArgument("parsing HTTP URL: %w", err)
	}

	port := url.Port()
	if port == "" {
		switch url.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return nil, structerr.NewInvalidArgument("unsupported schema %q", url.Scheme)
		}
	}

	// Sanity-check whether the resolved address is a valid IP address.
	if net.ParseIP(resolvedAddress) == nil {
		return nil, structerr.NewInvalidArgument("invalid resolved address %q", resolvedAddress)
	}

	transport := httpTransport.Clone()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return httpTransport.DialContext(ctx, network, fmt.Sprintf("%s:%s", resolvedAddress, port))
	}

	return &http.Client{
		Transport: correlation.NewInstrumentedRoundTripper(tracing.NewRoundTripper(transport)),
		// Here we directly return the `ErrUseLastResponse` to prevent redirects
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}

func untar(ctx context.Context, path string, in *gitalypb.CreateRepositoryFromSnapshotRequest) error {
	req, err := http.NewRequestWithContext(ctx, "GET", in.HttpUrl, nil)
	if err != nil {
		return structerr.NewInvalidArgument("Bad HTTP URL: %w", err)
	}

	client := httpClient
	if resolvedAddress := in.GetResolvedAddress(); resolvedAddress != "" {
		client, err = newResolvedHTTPClient(in.HttpUrl, resolvedAddress)
		if err != nil {
			return structerr.NewInvalidArgument("creating resolved HTTP client: %w", err)
		}
	}

	if in.HttpAuth != "" {
		req.Header.Set("Authorization", in.HttpAuth)
	}
	//nolint:staticcheck
	if httpHost := in.GetHttpHost(); httpHost != "" {
		req.Host = httpHost
	}

	rsp, err := client.Do(req)
	if err != nil {
		return structerr.NewInternal("HTTP request failed: %w", err)
	}
	defer rsp.Body.Close()

	if rsp.StatusCode < http.StatusOK || rsp.StatusCode >= http.StatusMultipleChoices {
		return structerr.NewInternal("HTTP server: %s", rsp.Status)
	}

	cmd, err := command.New(ctx, []string{"tar", "-C", path, "-xvf", "-"}, command.WithStdin(rsp.Body))
	if err != nil {
		return err
	}

	return cmd.Wait()
}

func (s *server) CreateRepositoryFromSnapshot(ctx context.Context, in *gitalypb.CreateRepositoryFromSnapshotRequest) (*gitalypb.CreateRepositoryFromSnapshotResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := repoutil.Create(ctx, s.locator, s.gitCmdFactory, s.txManager, repository, func(repo *gitalypb.Repository) error {
		path, err := s.locator.GetPath(repo)
		if err != nil {
			return structerr.NewInternal("getting repo path: %w", err)
		}

		// The archive contains a partial git repository, missing a config file and
		// other important items. Initializing a new bare one and extracting the
		// archive on top of it ensures the created git repository has everything
		// it needs (especially, the config file and hooks directory).
		//
		// NOTE: The received archive is trusted *a lot*. Before pointing this RPC
		// at endpoints not under our control, it should undergo a lot of hardening.
		if err := untar(ctx, path, in); err != nil {
			return structerr.NewInternal("extracting snapshot: %w", err)
		}

		return nil
	}); err != nil {
		return nil, structerr.NewInternal("creating repository: %w", err)
	}

	return &gitalypb.CreateRepositoryFromSnapshotResponse{}, nil
}
