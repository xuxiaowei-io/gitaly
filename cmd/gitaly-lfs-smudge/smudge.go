package main

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/git-lfs/git-lfs/v3/lfs"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/smudge"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitlab"
	"gitlab.com/gitlab-org/labkit/log"
)

func filter(ctx context.Context, cfg smudge.Config, to io.Writer, from io.Reader) (returnedErr error) {
	client, err := gitlab.NewHTTPClient(log.ContextLogger(ctx), cfg.Gitlab, cfg.TLS, prometheus.Config{})
	if err != nil {
		return fmt.Errorf("creating HTTP client: %w", err)
	}

	output, err := smudgeOneObject(ctx, cfg, client, from)
	if err != nil {
		return fmt.Errorf("smudging contents: %w", err)
	}
	defer func() {
		if err := output.Close(); err != nil && returnedErr == nil {
			returnedErr = fmt.Errorf("closing LFS object: %w", err)
		}
	}()

	if _, err := io.Copy(to, output); err != nil {
		return fmt.Errorf("writing smudged contents: %w", err)
	}

	return nil
}

func smudgeOneObject(ctx context.Context, cfg smudge.Config, gitlabClient *gitlab.HTTPClient, from io.Reader) (io.ReadCloser, error) {
	logger := log.ContextLogger(ctx)

	ptr, contents, err := lfs.DecodeFrom(from)
	if err != nil {
		// This isn't a valid LFS pointer. Just copy the existing pointer data.
		return io.NopCloser(contents), nil
	}

	logger.WithField("oid", ptr.Oid).Debug("decoded LFS OID")

	qs := url.Values{}
	qs.Set("oid", ptr.Oid)
	qs.Set("gl_repository", cfg.GlRepository)
	u := url.URL{Path: "/lfs", RawQuery: qs.Encode()}

	response, err := gitlabClient.Get(ctx, u.String())
	if err != nil {
		return nil, fmt.Errorf("error loading LFS object: %v", err)
	}

	if response.StatusCode == 200 {
		return response.Body, nil
	}

	if err := response.Body.Close(); err != nil {
		logger.WithError(err).Error("closing LFS pointer body: %w", err)
	}

	return io.NopCloser(contents), nil
}
