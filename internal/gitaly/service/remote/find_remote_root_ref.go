package remote

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

const headPrefix = "HEAD branch: "

func (s *server) findRemoteRootRef(ctx context.Context, request *gitalypb.FindRemoteRootRefRequest) (string, error) {
	config := []git.ConfigPair{
		{Key: "remote.inmemory.url", Value: request.RemoteUrl},
	}

	if authHeader := request.GetHttpAuthorizationHeader(); authHeader != "" {
		config = append(config, git.ConfigPair{
			Key:   fmt.Sprintf("http.%s.extraHeader", request.RemoteUrl),
			Value: "Authorization: " + authHeader,
		})
	}
	//nolint:staticcheck
	if host := request.GetHttpHost(); host != "" {
		config = append(config, git.ConfigPair{
			Key:   fmt.Sprintf("http.%s.extraHeader", request.RemoteUrl),
			Value: "Host: " + host,
		})
	}

	cmd, err := s.gitCmdFactory.New(ctx, request.Repository,
		git.SubSubCmd{
			Name:   "remote",
			Action: "show",
			Args:   []string{"inmemory"},
		},
		git.WithRefTxHook(request.Repository),
		git.WithConfigEnv(config...),
	)
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, headPrefix) {
			rootRef := strings.TrimPrefix(line, headPrefix)
			if rootRef == "(unknown)" {
				return "", helper.ErrNotFoundf("no remote HEAD found")
			}
			return rootRef, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", err
	}

	return "", helper.ErrNotFoundf("couldn't query the remote HEAD")
}

// FindRemoteRootRef queries the remote to determine its HEAD
func (s *server) FindRemoteRootRef(ctx context.Context, in *gitalypb.FindRemoteRootRefRequest) (*gitalypb.FindRemoteRootRefResponse, error) {
	if in.GetRemoteUrl() == "" {
		return nil, helper.ErrInvalidArgumentf("missing remote URL")
	}
	if in.Repository == nil {
		return nil, helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}

	ref, err := s.findRemoteRootRef(ctx, in)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.FindRemoteRootRefResponse{Ref: ref}, nil
}
