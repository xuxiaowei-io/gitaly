package repository

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) cloneFromURLCommand(
	ctx context.Context,
	repoURL, resolvedAddress, repositoryFullPath, authorizationToken string, mirror bool,
	opts ...git.CmdOpt,
) (*command.Command, error) {
	cloneFlags := []git.Option{
		git.Flag{Name: "--quiet"},
	}

	if mirror {
		cloneFlags = append(cloneFlags, git.Flag{Name: "--mirror"})
	} else {
		cloneFlags = append(cloneFlags, git.Flag{Name: "--bare"})
	}

	u, err := url.Parse(repoURL)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	var config []git.ConfigPair
	if u.User != nil {
		password, hasPassword := u.User.Password()

		var creds string
		if hasPassword {
			creds = u.User.Username() + ":" + password
		} else {
			creds = u.User.Username()
		}

		u.User = nil
		authHeader := fmt.Sprintf("Authorization: Basic %s", base64.StdEncoding.EncodeToString([]byte(creds)))
		config = append(config, git.ConfigPair{Key: "http.extraHeader", Value: authHeader})
	} else if len(authorizationToken) > 0 {
		authHeader := fmt.Sprintf("Authorization: %s", authorizationToken)
		config = append(config, git.ConfigPair{Key: "http.extraHeader", Value: authHeader})
	}

	urlString := u.String()

	if resolvedAddress != "" {
		modifiedURL, resolveConfig, err := git.GetURLAndResolveConfig(u.String(), resolvedAddress)
		if err != nil {
			return nil, structerr.NewInvalidArgument("couldn't get curloptResolve config: %w", err)
		}

		urlString = modifiedURL
		config = append(config, resolveConfig...)
	}

	return s.gitCmdFactory.NewWithoutRepo(ctx,
		git.Command{
			Name:  "clone",
			Flags: cloneFlags,
			Args:  []string{urlString, repositoryFullPath},
		},
		append(opts, git.WithConfigEnv(config...))...,
	)
}

func (s *server) CreateRepositoryFromURL(ctx context.Context, req *gitalypb.CreateRepositoryFromURLRequest) (*gitalypb.CreateRepositoryFromURLResponse, error) {
	if err := validateCreateRepositoryFromURLRequest(s.locator, req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := repoutil.Create(ctx, s.locator, s.gitCmdFactory, s.txManager, s.repositoryCounter, req.GetRepository(), func(repo *gitalypb.Repository) error {
		targetPath, err := s.locator.GetRepoPath(repo, storage.WithRepositoryVerificationSkipped())
		if err != nil {
			return fmt.Errorf("getting temporary repository path: %w", err)
		}

		var stderr bytes.Buffer
		cmd, err := s.cloneFromURLCommand(ctx,
			req.GetUrl(),
			req.GetResolvedAddress(),
			targetPath,
			req.GetHttpAuthorizationHeader(),
			req.GetMirror(),
			git.WithStderr(&stderr),
			git.WithDisabledHooks(),
		)
		if err != nil {
			return fmt.Errorf("starting clone: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("cloning repository: %w, stderr: %q", err, stderr.String())
		}

		if err := s.removeOriginInRepo(ctx, repo); err != nil {
			return fmt.Errorf("removing origin remote: %w", err)
		}

		return nil
	}, repoutil.WithSkipInit()); err != nil {
		return nil, structerr.NewInternal("creating repository: %w", err)
	}

	return &gitalypb.CreateRepositoryFromURLResponse{}, nil
}

func validateCreateRepositoryFromURLRequest(locator storage.Locator, req *gitalypb.CreateRepositoryFromURLRequest) error {
	if err := locator.ValidateRepository(req.GetRepository(), storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return err
	}

	if req.GetUrl() == "" {
		return fmt.Errorf("empty Url")
	}

	return nil
}
