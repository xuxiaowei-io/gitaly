package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// NotAllowedError is needed to report internal API errors that
// are made by the pre-receive hook.
type NotAllowedError struct {
	// Message is the error message returned by Rails.
	Message string
	// Protocol is the protocol used.
	Protocol string
	// userID is the ID of the user as whom we have performed access checks.
	UserID string
	// Changes is the changes we have requested.
	Changes []byte
}

func (e NotAllowedError) Error() string {
	return fmt.Sprintf("GitLab: %s", e.Message)
}

func getRelativeObjectDirs(repoPath, gitObjectDir, gitAlternateObjectDirs string) (string, []string, error) {
	repoPathReal, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		return "", nil, err
	}

	gitObjDirRel, err := filepath.Rel(repoPathReal, gitObjectDir)
	if err != nil {
		return "", nil, err
	}

	var gitAltObjDirsRel []string

	for _, gitAltObjDirAbs := range strings.Split(gitAlternateObjectDirs, ":") {
		gitAltObjDirRel, err := filepath.Rel(repoPathReal, gitAltObjDirAbs)
		if err != nil {
			return "", nil, err
		}

		gitAltObjDirsRel = append(gitAltObjDirsRel, gitAltObjDirRel)
	}

	return gitObjDirRel, gitAltObjDirsRel, nil
}

// PreReceiveHook will try to authenticate the changes against the GitLab API.
// If successful, it will execute custom hooks with the given parameters, push
// options and environment.
func (m *GitLabHookManager) PreReceiveHook(ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	payload, err := git.HooksPayloadFromEnv(env)
	if err != nil {
		return helper.ErrInternalf("extracting hooks payload: %w", err)
	}

	changes, err := io.ReadAll(stdin)
	if err != nil {
		return helper.ErrInternalf("reading stdin from request: %w", err)
	}

	fmt.Printf("\n\npush options: %v\n\n\n", pushOptions)

	// Only the primary should execute hooks and increment reference counters.
	if isPrimary(payload) {
		if err := m.preReceiveHook(ctx, payload, repo, pushOptions, env, changes, stdout, stderr); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Warn("stopping transaction because pre-receive hook failed")

			// If the pre-receive hook declines the push, then we need to stop any
			// secondaries voting on the transaction.
			if err := m.stopTransaction(ctx, payload); err != nil {
				ctxlogrus.Extract(ctx).WithError(err).Error("failed stopping transaction in pre-receive hook")
			}

			return err
		}
	}

	return nil
}

func (m *GitLabHookManager) preReceiveHook(ctx context.Context, payload git.HooksPayload, repo *gitalypb.Repository, pushOptions, env []string, changes []byte, stdout, stderr io.Writer) error {
	repoPath, err := m.locator.GetRepoPath(repo)
	if err != nil {
		return helper.ErrInternalf("getting repo path: %v", err)
	}

	if gitObjDir, gitAltObjDirs := getEnvVar("GIT_OBJECT_DIRECTORY", env), getEnvVar("GIT_ALTERNATE_OBJECT_DIRECTORIES", env); gitObjDir != "" && gitAltObjDirs != "" {
		gitObjectDirRel, gitAltObjectDirRel, err := getRelativeObjectDirs(repoPath, gitObjDir, gitAltObjDirs)
		if err != nil {
			return helper.ErrInternalf("getting relative git object directories: %v", err)
		}

		repo.GitObjectDirectory = gitObjectDirRel
		repo.GitAlternateObjectDirectories = gitAltObjectDirRel
	}

	if len(changes) == 0 {
		return helper.ErrInternalf("hook got no reference updates")
	}

	if repo.GetGlRepository() == "" {
		return helper.ErrInternalf("repository not set")
	}
	if payload.UserDetails == nil {
		return helper.ErrInternalf("payload has no receive hooks info")
	}
	if payload.UserDetails.UserID == "" {
		return helper.ErrInternalf("user ID not set")
	}
	if payload.UserDetails.Protocol == "" {
		return helper.ErrInternalf("protocol not set")
	}

	params := gitlab.AllowedParams{
		RepoPath:                      repoPath,
		GitObjectDirectory:            repo.GitObjectDirectory,
		GitAlternateObjectDirectories: repo.GitAlternateObjectDirectories,
		GLRepository:                  repo.GetGlRepository(),
		GLID:                          payload.UserDetails.UserID,
		GLProtocol:                    payload.UserDetails.Protocol,
		Changes:                       string(changes),
	}

	allowed, message, err := m.gitlabClient.Allowed(ctx, params)
	if err != nil {
		// This logic is broken because we just return every potential error to the
		// caller, even though we cannot tell whether the error message stems from
		// the API or if it is a generic error. Ideally, we'd be able to tell
		// whether the error was a PermissionDenied error and only then return
		// the error message as GitLab message. But this will require upstream
		// changes in gitlab-shell first.
		return NotAllowedError{
			Message:  err.Error(),
			UserID:   payload.UserDetails.UserID,
			Protocol: payload.UserDetails.Protocol,
			Changes:  changes,
		}
	}
	// Due to above comment, it means that this code won't ever be executed: when there
	// was an access error, then we would see an HTTP code which doesn't indicate
	// success and thus get an error from `Allowed()`.
	if !allowed {
		return NotAllowedError{
			Message:  message,
			UserID:   payload.UserDetails.UserID,
			Protocol: payload.UserDetails.Protocol,
			Changes:  changes,
		}
	}

	executor, err := m.newCustomHooksExecutor(repo, "pre-receive")
	if err != nil {
		return fmt.Errorf("creating custom hooks executor: %w", err)
	}

	customEnv, err := m.customHooksEnv(ctx, payload, pushOptions, env)
	if err != nil {
		return helper.ErrInternalf("constructing custom hook environment: %v", err)
	}

	if err = executor(
		ctx,
		nil,
		customEnv,
		bytes.NewReader(changes),
		stdout,
		stderr,
	); err != nil {
		return fmt.Errorf("executing custom hooks: %w", err)
	}

	// reference counter
	ok, err := m.gitlabClient.PreReceive(ctx, repo.GetGlRepository())
	if err != nil {
		return helper.ErrInternalf("calling pre_receive endpoint: %v", err)
	}

	if !ok {
		return errors.New("")
	}

	return nil
}
