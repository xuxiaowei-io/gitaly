package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

const (
	// A standard terminal window is (at least) 80 characters wide.
	terminalWidth                = 80
	gitRemoteMessagePrefixLength = len("remote: ")
	terminalMessagePadding       = 2

	// Git prefixes remote messages with "remote: ", so this width is subtracted
	// from the width available to us.
	maxMessageWidth = terminalWidth - gitRemoteMessagePrefixLength

	// Our centered text shouldn't start or end right at the edge of the window,
	// so we add some horizontal padding: 2 chars on either side.
	maxMessageTextWidth = maxMessageWidth - 2*terminalMessagePadding
)

func printMessages(messages []gitlab.PostReceiveMessage, w io.Writer) error {
	for _, message := range messages {
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}

		switch message.Type {
		case "basic":
			if _, err := w.Write([]byte(message.Message)); err != nil {
				return err
			}
		case "alert":
			if err := printAlert(message, w); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid message type: %v", message.Type)
		}

		if _, err := w.Write([]byte("\n\n")); err != nil {
			return err
		}
	}

	return nil
}

func centerLine(b []byte) []byte {
	b = bytes.TrimSpace(b)
	linePadding := int(math.Max((float64(maxMessageWidth)-float64(len(b)))/2, 0))
	return append(bytes.Repeat([]byte(" "), linePadding), b...)
}

func printAlert(m gitlab.PostReceiveMessage, w io.Writer) error {
	if _, err := w.Write(bytes.Repeat([]byte("="), maxMessageWidth)); err != nil {
		return err
	}

	if _, err := w.Write([]byte("\n\n")); err != nil {
		return err
	}

	words := strings.Fields(m.Message)

	line := bytes.NewBufferString("")

	for _, word := range words {
		if line.Len()+1+len(word) > maxMessageTextWidth {
			if _, err := w.Write(append(centerLine(line.Bytes()), '\n')); err != nil {
				return err
			}
			line.Reset()
		}

		if _, err := line.WriteString(word + " "); err != nil {
			return err
		}
	}

	if _, err := w.Write(centerLine(line.Bytes())); err != nil {
		return err
	}

	if _, err := w.Write([]byte("\n\n")); err != nil {
		return err
	}

	if _, err := w.Write(bytes.Repeat([]byte("="), maxMessageWidth)); err != nil {
		return err
	}

	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (m *GitLabHookManager) PostReceiveHook(ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	payload, err := git.HooksPayloadFromEnv(env)
	if err != nil {
		return structerr.NewInternal("extracting hooks payload: %w", err)
	}

	changes, err := io.ReadAll(stdin)
	if err != nil {
		return structerr.NewInternal("reading stdin from request: %w", err)
	}

	if isPrimary(payload) {
		if err := m.postReceiveHook(ctx, payload, repo, pushOptions, env, changes, stdout, stderr); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Warn("stopping transaction because post-receive hook failed")

			// If the post-receive hook declines the push, then we need to stop any
			// secondaries voting on the transaction.
			if err := m.stopTransaction(ctx, payload); err != nil {
				ctxlogrus.Extract(ctx).WithError(err).Error("failed stopping transaction in post-receive hook")
			}

			return err
		}
	}

	return nil
}

func (m *GitLabHookManager) postReceiveHook(ctx context.Context, payload git.HooksPayload, repo *gitalypb.Repository, pushOptions, env []string, stdin []byte, stdout, stderr io.Writer) error {
	if len(stdin) == 0 {
		return structerr.NewInternal("hook got no reference updates")
	}

	if payload.UserDetails == nil {
		return structerr.NewInternal("payload has no receive hooks info")
	}
	if payload.UserDetails.UserID == "" {
		return structerr.NewInternal("user ID not set")
	}
	if repo.GetGlRepository() == "" {
		return structerr.NewInternal("repository not set")
	}

	ok, messages, err := m.gitlabClient.PostReceive(
		ctx, repo.GetGlRepository(),
		payload.UserDetails.UserID,
		string(stdin),
		pushOptions...,
	)
	if err != nil {
		return fmt.Errorf("GitLab: %v", err)
	}

	if err := printMessages(messages, stdout); err != nil {
		return fmt.Errorf("error writing messages to stream: %v", err)
	}

	if !ok {
		return errors.New("")
	}

	executor, err := m.newCustomHooksExecutor(repo, "post-receive")
	if err != nil {
		return structerr.NewInternal("creating custom hooks executor: %v", err)
	}

	customEnv, err := m.customHooksEnv(ctx, payload, pushOptions, env)
	if err != nil {
		return structerr.NewInternal("constructing custom hook environment: %v", err)
	}

	if err = executor(
		ctx,
		nil,
		customEnv,
		bytes.NewReader(stdin),
		stdout,
		stderr,
	); err != nil {
		return fmt.Errorf("executing custom hooks: %w", err)
	}

	return nil
}
