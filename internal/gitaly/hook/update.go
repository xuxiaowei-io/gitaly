package hook

import (
	"context"
	"fmt"
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
func (m *GitLabHookManager) UpdateHook(ctx context.Context, tx *gitaly.Transaction, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
	payload, err := git.HooksPayloadFromEnv(env)
	if err != nil {
		return structerr.NewInternal("extracting hooks payload: %w", err)
	}

	if isPrimary(payload) {
		if err := m.updateHook(ctx, tx, payload, repo, ref, oldValue, newValue, env, stdout, stderr); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Warn("stopping transaction because update hook failed")

			// If the update hook declines the push, then we need
			// to stop any secondaries voting on the transaction.
			if err := m.stopTransaction(ctx, payload); err != nil {
				ctxlogrus.Extract(ctx).WithError(err).Error("failed stopping transaction in update hook")
			}

			return err
		}
	}

	return nil
}

func (m *GitLabHookManager) updateHook(ctx context.Context, tx *gitaly.Transaction, payload git.HooksPayload, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
	objectHash, err := git.ObjectHashByFormat(payload.ObjectFormat)
	if err != nil {
		return fmt.Errorf("looking up object hash: %w", err)
	}

	if ref == "" {
		return structerr.NewInternal("hook got no reference")
	}
	if err := objectHash.ValidateHex(oldValue); err != nil {
		return structerr.NewInternal("hook got invalid old value: %w", err)
	}
	if err := objectHash.ValidateHex(newValue); err != nil {
		return structerr.NewInternal("hook got invalid new value: %w", err)
	}
	if payload.UserDetails == nil {
		return structerr.NewInternal("payload has no receive hooks info")
	}

	executor, err := m.newCustomHooksExecutor(tx, repo, "update")
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	customEnv, err := m.customHooksEnv(ctx, payload, nil, env)
	if err != nil {
		return structerr.NewInternal("constructing custom hook environment: %w", err)
	}

	if err = executor(
		ctx,
		[]string{ref, oldValue, newValue},
		customEnv,
		nil,
		stdout,
		stderr,
	); err != nil {
		return fmt.Errorf("executing custom hooks: %w", err)
	}

	return nil
}
