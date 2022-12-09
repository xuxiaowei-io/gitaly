package objectpool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// Create creates an object pool for the given source repository. This is done by creating a local
// clone of the source repository where source and target repository will then have the same
// references afterwards.
//
// The source repository will not join the object pool. Thus, its objects won't get deduplicated.
func Create(
	ctx context.Context,
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	txManager transaction.Manager,
	housekeepingManager housekeeping.Manager,
	proto *gitalypb.ObjectPool,
	sourceRepo *localrepo.Repo,
) (*ObjectPool, error) {
	objectPoolPath, err := locator.GetPath(proto.GetRepository())
	if err != nil {
		return nil, err
	}

	if entries, err := os.ReadDir(objectPoolPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, helper.ErrInternalf("reading object pool directory: %w", err)
		}

		// This is the happy path as the target directory does not yet exist.
	} else if len(entries) > 0 {
		// TODO: we should eventually start to fail if the target exists even if it is an
		// empty directory.
		return nil, helper.ErrFailedPreconditionf("target path exists already and is not an empty directory")
	}

	sourceRepoPath, err := sourceRepo.Path()
	if err != nil {
		return nil, fmt.Errorf("getting source repository path: %w", err)
	}

	var stderr bytes.Buffer
	cmd, err := gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name: "clone",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--bare"},
				git.Flag{Name: "--local"},
			},
			Args: []string{sourceRepoPath, objectPoolPath},
		},
		git.WithRefTxHook(sourceRepo),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return nil, fmt.Errorf("spawning clone: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("cloning to pool: %w, stderr: %q", err, stderr.String())
	}

	if err := os.RemoveAll(filepath.Join(objectPoolPath, "hooks")); err != nil {
		return nil, fmt.Errorf("removing hooks: %v", err)
	}

	objectPool, err := FromProto(locator, gitCmdFactory, catfileCache, txManager, housekeepingManager, proto)
	if err != nil {
		return nil, err
	}

	return objectPool, nil
}
