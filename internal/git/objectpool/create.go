package objectpool

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
)

// Create creates an object pool for the given source repository. This is done by creating a local
// clone of the source repository where source and target repository will then have the same
// references afterwards.
//
// The source repository will not join the object pool. Thus, its objects won't get deduplicated.
func (o *ObjectPool) Create(ctx context.Context, sourceRepo *localrepo.Repo) (err error) {
	sourceRepoPath, err := sourceRepo.Path()
	if err != nil {
		return err
	}

	var stderr bytes.Buffer
	cmd, err := o.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name: "clone",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--bare"},
				git.Flag{Name: "--local"},
			},
			Args: []string{sourceRepoPath, o.FullPath()},
		},
		git.WithRefTxHook(sourceRepo),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return fmt.Errorf("spawning clone: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("cloning to pool: %w, stderr: %q", err, stderr.String())
	}

	if err := os.RemoveAll(filepath.Join(o.FullPath(), "hooks")); err != nil {
		return fmt.Errorf("removing hooks: %v", err)
	}

	return nil
}
