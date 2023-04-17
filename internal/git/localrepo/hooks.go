package localrepo

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
)

// GetCustomHooks writes the git hooks for a repository to out. The hooks are
// written in a tar archive containing a `custom_hooks` directory. If no hooks
// are present in the repository, the response will have no data.
func (repo *Repo) GetCustomHooks(ctx context.Context, out io.Writer) error {
	repoPath, err := repo.locator.GetRepoPath(repo)
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	if _, err := os.Lstat(filepath.Join(repoPath, hook.CustomHooksDir)); os.IsNotExist(err) {
		return nil
	}

	if err := archive.WriteTarball(ctx, out, repoPath, hook.CustomHooksDir); err != nil {
		return structerr.NewInternal("archiving hooks: %w", err)
	}
	return nil
}
