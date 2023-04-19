package repoutil

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
)

// GetCustomHooks fetches the git hooks for a repository. The hooks are written
// to writer as a tar archive containing a `custom_hooks` directory. If no
// hooks are present in the repository, the response will have no data.
func GetCustomHooks(
	ctx context.Context,
	locator storage.Locator,
	writer io.Writer,
	repo repository.GitRepo,
) error {
	repoPath, err := locator.GetRepoPath(repo)
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	if _, err := os.Lstat(filepath.Join(repoPath, hook.CustomHooksDir)); os.IsNotExist(err) {
		return nil
	}

	if err := archive.WriteTarball(ctx, writer, repoPath, hook.CustomHooksDir); err != nil {
		return structerr.NewInternal("archiving hooks: %w", err)
	}

	return nil
}
