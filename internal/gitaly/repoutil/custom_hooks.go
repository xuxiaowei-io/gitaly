package repoutil

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
)

// CustomHooksDir is the directory in which the custom hooks are stored in the repository.
// It's also the directory where the hooks are stored in the TAR archive containing the hooks.
const CustomHooksDir = "custom_hooks"

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

	if _, err := os.Lstat(filepath.Join(repoPath, CustomHooksDir)); os.IsNotExist(err) {
		return nil
	}

	if err := archive.WriteTarball(ctx, writer, repoPath, CustomHooksDir); err != nil {
		return structerr.NewInternal("archiving hooks: %w", err)
	}

	return nil
}

// ExtractHooks unpacks a tar file containing custom hooks into a `custom_hooks`
// directory at the specified path. If stripPrefix is set, the hooks are extracted directly
// to the target directory instead of in a `custom_hooks` directory in the target directory.
func ExtractHooks(ctx context.Context, reader io.Reader, path string, stripPrefix bool) error {
	// GNU tar does not accept an empty file as a valid tar archive and produces
	// an error. Since an empty hooks tar is symbolic of a repository having no
	// hooks, the reader is peeked to check if there is any data present.
	buf := bufio.NewReader(reader)
	if _, err := buf.Peek(1); err == io.EOF {
		return nil
	}

	stripComponents := "0"
	if stripPrefix {
		stripComponents = "1"
	}

	cmdArgs := []string{"-xf", "-", "-C", path, "--strip-components", stripComponents, CustomHooksDir}

	var stderrBuilder strings.Builder
	cmd, err := command.New(ctx, append([]string{"tar"}, cmdArgs...),
		command.WithStdin(buf),
		command.WithStderr(&stderrBuilder))
	if err != nil {
		return fmt.Errorf("executing tar command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		stderr := stderrBuilder.String()

		// GNU and BSD tar versions have differing errors when attempting to
		// extract specified members from a valid tar archive. If the tar
		// archive is valid the errors for GNU and BSD tar should have the
		// same prefix, which can be checked to validate whether the expected
		// content is present in the archive for extraction.
		if strings.HasPrefix(stderr, "tar: custom_hooks: Not found in archive") {
			return nil
		}

		return structerr.New("waiting for tar command completion: %w", err).WithMetadata("stderr", stderr)
	}

	return nil
}
