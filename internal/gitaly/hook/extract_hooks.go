package hook

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
)

// CustomHooksDir is the directory in which the custom hooks are stored in the repository.
// It's also the directory where the hooks are stored in the TAR archive containing the hooks.
const CustomHooksDir = "custom_hooks"

// ExtractHooks unpacks a tar file containing custom hooks into a `custom_hooks`
// directory at the specified path.
func ExtractHooks(ctx context.Context, reader io.Reader, path string) error {
	// GNU tar does not accept an empty file as a valid tar archive and produces
	// an error. Since an empty hooks tar is symbolic of a repository having no
	// hooks, the reader is peeked to check if there is any data present.
	buf := bufio.NewReader(reader)
	if _, err := buf.Peek(1); err == io.EOF {
		return nil
	}

	cmdArgs := []string{"-xf", "-", "-C", path, CustomHooksDir}

	var stderrBuilder strings.Builder
	cmd, err := command.New(ctx, append([]string{"tar"}, cmdArgs...),
		command.WithStdin(buf),
		command.WithStderr(&stderrBuilder))
	if err != nil {
		return fmt.Errorf("executing tar command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		// GNU and BSD tar versions have differing errors when attempting to
		// extract specified members from a valid tar archive. If the tar
		// archive is valid the errors for GNU and BSD tar should have the
		// same prefix, which can be checked to validate whether the expected
		// content is present in the archive for extraction.
		if strings.HasPrefix(stderrBuilder.String(), "tar: custom_hooks: Not found in archive") {
			return nil
		}

		return fmt.Errorf("waiting for tar command completion: %w", err)
	}

	return nil
}
