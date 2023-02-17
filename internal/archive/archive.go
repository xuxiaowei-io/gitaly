package archive

import (
	"context"
	"fmt"
	"io"
	"runtime"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
)

// WriteTarball writes a tarball to an `io.Writer` for the provided path
// containing the specified archive members. Members should be specified
// relative to `path`.
func WriteTarball(ctx context.Context, writer io.Writer, path string, members ...string) error {
	cmdArgs := []string{"-c", "-f", "-", "-C", path}

	if runtime.GOOS == "darwin" {
		cmdArgs = append(cmdArgs, "--no-mac-metadata")
	}

	cmdArgs = append(cmdArgs, members...)

	cmd, err := command.New(ctx, append([]string{"tar"}, cmdArgs...), command.WithStdout(writer))
	if err != nil {
		return fmt.Errorf("executing tar command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("waiting for tar command completion: %w", err)
	}

	return nil
}
