package localrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

// ErrEmptyBundle is returned when the bundle to be created would have been empty.
var ErrEmptyBundle = errors.New("refusing to create empty bundle")

// CreateBundle creates a bundle that contains all refs.
// When the bundle would be empty ErrEmptyBundle is returned.
func (repo *Repo) CreateBundle(ctx context.Context, out io.Writer) error {
	var stderr strings.Builder

	err := repo.ExecAndWait(ctx,
		git.Command{
			Name:   "bundle",
			Action: "create",
			Flags: []git.Option{
				git.OutputToStdout,
				git.Flag{Name: "--all"},
			},
		},
		git.WithStdout(out),
		git.WithStderr(&stderr),
	)
	if isExitWithCode(err, 128) && strings.HasPrefix(stderr.String(), "fatal: Refusing to create empty bundle.") {
		return fmt.Errorf("create bundle: %w", ErrEmptyBundle)
	} else if err != nil {
		return fmt.Errorf("create bundle: %w: stderr: %q", err, stderr.String())
	}

	return nil
}
