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

// CreateBundleOpts are optional configurations used when creating a bundle
type CreateBundleOpts struct {
	// Patterns contains all patterns which shall be bundled. Patterns should
	// be in the format accepted by git-rev-list(1) over stdin. Patterns which
	// don't match any reference will be silently ignored.
	Patterns io.Reader
}

// CreateBundle creates a bundle that contains all refs.
// When the bundle would be empty ErrEmptyBundle is returned.
func (repo *Repo) CreateBundle(ctx context.Context, out io.Writer, opts *CreateBundleOpts) error {
	if opts == nil {
		opts = &CreateBundleOpts{}
	}

	var stderr strings.Builder

	gitOpts := []git.Option{
		git.OutputToStdout,
	}
	cmdOpts := []git.CmdOpt{
		git.WithStdout(out),
		git.WithStderr(&stderr),
	}

	if opts.Patterns != nil {
		gitOpts = append(gitOpts,
			git.Flag{Name: "--ignore-missing"},
			git.Flag{Name: "--stdin"},
		)
		cmdOpts = append(cmdOpts, git.WithStdin(opts.Patterns))
	} else {
		gitOpts = append(gitOpts, git.Flag{Name: "--all"})
	}

	err := repo.ExecAndWait(ctx,
		git.Command{
			Name:   "bundle",
			Action: "create",
			Flags:  gitOpts,
		},
		cmdOpts...,
	)
	if isExitWithCode(err, 128) && strings.HasPrefix(stderr.String(), "fatal: Refusing to create empty bundle.") {
		return fmt.Errorf("create bundle: %w", ErrEmptyBundle)
	} else if err != nil {
		return fmt.Errorf("create bundle: %w: stderr: %q", err, stderr.String())
	}

	return nil
}
