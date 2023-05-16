package localrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tempdir"
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

// FetchBundleOpts are optional configurations used when fetching from a bundle.
type FetchBundleOpts struct {
	// UpdateHead updates HEAD based on the HEAD object ID in the bundle file,
	// if available.
	UpdateHead bool
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository with the refspec "+refs/*:refs/*".
func (repo *Repo) FetchBundle(ctx context.Context, txManager transaction.Manager, reader io.Reader, opts *FetchBundleOpts) error {
	if opts == nil {
		opts = &FetchBundleOpts{}
	}

	tmpDir, err := tempdir.New(ctx, repo.GetStorageName(), repo.locator)
	if err != nil {
		return fmt.Errorf("fetch bundle: temp file: %w", err)
	}

	bundlePath := filepath.Join(tmpDir.Path(), "repo.bundle")
	file, err := os.Create(bundlePath)
	if err != nil {
		return fmt.Errorf("fetch bundle: temp file: %w", err)
	}

	if _, err = io.Copy(file, reader); err != nil {
		return fmt.Errorf("fetch bundle: temp file: %w", err)
	}

	fetchConfig := []git.ConfigPair{
		{Key: "remote.inmemory.url", Value: bundlePath},
		{Key: "remote.inmemory.fetch", Value: git.MirrorRefSpec},
	}
	fetchOpts := FetchOpts{
		CommandOptions: []git.CmdOpt{git.WithConfigEnv(fetchConfig...)},
	}
	if err := repo.FetchRemote(ctx, "inmemory", fetchOpts); err != nil {
		return fmt.Errorf("fetch bundle: %w", err)
	}

	if opts.UpdateHead {
		if err := repo.updateHeadFromBundle(ctx, txManager, bundlePath); err != nil {
			return fmt.Errorf("fetch bundle: %w", err)
		}
	}

	return nil
}

// updateHeadFromBundle updates HEAD from a bundle file
func (repo *Repo) updateHeadFromBundle(ctx context.Context, txManager transaction.Manager, bundlePath string) error {
	head, err := repo.findBundleHead(ctx, bundlePath)
	if err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}
	if head == nil {
		return nil
	}

	branch, err := repo.GuessHead(ctx, *head)
	if err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}

	if err := repo.SetDefaultBranch(ctx, txManager, branch); err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}
	return nil
}

// findBundleHead tries to extract HEAD and its target from a bundle. Returns
// nil when HEAD is not found.
func (repo *Repo) findBundleHead(ctx context.Context, bundlePath string) (*git.Reference, error) {
	cmd, err := repo.Exec(ctx, git.Command{
		Name:   "bundle",
		Action: "list-heads",
		Args:   []string{bundlePath, "HEAD"},
	})
	if err != nil {
		return nil, fmt.Errorf("find bundle HEAD: %w", err)
	}
	decoder := git.NewShowRefDecoder(cmd)
	for {
		var ref git.Reference
		err := decoder.Decode(&ref)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("find bundle HEAD: %w", err)
		}
		if ref.Name != "HEAD" {
			continue
		}
		return &ref, nil
	}
	return nil, nil
}
