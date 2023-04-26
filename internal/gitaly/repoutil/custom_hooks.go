package repoutil

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/voting"
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

// SetCustomHooks transactionally and atomically sets custom hooks for a
// repository. The provided reader should be a tarball containing the custom
// hooks to be extracted to the specified Git repository.
func SetCustomHooks(
	ctx context.Context,
	locator storage.Locator,
	txManager transaction.Manager,
	reader io.Reader,
	repo repository.GitRepo,
) error {
	repoPath, err := locator.GetRepoPath(repo)
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	// The `custom_hooks` directory in the repository is locked to prevent
	// concurrent modification of hooks.
	hooksLock, err := safe.NewLockingDirectory(repoPath, CustomHooksDir)
	if err != nil {
		return fmt.Errorf("creating hooks lock: %w", err)
	}

	if err := hooksLock.Lock(); err != nil {
		return fmt.Errorf("locking hooks: %w", err)
	}
	defer func() {
		// If the `.lock` file is not removed from the `custom_hooks` directory,
		// future modifications to the repository's hooks will be prevented. If
		// this occurs, the `.lock` file will have to be manually removed.
		if err := hooksLock.Unlock(); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Error("failed to unlock hooks")
		}
	}()

	// Create a temporary directory to write the new hooks to and also
	// temporarily store the current repository hooks. This enables "atomic"
	// directory swapping by acting as an intermediary storage location between
	// moves.
	tmpDir, err := tempdir.NewWithoutContext(repo.GetStorageName(), locator)
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}

	defer func() {
		if err := os.RemoveAll(tmpDir.Path()); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Warn("failed to remove temporary directory")
		}
	}()

	if err := ExtractHooks(ctx, reader, tmpDir.Path(), false); err != nil {
		return fmt.Errorf("extracting hooks: %w", err)
	}

	tempHooksPath := filepath.Join(tmpDir.Path(), CustomHooksDir)

	// No hooks will be extracted if the tar archive is empty. If this happens
	// it means the repository should be set with an empty `custom_hooks`
	// directory. Create `custom_hooks` in the temporary directory so that any
	// existing repository hooks will be replaced with this empty directory.
	if err := os.Mkdir(tempHooksPath, perm.PublicDir); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("making temp hooks directory: %w", err)
	}

	preparedVote, err := newDirectoryVote(tempHooksPath)
	if err != nil {
		return fmt.Errorf("generating prepared vote: %w", err)
	}

	// Cast prepared vote with hash of the extracted archive in the temporary
	// `custom_hooks` directory.
	if err := voteCustomHooks(ctx, txManager, preparedVote, voting.Prepared); err != nil {
		return fmt.Errorf("casting prepared vote: %w", err)
	}

	repoHooksPath := filepath.Join(repoPath, CustomHooksDir)
	prevHooksPath := filepath.Join(tmpDir.Path(), "previous_hooks")

	// If the `custom_hooks` directory exists in the repository, move the
	// current hooks to `previous_hooks` in the temporary directory.
	if err := os.Rename(repoHooksPath, prevHooksPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("moving current hooks to temp: %w", err)
	}

	// Move `custom_hooks` from the temporary directory to the repository.
	if err := os.Rename(tempHooksPath, repoHooksPath); err != nil {
		return fmt.Errorf("moving new hooks to repo: %w", err)
	}

	committedVote, err := newDirectoryVote(repoHooksPath)
	if err != nil {
		return fmt.Errorf("generating committed vote: %w", err)
	}

	// Cast committed vote with hash of the extracted archive in the repository
	// `custom_hooks` directory.
	if err := voteCustomHooks(ctx, txManager, committedVote, voting.Committed); err != nil {
		return fmt.Errorf("casting committed vote: %w", err)
	}

	return nil
}

// newDirectoryVote creates a voting.VoteHash by walking the specified path and
// generating a hash based on file name, permissions, and data.
func newDirectoryVote(basePath string) (*voting.VoteHash, error) {
	voteHash := voting.NewVoteHash()

	if err := filepath.WalkDir(basePath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(basePath, path)
		if err != nil {
			return fmt.Errorf("getting relative path: %w", err)
		}

		// Write file relative path to hash.
		_, _ = voteHash.Write([]byte(relPath))

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("getting file info: %w", err)
		}

		// Write file permissions to hash.
		permBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(permBytes, uint32(info.Mode()))
		_, _ = voteHash.Write(permBytes)

		if entry.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file: %w", err)
		}
		defer file.Close()

		// Copy file data to hash.
		if _, err = io.Copy(voteHash, file); err != nil {
			return fmt.Errorf("copying file to hash: %w", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking directory: %w", err)
	}

	return &voteHash, nil
}

// voteCustomHooks casts a vote symbolic of the custom hooks received. If there
// is no transaction voting is skipped.
func voteCustomHooks(
	ctx context.Context,
	txManager transaction.Manager,
	v *voting.VoteHash,
	phase voting.Phase,
) error {
	tx, err := txinfo.TransactionFromContext(ctx)
	if errors.Is(err, txinfo.ErrTransactionNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	vote, err := v.Vote()
	if err != nil {
		return err
	}

	if err := txManager.Vote(ctx, tx, vote, phase); err != nil {
		return fmt.Errorf("vote failed: %w", err)
	}

	return nil
}
