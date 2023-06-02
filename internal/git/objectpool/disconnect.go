package objectpool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
)

// Disconnect disconnects the specified repository from its object pool. If the repository does not
// utilize an alternate object database, no error is returned. For repositories that depend on
// alternate objects, the following steps are performed:
//   - Alternate objects are hard-linked to the main repository.
//   - The repository's Git alternates file is backed up and object pool disconnected.
//   - A connectivity check is performed to ensure the repository is complete. If this check fails,
//     the repository is reconnected to the object pool via the backup and an error returned.
//
// This operation carries some risk. If the repository is in a broken state, it will not be restored
// until after the connectivity check completes. If Gitaly crashes before the backup is restored,
// the repository may be in a broken state until an administrator intervenes and restores the backed
// up copy of objects/info/alternates.
func Disconnect(ctx context.Context, repo *localrepo.Repo) error {
	repoPath, err := repo.Path()
	if err != nil {
		return err
	}

	altFile, err := repo.InfoAlternatesPath()
	if err != nil {
		return err
	}

	altContents, err := os.ReadFile(altFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	altDir := strings.TrimSpace(string(altContents))
	if strings.Contains(altDir, "\n") {
		return &invalidAlternatesError{altContents: altContents}
	}

	if !filepath.IsAbs(altDir) {
		altDir = filepath.Join(repoPath, "objects", altDir)
	}

	stat, err := os.Stat(altDir)
	if err != nil {
		return err
	}

	if !stat.IsDir() {
		return &invalidAlternatesError{altContents: altContents}
	}

	objectFiles, err := findObjectFiles(altDir)
	if err != nil {
		return err
	}

	for _, path := range objectFiles {
		source := filepath.Join(altDir, path)
		target := filepath.Join(repoPath, "objects", path)

		if err := os.MkdirAll(filepath.Dir(target), perm.SharedDir); err != nil {
			return err
		}

		if err := os.Link(source, target); err != nil {
			if os.IsExist(err) {
				continue
			}

			return err
		}
	}

	backupFile, err := newBackupFile(altFile)
	if err != nil {
		return err
	}

	return removeAlternatesIfOk(ctx, repo, altFile, backupFile)
}

type invalidAlternatesError struct {
	altContents []byte
}

func (e *invalidAlternatesError) Error() string {
	return fmt.Sprintf("invalid content in objects/info/alternates: %q", e.altContents)
}

func findObjectFiles(altDir string) ([]string, error) {
	var objectFiles []string
	if walkErr := filepath.Walk(altDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(altDir, path)
		if err != nil {
			return err
		}

		if strings.HasPrefix(rel, "info/") {
			return nil
		}

		if info.IsDir() {
			return nil
		}

		objectFiles = append(objectFiles, rel)

		return nil
	}); walkErr != nil {
		return nil, walkErr
	}

	sort.Sort(objectPaths(objectFiles))

	return objectFiles, nil
}

type objectPaths []string

func (o objectPaths) Len() int      { return len(o) }
func (o objectPaths) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func (o objectPaths) Less(i, j int) bool {
	return objectPriority(o[i]) <= objectPriority(o[j])
}

// Based on pack_copy_priority in git/tmp-objdir.c
func objectPriority(name string) int {
	if !strings.HasPrefix(name, "pack") {
		return 0
	}
	if strings.HasSuffix(name, ".keep") {
		return 1
	}
	if strings.HasSuffix(name, ".pack") {
		return 2
	}
	if strings.HasSuffix(name, ".idx") {
		return 3
	}
	return 4
}

func newBackupFile(altFile string) (string, error) {
	randSuffix, err := text.RandomHex(6)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.%d.%s", altFile, time.Now().Unix(), randSuffix), nil
}

// removeAlternatesIfOk is dangerous. We optimistically temporarily
// rename objects/info/alternates, and run `git fsck` to see if the
// resulting repo is connected. If this fails we restore
// objects/info/alternates. If the repo is not connected for whatever
// reason, then until this function returns, probably **all concurrent
// RPC calls to the repo will fail**. Also, if Gitaly crashes in the
// middle of this function, the repo is left in a broken state. We do
// take care to leave a copy of the alternates file, so that it can be
// manually restored by an administrator if needed.
func removeAlternatesIfOk(ctx context.Context, repo *localrepo.Repo, altFile, backupFile string) error {
	if err := os.Rename(altFile, backupFile); err != nil {
		return err
	}

	rollback := true
	defer func() {
		if !rollback {
			return
		}

		logger := ctxlogrus.Extract(ctx)

		// If we would do a os.Rename, and then someone else comes and clobbers
		// our file, it's gone forever. This trick with os.Link and os.Rename
		// is equivalent to "cp $backupFile $altFile", meaning backupFile is
		// preserved for possible forensic use.
		tmp := backupFile + ".2"

		if err := os.Link(backupFile, tmp); err != nil {
			logger.WithError(err).Error("copy backup alternates file")
			return
		}

		if err := os.Rename(tmp, altFile); err != nil {
			logger.WithError(err).Error("restore backup alternates file")
		}
	}()

	// The choice here of git rev-list is for performance reasons.
	// git fsck --connectivity-only performed badly for large
	// repositories. The reasons are detailed in https://lore.kernel.org/git/9304B938-4A59-456B-B091-DBBCAA1823B2@gmail.com/
	cmd, err := repo.Exec(ctx, git.Command{
		Name: "rev-list",
		Flags: []git.Option{
			git.Flag{Name: "--objects"},
			git.Flag{Name: "--all"},
			git.Flag{Name: "--quiet"},
		},
	})
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return &connectivityError{error: err}
	}

	rollback = false
	return nil
}

type connectivityError struct{ error }

func (fe *connectivityError) Error() string {
	return fmt.Sprintf("git connectivity error while disconnected: %v", fe.error)
}
