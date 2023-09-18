package perm

import (
	"context"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// FixDirectoryPermissions does a recursive directory walk to look for
// directories that cannot be accessed by the current user, and tries to
// fix those with chmod. The motivating problem is that directories with mode
// 0 break os.RemoveAll.
func FixDirectoryPermissions(ctx context.Context, path string) error {
	return fixDirectoryPermissions(ctx, path, make(map[string]struct{}))
}

func fixDirectoryPermissions(ctx context.Context, path string, retriedPaths map[string]struct{}) error {
	return filepath.Walk(path, func(path string, info os.FileInfo, errIncoming error) error {
		if info == nil {
			log.FromContext(ctx).WithFields(log.Fields{
				"path": path,
			}).WithError(errIncoming).Error("nil FileInfo in perm.fixDirectoryPermissions")

			return nil
		}

		const minimumDirPerm = PrivateDir
		if !info.IsDir() || info.Mode()&minimumDirPerm == minimumDirPerm {
			return nil
		}

		if err := os.Chmod(path, info.Mode()|minimumDirPerm); err != nil {
			return err
		}

		if _, retried := retriedPaths[path]; !retried && os.IsPermission(errIncoming) {
			retriedPaths[path] = struct{}{}
			return fixDirectoryPermissions(ctx, path, retriedPaths)
		}

		return nil
	})
}
