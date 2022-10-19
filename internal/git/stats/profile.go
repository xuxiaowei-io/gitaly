package stats

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

// HasBitmap returns whether or not the repository contains an object bitmap.
func HasBitmap(repoPath string) (bool, error) {
	bitmaps, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.bitmap"))
	if err != nil {
		return false, err
	}

	return len(bitmaps) > 0, nil
}

// PackfilesCount returns the number of packfiles a repository has.
func PackfilesCount(repoPath string) (int, error) {
	packFiles, err := GetPackfiles(repoPath)
	if err != nil {
		return 0, err
	}

	return len(packFiles), nil
}

// GetPackfiles returns the FileInfo of packfiles inside a repository.
func GetPackfiles(repoPath string) ([]fs.DirEntry, error) {
	files, err := os.ReadDir(filepath.Join(repoPath, "objects/pack/"))
	if err != nil {
		return nil, err
	}

	var packFiles []fs.DirEntry
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".pack" {
			packFiles = append(packFiles, f)
		}
	}

	return packFiles, nil
}

// LooseObjects returns the number of loose objects that are not in a packfile.
func LooseObjects(ctx context.Context, repo git.RepositoryExecutor) (int64, error) {
	cmd, err := repo.Exec(ctx, git.SubCmd{
		Name:  "count-objects",
		Flags: []git.Option{git.Flag{Name: "--verbose"}},
	})
	if err != nil {
		return 0, err
	}

	objectStats, err := readObjectInfoStatistic(cmd)
	if err != nil {
		return 0, err
	}

	count, ok := objectStats["count"].(int64)
	if !ok {
		return 0, errors.New("could not get object count")
	}

	return count, nil
}
