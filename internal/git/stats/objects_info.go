package stats

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
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

// LogObjectsInfo read statistics of the git repo objects
// and logs it under 'count-objects' key as structured entry.
func LogObjectsInfo(ctx context.Context, repo git.RepositoryExecutor) {
	logger := ctxlogrus.Extract(ctx)

	cmd, err := repo.Exec(ctx, git.SubCmd{
		Name:  "count-objects",
		Flags: []git.Option{git.Flag{Name: "--verbose"}},
	})
	if err != nil {
		logger.WithError(err).Warn("failed on bootstrapping to gather object statistic")
		return
	}

	stats, err := readObjectInfoStatistic(cmd)
	if err != nil {
		logger.WithError(err).Warn("failed on reading to gather object statistic")
	}

	if err := cmd.Wait(); err != nil {
		logger.WithError(err).Warn("failed on waiting to gather object statistic")
		return
	}

	if len(stats) > 0 {
		logger.WithField("count_objects", stats).Info("git repo statistic")
	}
}

/*
	readObjectInfoStatistic parses output of 'git count-objects -v' command and represents it as dictionary

current supported format is:

	count: 12
	packs: 2
	size-garbage: 934
	alternate: /some/path/to/.git/objects
	alternate: "/some/other path/to/.git/objects"

will result in:

	{
	  "count": 12,
	  "packs": 2,
	  "size-garbage": 934,
	  "alternate": ["/some/path/to/.git/objects", "/some/other path/to/.git/objects"]
	}
*/
func readObjectInfoStatistic(reader io.Reader) (map[string]interface{}, error) {
	stats := map[string]interface{}{}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue
		}

		// one of: count, size, in-pack, packs, size-pack, prune-packable, garbage, size-garbage, alternate (repeatable)
		key := parts[0]
		rawVal := strings.TrimPrefix(parts[1], ": ")

		switch key {
		case "alternate":
			addMultiString(stats, key, rawVal)
		default:
			addInt(stats, key, rawVal)
		}
	}

	return stats, scanner.Err()
}

func addMultiString(stats map[string]interface{}, key, rawVal string) {
	val := strings.Trim(rawVal, "\" \t\n")

	statVal, found := stats[key]
	if !found {
		stats[key] = val
		return
	}

	statAggr, ok := statVal.([]string) // 'alternate' is only repeatable key and it is a string type
	if ok {
		statAggr = append(statAggr, val)
	} else {
		delete(stats, key) // remove single string value of 'alternate' to replace it with slice
		statAggr = []string{statVal.(string), val}
	}
	stats[key] = statAggr
}

func addInt(stats map[string]interface{}, key, rawVal string) {
	val, err := strconv.ParseInt(rawVal, 10, 64)
	if err != nil {
		return
	}

	stats[key] = val
}
