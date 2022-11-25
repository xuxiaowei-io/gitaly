package stats

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
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
func LooseObjects(ctx context.Context, repo *localrepo.Repo) (uint64, error) {
	objectsInfo, err := ObjectsInfoForRepository(ctx, repo)
	if err != nil {
		return 0, err
	}

	return objectsInfo.LooseObjects, nil
}

// LogObjectsInfo read statistics of the git repo objects
// and logs it under 'objects_info' key as structured entry.
func LogObjectsInfo(ctx context.Context, repo *localrepo.Repo) {
	logger := ctxlogrus.Extract(ctx)

	objectsInfo, err := ObjectsInfoForRepository(ctx, repo)
	if err != nil {
		logger.WithError(err).Warn("failed reading objects info")
	} else {
		logger.WithField("objects_info", objectsInfo).Info("repository objects info")
	}
}

// ObjectsInfo contains information on the object database.
type ObjectsInfo struct {
	// LooseObjects is the count of loose objects.
	LooseObjects uint64 `json:"loose_objects"`
	// LooseObjectsSize is the accumulated on-disk size of all loose objects in KiB.
	LooseObjectsSize uint64 `json:"loose_objects_size"`
	// PrunableObjects is the number of objects that exist both as loose and as packed objects.
	// The loose objects may be pruned in that case.
	PruneableObjects uint64 `json:"prunable_objects"`

	// PackedObjects is the count of packed objects.
	PackedObjects uint64 `json:"packed_objects"`
	// Packfiles is the number of packfiles.
	Packfiles uint64 `json:"packfiles"`
	// PackfilesSize is the accumulated on-disk size of all packfiles in KiB.
	PackfilesSize uint64 `json:"packfiles_size"`
	// PackfileBitmapExists indicates whether the repository has a bitmap for one of its
	// packfiles.
	PackfileBitmapExists bool `json:"packfile_bitmap_exists"`

	// CommitGraph contains information about the repository's commit-graphs.
	CommitGraph CommitGraphInfo `json:"commit_graph"`

	// Garbage is the count of files in the object database that are neither a valid loose
	// object nor a valid packfile.
	Garbage uint64 `json:"garbage"`
	// GarbageSize is the accumulated on-disk size of garbage files.
	GarbageSize uint64 `json:"garbage_size"`
	// Alternates is the list of absolute paths of alternate object databases this repository is
	// connected to.
	Alternates []string `json:"alternates"`
}

// ObjectsInfoForRepository computes the ObjectsInfo for a repository.
func ObjectsInfoForRepository(ctx context.Context, repo *localrepo.Repo) (ObjectsInfo, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return ObjectsInfo{}, err
	}

	countObjects, err := repo.Exec(ctx, git.SubCmd{
		Name:  "count-objects",
		Flags: []git.Option{git.Flag{Name: "--verbose"}},
	})
	if err != nil {
		return ObjectsInfo{}, fmt.Errorf("running git-count-objects: %w", err)
	}

	var info ObjectsInfo

	// The expected format is:
	//
	//	count: 12
	//	packs: 2
	//	size-garbage: 934
	//	alternate: /some/path/to/.git/objects
	//	alternate: "/some/other path/to/.git/objects"
	scanner := bufio.NewScanner(countObjects)
	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue
		}

		var err error
		switch parts[0] {
		case "count":
			info.LooseObjects, err = strconv.ParseUint(parts[1], 10, 64)
		case "size":
			info.LooseObjectsSize, err = strconv.ParseUint(parts[1], 10, 64)
		case "in-pack":
			info.PackedObjects, err = strconv.ParseUint(parts[1], 10, 64)
		case "packs":
			info.Packfiles, err = strconv.ParseUint(parts[1], 10, 64)
		case "size-pack":
			info.PackfilesSize, err = strconv.ParseUint(parts[1], 10, 64)
		case "prune-packable":
			info.PruneableObjects, err = strconv.ParseUint(parts[1], 10, 64)
		case "garbage":
			info.Garbage, err = strconv.ParseUint(parts[1], 10, 64)
		case "size-garbage":
			info.GarbageSize, err = strconv.ParseUint(parts[1], 10, 64)
		case "alternate":
			info.Alternates = append(info.Alternates, strings.Trim(parts[1], "\" \t\n"))
		}

		if err != nil {
			return ObjectsInfo{}, fmt.Errorf("parsing %q: %w", parts[0], err)
		}
	}

	if err := scanner.Err(); err != nil {
		return ObjectsInfo{}, fmt.Errorf("scanning object info: %w", err)
	}

	if err := countObjects.Wait(); err != nil {
		return ObjectsInfo{}, fmt.Errorf("counting objects: %w", err)
	}

	if info.PackfileBitmapExists, err = HasBitmap(repoPath); err != nil {
		return ObjectsInfo{}, fmt.Errorf("checking for bitmap: %w", err)
	}

	info.CommitGraph, err = CommitGraphInfoForRepository(repoPath)
	if err != nil {
		return ObjectsInfo{}, fmt.Errorf("checking commit-graph info: %w", err)
	}

	return info, nil
}

// CountLooseObjects counts the number of loose objects in the repository. If a cutoff date is
// given, then this function will only take into account objects which are older than the given
// point in time.
func CountLooseObjects(repo *localrepo.Repo, cutoffDate time.Time) (uint64, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return 0, fmt.Errorf("getting repository path: %w", err)
	}

	var looseObjects uint64
	for i := 0; i <= 0xFF; i++ {
		entries, err := os.ReadDir(filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i)))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return 0, fmt.Errorf("reading loose object shard: %w", err)
		}

		for _, entry := range entries {
			if !isValidLooseObjectName(entry.Name()) {
				continue
			}

			entryInfo, err := entry.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}

				return 0, fmt.Errorf("reading object info: %w", err)
			}

			if entryInfo.ModTime().After(cutoffDate) {
				continue
			}

			looseObjects++
		}
	}

	return looseObjects, nil
}

func isValidLooseObjectName(s string) bool {
	for _, c := range []byte(s) {
		if strings.IndexByte("0123456789abcdef", c) < 0 {
			return false
		}
	}
	return true
}

// PackfilesInfo contains information about packfiles.
type PackfilesInfo struct {
	// Count is the number of loose objects, including stale ones.
	Count uint64 `json:"count"`
	// Size is the total size of all loose objects in bytes, including stale ones.
	Size uint64 `json:"size"`
	// GarbageCount is the number of garbage files.
	GarbageCount uint64 `json:"garbage_count"`
	// GarbageSize is the total size of all garbage files in bytes.
	GarbageSize uint64 `json:"garbage_size"`
}

// PackfilesInfoForRepository derives various information about packfiles for the given repository.
func PackfilesInfoForRepository(repo *localrepo.Repo) (PackfilesInfo, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return PackfilesInfo{}, fmt.Errorf("getting repository path: %w", err)
	}

	entries, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return PackfilesInfo{}, nil
		}

		return PackfilesInfo{}, err
	}

	var info PackfilesInfo
	for _, entry := range entries {
		entryInfo, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return PackfilesInfo{}, fmt.Errorf("getting packfile info: %w", err)
		}

		// We're overly lenient here and only verify for known prefixes. This would already
		// catch things like temporary packfiles, but it wouldn't catch other bogus files.
		// This is on purpose though because Git has grown more and more metadata-style file
		// formats, and we don't want to copy the list here.
		if !strings.HasPrefix(entry.Name(), "pack-") {
			info.GarbageCount++
			if entryInfo.Size() > 0 {
				info.GarbageSize += uint64(entryInfo.Size())
			}

			continue
		}

		if !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}

		info.Count++
		if entryInfo.Size() > 0 {
			info.Size += uint64(entryInfo.Size())
		}
	}

	return info, nil
}
