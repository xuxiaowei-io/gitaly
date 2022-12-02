package stats

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
)

// StaleObjectsGracePeriod is time delta that is used to indicate cutoff wherein an object would be
// considered old. Currently this is set to being 2 weeks (2 * 7days * 24hours).
const StaleObjectsGracePeriod = -14 * 24 * time.Hour

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

	return objectsInfo.LooseObjects.Count, nil
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
	// LooseObjects contains information about loose objects.
	LooseObjects LooseObjectsInfo `json:"loose_objects"`
	// Packfiles contains information about packfiles.
	Packfiles PackfilesInfo `json:"packfiles"`
	// CommitGraph contains information about the repository's commit-graphs.
	CommitGraph CommitGraphInfo `json:"commit_graph"`
	// Alternates is the list of absolute paths of alternate object databases this repository is
	// connected to.
	Alternates []string `json:"alternates"`
}

// ObjectsInfoForRepository computes the ObjectsInfo for a repository.
func ObjectsInfoForRepository(ctx context.Context, repo *localrepo.Repo) (ObjectsInfo, error) {
	var info ObjectsInfo
	var err error

	repoPath, err := repo.Path()
	if err != nil {
		return ObjectsInfo{}, err
	}

	info.LooseObjects, err = LooseObjectsInfoForRepository(repo, time.Now().Add(StaleObjectsGracePeriod))
	if err != nil {
		return ObjectsInfo{}, fmt.Errorf("counting loose objects: %w", err)
	}

	info.Packfiles, err = PackfilesInfoForRepository(repo)
	if err != nil {
		return ObjectsInfo{}, fmt.Errorf("counting packfiles: %w", err)
	}

	info.CommitGraph, err = CommitGraphInfoForRepository(repoPath)
	if err != nil {
		return ObjectsInfo{}, fmt.Errorf("checking commit-graph info: %w", err)
	}

	info.Alternates, err = readAlternates(repo)
	if err != nil {
		return ObjectsInfo{}, fmt.Errorf("reading alterantes: %w", err)
	}

	return info, nil
}

// LooseObjectsInfo contains information about loose objects.
type LooseObjectsInfo struct {
	// Count is the number of loose objects.
	Count uint64 `json:"count"`
	// Size is the total size of all loose objects in bytes.
	Size uint64 `json:"size"`
	// StaleCount is the number of stale loose objects when taking into account the specified cutoff
	// date.
	StaleCount uint64 `json:"stale_count"`
	// StaleSize is the total size of stale loose objects when taking into account the specified
	// cutoff date.
	StaleSize uint64 `json:"stale_size"`
	// GarbageCount is the number of garbage files in the loose-objects shards.
	GarbageCount uint64 `json:"garbage_count"`
	// GarbageSize is the total size of garbage in the loose-objects shards.
	GarbageSize uint64 `json:"garbage_size"`
}

// LooseObjectsInfoForRepository derives information about loose objects in the repository. If a
// cutoff date is given, then this function will only take into account objects which are older than
// the given point in time.
func LooseObjectsInfoForRepository(repo *localrepo.Repo, cutoffDate time.Time) (LooseObjectsInfo, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return LooseObjectsInfo{}, fmt.Errorf("getting repository path: %w", err)
	}

	var info LooseObjectsInfo
	for i := 0; i <= 0xFF; i++ {
		entries, err := os.ReadDir(filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i)))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return LooseObjectsInfo{}, fmt.Errorf("reading loose object shard: %w", err)
		}

		for _, entry := range entries {
			entryInfo, err := entry.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}

				return LooseObjectsInfo{}, fmt.Errorf("reading object info: %w", err)
			}

			if !isValidLooseObjectName(entry.Name()) {
				info.GarbageCount++
				info.GarbageSize += uint64(entryInfo.Size())
				continue
			}

			// Note: we don't `continue` here as we count stale objects into the total
			// number of objects.
			if entryInfo.ModTime().Before(cutoffDate) {
				info.StaleCount++
				info.StaleSize += uint64(entryInfo.Size())
			}

			info.Count++
			info.Size += uint64(entryInfo.Size())
		}
	}

	return info, nil
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
	// HasBitmap indicates whether the packfiles have a bitmap.
	HasBitmap bool `json:"has_bitmap"`
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

	if info.HasBitmap, err = HasBitmap(repoPath); err != nil {
		return PackfilesInfo{}, fmt.Errorf("checking for bitmap: %w", err)
	}

	return info, nil
}

func readAlternates(repo *localrepo.Repo) ([]string, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return nil, fmt.Errorf("getting repository path: %w", err)
	}

	contents, err := os.ReadFile(filepath.Join(repoPath, "objects", "info", "alternates"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading alternates: %w", err)
	}

	relativeAlternatePaths := strings.Split(text.ChompBytes(contents), "\n")
	alternatePaths := make([]string, 0, len(relativeAlternatePaths))
	for _, relativeAlternatePath := range relativeAlternatePaths {
		if filepath.IsAbs(relativeAlternatePath) {
			alternatePaths = append(alternatePaths, relativeAlternatePath)
		} else {
			alternatePaths = append(alternatePaths, filepath.Join(repoPath, "objects", relativeAlternatePath))
		}
	}

	return alternatePaths, nil
}
