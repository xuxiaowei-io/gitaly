package housekeeping

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
)

const (
	// looseObjectLimit is the limit of loose objects we accept both when doing incremental
	// repacks and when pruning objects.
	looseObjectLimit = 1024
	// rfc2822DateFormat is the date format that Git typically uses for dates.
	rfc2822DateFormat = "Mon Jan 02 2006 15:04:05 -0700"
)

// RepackObjectsConfig is configuration for RepackObjects.
type RepackObjectsConfig struct {
	// FullRepack determines whether all reachable objects should be repacked into a single
	// packfile. This is much less efficient than doing incremental repacks, which only soak up
	// all loose objects into a new packfile.
	FullRepack bool
	// WriteBitmap determines whether reachability bitmaps should be written or not. There is no
	// reason to set this to `false`, except for legacy compatibility reasons with existing RPC
	// behaviour
	WriteBitmap bool
	// WriteMultiPackIndex determines whether a multi-pack index should be written or not.
	WriteMultiPackIndex bool
	// WriteCruftPack determines whether unreachable objects shall be written into cruft packs.
	WriteCruftPack bool
	// CruftExpireBefore determines the cutoff date before which unreachable cruft objects shall
	// be expired and thus deleted.
	CruftExpireBefore time.Time
}

// RepackObjects repacks objects in the given repository and updates the commit-graph. The way
// objects are repacked is determined via the RepackObjectsConfig.
func RepackObjects(ctx context.Context, repo *localrepo.Repo, cfg RepackObjectsConfig) error {
	repoPath, err := repo.Path()
	if err != nil {
		return err
	}

	if !cfg.FullRepack && !cfg.WriteMultiPackIndex && cfg.WriteBitmap {
		return structerr.NewInvalidArgument("cannot write packfile bitmap for an incremental repack")
	}

	if !cfg.FullRepack && cfg.WriteCruftPack {
		return structerr.NewInvalidArgument("cannot write cruft packs for an incremental repack")
	}

	if !cfg.WriteCruftPack && !cfg.CruftExpireBefore.IsZero() {
		return structerr.NewInvalidArgument("cannot expire cruft objects when not writing cruft packs")
	}

	var options []git.Option
	if cfg.FullRepack {
		options = append(options,
			git.Flag{Name: "--pack-kept-objects"},
			git.Flag{Name: "-l"},
		)

		if cfg.WriteCruftPack {
			options = append(options, git.Flag{Name: "--cruft"})
		} else {
			options = append(options, git.Flag{Name: "-A"})
		}

		if !cfg.CruftExpireBefore.IsZero() {
			options = append(options, git.ValueFlag{
				Name:  "--cruft-expiration",
				Value: cfg.CruftExpireBefore.Format(rfc2822DateFormat),
			})
		}

		// When we have performed a full repack we're updating the "full-repack-timestamp"
		// file. This is done so that we can tell when we have last performed a full repack
		// in a repository. This information can be used by our heuristics to effectively
		// rate-limit the frequency of full repacks.
		//
		// Note that we write the file _before_ actually writing the new pack, which means
		// that even if the full repack fails, we would still pretend to have done it. This
		// is done intentionally, as the likelihood for huge repositories to fail during a
		// full repack is comparatively high. So if we didn't update the timestamp in case
		// of a failure we'd potentially busy-spin trying to do a full repack.
		if err := os.WriteFile(filepath.Join(repoPath, stats.FullRepackTimestampFilename), nil, perm.PrivateFile); err != nil {
			return fmt.Errorf("updating timestamp: %w", err)
		}
	}

	if cfg.WriteMultiPackIndex {
		options = append(options, git.Flag{Name: "--write-midx"})
	}

	if err := repo.ExecAndWait(ctx, git.Command{
		Name: "repack",
		Flags: append([]git.Option{
			git.Flag{Name: "-d"},
		}, options...),
	}, git.WithConfig(GetRepackGitConfig(ctx, repo, cfg.WriteBitmap)...)); err != nil {
		return err
	}

	return nil
}

// GetRepackGitConfig returns configuration suitable for Git commands which write new packfiles.
func GetRepackGitConfig(ctx context.Context, repo repository.GitRepo, bitmap bool) []git.ConfigPair {
	config := []git.ConfigPair{
		{Key: "repack.useDeltaIslands", Value: "true"},
		{Key: "repack.writeBitmaps", Value: strconv.FormatBool(bitmap)},
		{Key: "pack.writeBitmapLookupTable", Value: "true"},
	}

	if stats.IsPoolRepository(repo) {
		config = append(config,
			git.ConfigPair{Key: "pack.island", Value: git.ObjectPoolRefNamespace + "/he(a)ds"},
			git.ConfigPair{Key: "pack.island", Value: git.ObjectPoolRefNamespace + "/t(a)gs"},
			git.ConfigPair{Key: "pack.islandCore", Value: "a"},
		)
	} else {
		config = append(config,
			git.ConfigPair{Key: "pack.island", Value: "r(e)fs/heads"},
			git.ConfigPair{Key: "pack.island", Value: "r(e)fs/tags"},
			git.ConfigPair{Key: "pack.islandCore", Value: "e"},
		)
	}

	return config
}

// PruneObjectsConfig determines which objects should be pruned in PruneObjects.
type PruneObjectsConfig struct {
	// ExpireBefore controls the grace period after which unreachable objects shall be pruned.
	// An unreachable object must be older than the given date in order to be considered for
	// deletion.
	ExpireBefore time.Time
}

// PruneObjects prunes loose objects from the repository that are already packed or which are
// unreachable and older than the configured expiry date.
func PruneObjects(ctx context.Context, repo *localrepo.Repo, cfg PruneObjectsConfig) error {
	if err := repo.ExecAndWait(ctx, git.Command{
		Name: "prune",
		Flags: []git.Option{
			// By default, this prunes all unreachable objects regardless of when they
			// have last been accessed. This opens us up for races when there are
			// concurrent commands which are just at the point of writing objects into
			// the repository, but which haven't yet updated any references to make them
			// reachable.
			//
			// To avoid this race, we use a grace window that can be specified by the
			// caller so that we only delete objects that are older than this grace
			// window.
			git.ValueFlag{Name: "--expire", Value: cfg.ExpireBefore.Format(rfc2822DateFormat)},
		},
	}); err != nil {
		return fmt.Errorf("executing prune: %w", err)
	}

	return nil
}
