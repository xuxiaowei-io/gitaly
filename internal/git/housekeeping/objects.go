package housekeeping

import (
	"context"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
)

const (
	// looseObjectLimit is the limit of loose objects we accept both when doing incremental
	// repacks and when pruning objects.
	looseObjectLimit = 1024
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
}

// RepackObjects repacks objects in the given repository and updates the commit-graph. The way
// objects are repacked is determined via the RepackObjectsConfig.
func RepackObjects(ctx context.Context, repo *localrepo.Repo, cfg RepackObjectsConfig) error {
	if !cfg.FullRepack && !cfg.WriteMultiPackIndex && cfg.WriteBitmap {
		return structerr.NewInvalidArgument("cannot write packfile bitmap for an incremental repack")
	}

	var options []git.Option
	if cfg.FullRepack {
		options = append(options,
			git.Flag{Name: "-A"},
			git.Flag{Name: "--pack-kept-objects"},
			git.Flag{Name: "-l"},
		)
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
