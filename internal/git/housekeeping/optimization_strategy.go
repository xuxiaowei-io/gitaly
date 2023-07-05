package housekeeping

import (
	"context"
	"math"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
)

const (
	// FullRepackCooldownPeriod is the cooldown period that needs to pass since the last full
	// repack before we consider doing another full repack.
	FullRepackCooldownPeriod = 5 * 24 * time.Hour
)

// OptimizationStrategy is an interface to determine which parts of a repository should be
// optimized.
type OptimizationStrategy interface {
	// ShouldRepackObjects determines whether the repository needs to be repacked and, if so,
	// how it should be done.
	ShouldRepackObjects(context.Context) (bool, RepackObjectsConfig)
	// ShouldPruneObjects determines whether the repository has stale objects that should be
	// pruned and, if so, how it should be done.
	ShouldPruneObjects(context.Context) (bool, PruneObjectsConfig)
	// ShouldRepackReferences determines whether the repository's references need to be
	// repacked.
	ShouldRepackReferences(context.Context) bool
	// ShouldWriteCommitGraph determines whether we need to write the commit-graph and how it
	// should be written.
	ShouldWriteCommitGraph(context.Context) (bool, WriteCommitGraphConfig)
}

// HeuristicalOptimizationStrategy is an optimization strategy that is based on a set of
// heuristics.
type HeuristicalOptimizationStrategy struct {
	gitVersion   git.Version
	info         stats.RepositoryInfo
	expireBefore time.Time
}

// NewHeuristicalOptimizationStrategy constructs a heuristicalOptimizationStrategy for the given
// repository info. It derives all data from the repository so that the heuristics used by this
// repository can be decided without further disk reads.
func NewHeuristicalOptimizationStrategy(gitVersion git.Version, info stats.RepositoryInfo) HeuristicalOptimizationStrategy {
	return HeuristicalOptimizationStrategy{
		gitVersion:   gitVersion,
		info:         info,
		expireBefore: time.Now().Add(stats.StaleObjectsGracePeriod),
	}
}

// ShouldRepackObjects checks whether the repository's objects need to be repacked. This uses a
// set of heuristics that scales with the size of the object database: the larger the repository,
// the less frequent does it get a full repack.
func (s HeuristicalOptimizationStrategy) ShouldRepackObjects(ctx context.Context) (bool, RepackObjectsConfig) {
	// If there are neither packfiles nor loose objects in this repository then there is no need
	// to repack anything.
	if s.info.Packfiles.Count == 0 && s.info.LooseObjects.Count == 0 {
		return false, RepackObjectsConfig{}
	}

	if featureflag.GeometricRepacking.IsEnabled(ctx) {
		nonCruftPackfilesCount := s.info.Packfiles.Count - s.info.Packfiles.CruftCount
		timeSinceLastFullRepack := time.Since(s.info.Packfiles.LastFullRepack)

		fullRepackCfg := RepackObjectsConfig{
			// We use the full-with-unreachable strategy to also pack all unreachable
			// objects into the packfile. This only happens for object pools though,
			// as they should never delete objects.
			Strategy: RepackObjectsStrategyFullWithUnreachable,
			// We cannot write bitmaps when there are alternates as we don't have full
			// closure of all objects in the packfile.
			WriteBitmap: len(s.info.Alternates.ObjectDirectories) == 0,
			// We rewrite all packfiles into a single one and thus change the layout
			// that was indexed by the multi-pack-index. We thus need to update it, as
			// well.
			WriteMultiPackIndex: true,
		}
		if !s.info.IsObjectPool {
			// When we don't have an object pool at hand we want to be able to expire
			// unreachable objects. We thus use cruft packs with an expiry date.
			fullRepackCfg.Strategy = RepackObjectsStrategyFullWithCruft
			fullRepackCfg.CruftExpireBefore = s.expireBefore
		}

		geometricRepackCfg := RepackObjectsConfig{
			Strategy: RepackObjectsStrategyGeometric,
			// We cannot write bitmaps when there are alternates as we don't have full
			// closure of all objects in the packfile.
			WriteBitmap: len(s.info.Alternates.ObjectDirectories) == 0,
			// We're rewriting packfiles that may be part of the multi-pack-index, so we
			// do want to update it to reflect the new layout.
			WriteMultiPackIndex: true,
		}

		// Incremental repacks only pack unreachable objects into a new pack. As we only
		// perform this kind of repack in the case where the overall repository structure
		// looks good to us we try to do use the least amount of resources to update them.
		// We thus neither update the multi-pack-index nor do we update bitmaps.
		incrementalRepackCfg := RepackObjectsConfig{
			Strategy:            RepackObjectsStrategyIncrementalWithUnreachable,
			WriteBitmap:         false,
			WriteMultiPackIndex: false,
		}

		// When alternative object directories have been modified since our last full repack
		// then we have likely joined an object pool since then. This means that we'll want
		// to perform a full repack in order to deduplicate objects that are part of the
		// object pool.
		if s.info.Alternates.LastModified.After(s.info.Packfiles.LastFullRepack) {
			return true, fullRepackCfg
		}

		// It is mandatory for us that we perform regular full repacks in repositories so
		// that we can evict objects which are unreachable into a separate cruft pack. So in
		// the case where we have more than one non-cruft packfiles and the time since our
		// last full repack is longer than the grace period we'll perform a full repack.
		//
		// This heuristic is simple on purpose: customers care about when objects will be
		// declared as unreachable and when the pruning grace period starts as it impacts
		// usage quotas. So with this simple policy we can tell customers that we evict and
		// expire unreachable objects on a regular schedule.
		//
		// On the other hand, for object pools, we also need to perform regular full
		// repacks. The reason is different though, as we don't ever delete objects from
		// pool repositories anyway.
		//
		// Geometric repacking does not take delta islands into account as it does not
		// perform a graph walk. We need proper delta islands though so that packfiles can
		// be efficiently served across forks of a repository.
		//
		// Once a full repack has been performed, the deltas will be carried forward even
		// across geometric repacks. That being said, the quality of our delta islands will
		// regress over time as new objects are pulled into the pool repository.
		//
		// So we perform regular full repacks in the repository to ensure that the delta
		// islands will be "freshened" again. If geometric repacks ever learn to take delta
		// islands into account we can get rid of this condition and only do geometric
		// repacks.
		if nonCruftPackfilesCount > 1 && timeSinceLastFullRepack > FullRepackCooldownPeriod {
			return true, fullRepackCfg
		}

		// In case both packfiles and loose objects are in a good state, but we don't yet
		// have a multi-pack-index we perform an incremental repack to generate one. We need
		// to have multi-pack-indices for the next heuristic, so it's bad if it was missing.
		if !s.info.Packfiles.MultiPackIndex.Exists {
			return true, geometricRepackCfg
		}

		// Last but not least, we also need to take into account whether new packfiles have
		// been written into the repository since our last geometric repack. This is
		// necessary so that we can enforce the geometric sequence of packfiles and to make
		// sure that the multi-pack-index tracks those new packfiles.
		//
		// To calculate this we use the number of packfiles tracked by the multi-pack index:
		// the difference between the total number of packfiles and the number of packfiles
		// tracked by the index is the amount of packfiles written since the last geometric
		// repack. As we only update the MIDX during housekeeping this metric should in
		// theory be accurate.
		//
		// Theoretically, we could perform a geometric repack whenever there is at least one
		// untracked packfile as git-repack(1) would exit early in case it finds that the
		// geometric sequence is kept. But there are multiple reasons why we want to avoid
		// this:
		//
		// - We would end up spawning git-repack(1) on almost every single repository
		//   optimization, but ideally we want to be lazy and do only as much work as is
		//   really required.
		//
		// - While we wouldn't need to repack objects in case the geometric sequence is kept
		//   anyway, we'd still need to update the multi-pack-index. This action scales with
		//   the number of overall objects in the repository.
		//
		// Instead, we use a strategy that heuristically determines whether the repository
		// has too many untracked packfiles and scale the number with the combined size of
		// all packfiles. The intent is to perform geometric repacks less often the larger
		// the repository, also because larger repositories tend to be more active, too.
		//
		// The formula we use is:
		//
		//	log(total_packfile_size) / log(1.8)
		//
		// Which gives us the following allowed number of untracked packfiles:
		//
		// -----------------------------------------------------
		// | total packfile size | allowed untracked packfiles |
		// -----------------------------------------------------
		// | none or <10MB       |  2                          |
		// | 10MB                |  3                          |
		// | 100MB               |  7                          |
		// | 500MB               | 10                          |
		// | 1GB                 | 11                          |
		// | 5GB                 | 14                          |
		// | 10GB                | 15                          |
		// | 100GB               | 19                          |
		// -----------------------------------------------------
		allowedLowerLimit := 2.0
		allowedUpperLimit := math.Log(float64(s.info.Packfiles.Size/1024/1024)) / math.Log(1.8)
		actualLimit := math.Max(allowedLowerLimit, allowedUpperLimit)

		untrackedPackfiles := s.info.Packfiles.Count - s.info.Packfiles.MultiPackIndex.PackfileCount

		if untrackedPackfiles > uint64(actualLimit) {
			return true, geometricRepackCfg
		}

		// If there are loose objects then we want to roll them up into a new packfile.
		// Loose objects naturally accumulate during day-to-day operations, e.g. when
		// executing RPCs part of the OperationsService which write objects into the repo
		// directly.
		//
		// As we have already verified that the packfile structure looks okay-ish to us, we
		// don't need to perform a geometric repack here as that could be expensive: we
		// might end up soaking up packfiles because the geometric sequence is not intact,
		// but more importantly we would end up writing the multi-pack-index and potentially
		// a bitmap. Writing these data structures introduces overhead that scales with the
		// number of objects in the repository.
		//
		// So instead, we only do an incremental repack of all loose objects, regardless of
		// their reachability. This is the cheapest we can do: we don't need to compute
		// whether objects are reachable and we don't need to update any data structures
		// that scale with the repository size.
		if s.info.LooseObjects.Count > looseObjectLimit {
			return true, incrementalRepackCfg
		}

		return false, RepackObjectsConfig{}
	}

	fullRepackCfg := RepackObjectsConfig{
		// We use the full-with-unreachable strategy to also pack all unreachable objects
		// into the packfile. This only happens for object pools though, as they should
		// never delete objects.
		//
		// Note that this is quite inefficient, as all unreachable objects will be exploded
		// into loose objects now. This is fixed in our geometric repacking strategy, where
		// we append unreachable objects to the new pack.
		Strategy: RepackObjectsStrategyFullWithLooseUnreachable,
		// We cannot write bitmaps when there are alternates as we don't have full closure
		// of all objects in the packfile.
		WriteBitmap: len(s.info.Alternates.ObjectDirectories) == 0,
		// We want to always update the multi-pack-index while we're already at it repacking
		// some of the objects.
		WriteMultiPackIndex: true,
	}
	if !s.info.IsObjectPool {
		// When we don't have an object pool at hand we want to be able to expire
		// unreachable objects. We thus use cruft packs with an expiry date.
		fullRepackCfg.Strategy = RepackObjectsStrategyFullWithCruft
		fullRepackCfg.CruftExpireBefore = s.expireBefore
	}

	incrementalRepackCfg := RepackObjectsConfig{
		Strategy: RepackObjectsStrategyIncremental,
		// We cannot write bitmaps when there are alternates as we don't have full closure
		// of all objects in the packfile.
		WriteBitmap: len(s.info.Alternates.ObjectDirectories) == 0,
		// We want to always update the multi-pack-index while we're already at it repacking
		// some of the objects.
		WriteMultiPackIndex: true,
	}

	// When alternative object directories have been modified since our last full repack then we
	// have likely joined an object pool since then. This means that we'll want to perform a
	// full repack in order to deduplicate objects that are part of the object pool.
	if s.info.Alternates.LastModified.After(s.info.Packfiles.LastFullRepack) {
		return true, fullRepackCfg
	}

	// Whenever we do an incremental repack we create a new packfile, and as a result Git may
	// have to look into every one of the packfiles to find objects. This is less efficient the
	// more packfiles we have, but we cannot repack the whole repository every time either given
	// that this may take a lot of time.
	//
	// Instead, we determine whether the repository has "too many" packfiles. "Too many" is
	// relative though: for small repositories it's fine to do full repacks regularly, but for
	// large repositories we need to be more careful. We thus use a heuristic of "repository
	// largeness": we take the total size of all packfiles, and then the maximum allowed number
	// of packfiles is `log(total_packfile_size) / log(1.3)` for normal repositories and
	// `log(total_packfile_size) / log(10.0)` for pools. This gives the following allowed number
	// of packfiles:
	//
	// -----------------------------------------------------------------------------------
	// | total packfile size | allowed packfiles for repos | allowed packfiles for pools |
	// -----------------------------------------------------------------------------------
	// | none or <10MB         | 5                           | 2                         |
	// | 10MB                  | 8                           | 2                         |
	// | 100MB                 | 17                          | 2                         |
	// | 500MB                 | 23                          | 2                         |
	// | 1GB                   | 26                          | 3                         |
	// | 5GB                   | 32                          | 3                         |
	// | 10GB                  | 35                          | 4                         |
	// | 100GB                 | 43                          | 5                         |
	// -----------------------------------------------------------------------------------
	//
	// The goal is to have a comparatively quick ramp-up of allowed packfiles as the repository
	// size grows, but then slow down such that we're effectively capped and don't end up with
	// an excessive amount of packfiles. On the other hand, pool repositories are potentially
	// reused as basis for many forks and should thus be packed much more aggressively.
	//
	// This is a heuristic and thus imperfect by necessity. We may tune it as we gain experience
	// with the way it behaves.
	lowerLimit, log := 5.0, 1.3
	if s.info.IsObjectPool {
		lowerLimit, log = 2.0, 10.0
	}

	if uint64(math.Max(lowerLimit,
		math.Log(float64(s.info.Packfiles.Size/1024/1024))/math.Log(log))) <= s.info.Packfiles.Count {
		return true, fullRepackCfg
	}

	// Most Git commands do not write packfiles directly, but instead write loose objects into
	// the object database. So while we now know that there ain't too many packfiles, we still
	// need to check whether we have too many objects.
	//
	// In this case it doesn't make a lot of sense to scale incremental repacks with the repo's
	// size: we only pack loose objects, so the time to pack them doesn't scale with repository
	// size but with the number of loose objects we have. git-gc(1) uses a threshold of 6700
	// loose objects to start an incremental repack, but one needs to keep in mind that Git
	// typically has defaults which are better suited for the client-side instead of the
	// server-side in most commands.
	//
	// In our case we typically want to ensure that our repositories are much better packed than
	// it is necessary on the client side. We thus take a much stricter limit of 1024 objects.
	if s.info.LooseObjects.Count > looseObjectLimit {
		return true, incrementalRepackCfg
	}

	// In case both packfiles and loose objects are in a good state, but we don't yet have a
	// multi-pack-index we perform an incremental repack to generate one.
	if !s.info.Packfiles.MultiPackIndex.Exists {
		return true, incrementalRepackCfg
	}

	return false, RepackObjectsConfig{}
}

// ShouldWriteCommitGraph determines whether we need to write the commit-graph and how it should be
// written.
func (s HeuristicalOptimizationStrategy) ShouldWriteCommitGraph(ctx context.Context) (bool, WriteCommitGraphConfig) {
	// If the repository doesn't have any references at all then there is no point in writing
	// commit-graphs given that it would only contain reachable objects, of which there are
	// none.
	if s.info.References.LooseReferencesCount == 0 && s.info.References.PackedReferencesSize == 0 {
		return false, WriteCommitGraphConfig{}
	}

	// When we have pruned objects in the repository then it may happen that the commit-graph
	// still refers to commits that have now been deleted. While this wouldn't typically cause
	// any issues during runtime, it may cause errors when explicitly asking for any commit that
	// does exist in the commit-graph, only. Furthermore, it causes git-fsck(1) to report that
	// the commit-graph is inconsistent.
	//
	// To fix this case we will replace the complete commit-chain when we have pruned objects
	// from the repository.
	if shouldPrune, _ := s.ShouldPruneObjects(ctx); shouldPrune {
		return true, WriteCommitGraphConfig{
			ReplaceChain: true,
		}
	}

	if commitGraphNeedsRewrite(ctx, s.info.CommitGraph) {
		return true, WriteCommitGraphConfig{
			ReplaceChain: true,
		}
	}

	// When we repacked the repository then chances are high that we have accumulated quite some
	// objects since the last time we wrote a commit-graph.
	if needsRepacking, repackCfg := s.ShouldRepackObjects(ctx); needsRepacking {
		return true, WriteCommitGraphConfig{
			// Same as with pruning: if we are repacking the repository and write cruft
			// packs with an expiry date then we may end up pruning objects. We thus
			// need to replace the commit-graph chain in that case.
			ReplaceChain: repackCfg.Strategy == RepackObjectsStrategyFullWithCruft && !repackCfg.CruftExpireBefore.IsZero(),
		}
	}

	return false, WriteCommitGraphConfig{}
}

// ShouldPruneObjects determines whether the repository has stale objects that should be pruned.
// Object pools are never pruned to not lose data in them, but otherwise we prune when we've found
// enough stale objects that might in fact get pruned.
func (s HeuristicalOptimizationStrategy) ShouldPruneObjects(context.Context) (bool, PruneObjectsConfig) {
	// Pool repositories must never prune any objects, or otherwise we may corrupt members of
	// that pool if they still refer to that object.
	if s.info.IsObjectPool {
		return false, PruneObjectsConfig{}
	}

	// When we have a number of loose objects that is older than two weeks then they have
	// surpassed the grace period and may thus be pruned.
	if s.info.LooseObjects.StaleCount <= looseObjectLimit {
		return false, PruneObjectsConfig{}
	}

	return true, PruneObjectsConfig{
		ExpireBefore: s.expireBefore,
	}
}

// ShouldRepackReferences determines whether the repository's references need to be repacked based
// on heuristics. The more references there are, the more loose referencos may exist until they are
// packed again.
func (s HeuristicalOptimizationStrategy) ShouldRepackReferences(context.Context) bool {
	// If there aren't any loose refs then there is nothing we need to do.
	if s.info.References.LooseReferencesCount == 0 {
		return false
	}

	// Packing loose references into the packed-refs file scales with the number of references
	// we're about to write. We thus decide whether we repack refs by weighing the current size
	// of the packed-refs file against the number of loose references. This is done such that we
	// do not repack too often on repositories with a huge number of references, where we can
	// expect a lot of churn in the number of references.
	//
	// As a heuristic, we repack if the number of loose references in the repository exceeds
	// `log(packed_refs_size_in_bytes/100)/log(1.15)`, which scales as following (number of refs
	// is estimated with 100 bytes per reference):
	//
	// - 1kB ~ 10 packed refs: 16 refs
	// - 10kB ~ 100 packed refs: 33 refs
	// - 100kB ~ 1k packed refs: 49 refs
	// - 1MB ~ 10k packed refs: 66 refs
	// - 10MB ~ 100k packed refs: 82 refs
	// - 100MB ~ 1m packed refs: 99 refs
	//
	// We thus allow roughly 16 additional loose refs per factor of ten of packed refs.
	//
	// This heuristic may likely need tweaking in the future, but should serve as a good first
	// iteration.
	if uint64(math.Max(16, math.Log(float64(s.info.References.PackedReferencesSize)/100)/math.Log(1.15))) > s.info.References.LooseReferencesCount {
		return false
	}

	return true
}

// EagerOptimizationStrategy is a strategy that will eagerly perform optimizations. All of the data
// structures will be optimized regardless of whether they already are in an optimal state or not.
type EagerOptimizationStrategy struct {
	info         stats.RepositoryInfo
	expireBefore time.Time
}

// NewEagerOptimizationStrategy creates a new EagerOptimizationStrategy.
func NewEagerOptimizationStrategy(info stats.RepositoryInfo) EagerOptimizationStrategy {
	return EagerOptimizationStrategy{
		info:         info,
		expireBefore: time.Now().Add(stats.StaleObjectsGracePeriod),
	}
}

// ShouldRepackObjects always instructs the caller to repack objects. The strategy will always be to
// repack all objects into a single packfile. The bitmap will be written in case the repository does
// not have any alternates.
func (s EagerOptimizationStrategy) ShouldRepackObjects(ctx context.Context) (bool, RepackObjectsConfig) {
	cfg := RepackObjectsConfig{
		WriteBitmap:         len(s.info.Alternates.ObjectDirectories) == 0,
		WriteMultiPackIndex: true,
	}

	// Object pools should generally never contain unreachable objects. However, if an object
	// pool does contain unreachable objects, they should never be deleted or we could end up
	// corrupting object pool members that depend on them. To ensure this, we disable cruft
	// packs and expiration times in this context.
	//
	// However, by disabling cruft packs Git would by default explode unreachable objects into
	// loose objects. This is inefficient as these objects cannot be deltified and thus take up
	// more space. Instead, we ask git-repack(1) to append unreachable objects to the newly
	// created packfile.
	if !s.info.IsObjectPool {
		cfg.Strategy = RepackObjectsStrategyFullWithCruft
		cfg.CruftExpireBefore = s.expireBefore
	} else {
		cfg.Strategy = RepackObjectsStrategyFullWithUnreachable
	}

	return true, cfg
}

// ShouldWriteCommitGraph always instructs the caller to write the commit-graph. The strategy will
// always be to completely rewrite the commit-graph chain.
func (s EagerOptimizationStrategy) ShouldWriteCommitGraph(context.Context) (bool, WriteCommitGraphConfig) {
	return true, WriteCommitGraphConfig{
		ReplaceChain: true,
	}
}

// ShouldPruneObjects always instructs the caller to prune objects, unless the repository is an
// object pool.
func (s EagerOptimizationStrategy) ShouldPruneObjects(context.Context) (bool, PruneObjectsConfig) {
	if s.info.IsObjectPool {
		return false, PruneObjectsConfig{}
	}

	return true, PruneObjectsConfig{
		ExpireBefore: s.expireBefore,
	}
}

// ShouldRepackReferences always instructs the caller to repack references.
func (s EagerOptimizationStrategy) ShouldRepackReferences(context.Context) bool {
	return true
}
