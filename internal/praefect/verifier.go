package praefect

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// MetadataVerifier verifies the repository metadata against the actual replicas on the
// Gitaly nodes. It queries the database for replicas that haven't been verified in a given
// time and checks whether the Gitalys still have them. If a Gitaly doesn't have a replica,
// the replica's metadata record is removed and the removal logged. The repository's record
// is still left in place even if all of the replicas are lost to ensure the data loss doesn't
// go unnoticed.
type MetadataVerifier struct {
	log                  logrus.FieldLogger
	db                   glsql.Querier
	conns                Connections
	batchSize            int
	leaseDuration        time.Duration
	healthChecker        HealthChecker
	verificationInterval time.Duration
}

// NewMetadataVerifier creates a new MetadataVerifier.
func NewMetadataVerifier(
	log logrus.FieldLogger,
	db glsql.Querier,
	conns Connections,
	healthChecker HealthChecker,
	verificationInterval time.Duration,
) *MetadataVerifier {
	return &MetadataVerifier{
		log:                  log,
		db:                   db,
		conns:                conns,
		batchSize:            25,
		leaseDuration:        30 * time.Second,
		healthChecker:        healthChecker,
		verificationInterval: verificationInterval,
	}
}

type verificationJob struct {
	repositoryID   int64
	virtualStorage string
	relativePath   string
	storage        string
	replicaPath    string
}

type verificationResult struct {
	job    verificationJob
	exists bool
	error  error
}

// Run runs the metadata verifier. It keeps running until the context is canceled.
func (v *MetadataVerifier) Run(ctx context.Context, ticker helper.Ticker) error {
	defer ticker.Stop()

	for {
		ticker.Reset()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
			if err := v.run(ctx); err != nil {
				v.log.WithError(err).Error("failed a background verification run")
			}
		}
	}
}

func (v *MetadataVerifier) run(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, v.leaseDuration)
	defer cancel()

	jobs, err := v.pickJobs(ctx)
	if err != nil {
		return fmt.Errorf("pick jobs: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(len(jobs))
	results := make([]verificationResult, len(jobs))
	for i, job := range jobs {
		i, job := i, job
		go func() {
			defer wg.Done()

			exists, err := v.verify(ctx, jobs[i])
			results[i] = verificationResult{
				job:    job,
				exists: exists,
				error:  err,
			}
		}()
	}

	wg.Wait()

	return v.updateMetadata(ctx, results)
}

// logRecord is a helper type for gathering the removed replicas and logging them.
type logRecord map[string]map[string][]string

// markRemoved marks the given replica as removed.
func (r logRecord) markRemoved(virtualStorage, relativePath, storage string) {
	relativePaths, ok := r[virtualStorage]
	if !ok {
		relativePaths = map[string][]string{}
	}

	relativePaths[relativePath] = append(relativePaths[relativePath], storage)
	r[virtualStorage] = relativePaths
	sort.Strings(relativePaths[relativePath])
}

func (v *MetadataVerifier) updateMetadata(ctx context.Context, results []verificationResult) error {
	repositoryIDs := make([]int64, len(results))
	storages := make([]string, len(results))
	successfullyVerifieds := make([]bool, len(results))
	exists := make([]bool, len(results))

	logRecords := logRecord{}
	for i, result := range results {
		repositoryIDs[i] = result.job.repositoryID
		storages[i] = result.job.storage
		exists[i] = result.exists
		successfullyVerifieds[i] = result.error == nil

		if result.error != nil {
			v.log.WithFields(logrus.Fields{
				"repository_id":   result.job.repositoryID,
				"replica_path":    result.job.replicaPath,
				"virtual_storage": result.job.virtualStorage,
				"storage":         result.job.storage,
				"relative_path":   result.job.relativePath,
				logrus.ErrorKey:   result.error,
			}).Error("failed to verify replica's existence")
		} else if !result.exists {
			logRecords.markRemoved(result.job.virtualStorage, result.job.relativePath, result.job.storage)
		}
	}

	if len(logRecords) > 0 {
		v.log.WithField("replicas", logRecords).Info("removing metadata records of non-existent replicas")
	}

	_, err := v.db.ExecContext(ctx, `
WITH results AS (
	SELECT repository_id, storage, successfully_verified, exists
	FROM (
		SELECT unnest($1::bigint[]) AS repository_id,
	           unnest($2::text[]) AS storage,
	           unnest($3::bool[]) as successfully_verified,
	           unnest($4::bool[]) AS exists
	) AS results
	JOIN (
		SELECT repository_id
		FROM repositories
		WHERE repository_id = ANY($1::bigint[])
		FOR UPDATE
	) AS lock_repositories USING (repository_id)
),

release_leases AS (
	UPDATE storage_repositories
	SET verification_leased_until = NULL,
	    verified_at = CASE WHEN successfully_verified THEN now() ELSE verified_at END
	FROM results
	WHERE storage_repositories.repository_id = results.repository_id
	AND   storage_repositories.storage = results.storage
)

DELETE FROM storage_repositories
USING results
WHERE storage_repositories.repository_id = results.repository_id
AND   storage_repositories.storage       = results.storage
AND   successfully_verified AND NOT exists
	`, repositoryIDs, storages, successfullyVerifieds, exists)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	return nil
}

func (v *MetadataVerifier) pickJobs(ctx context.Context) ([]verificationJob, error) {
	var healthyVirtualStorages, healthyStorages []string
	for virtualStorage, storages := range v.healthChecker.HealthyNodes() {
		for _, storage := range storages {
			healthyVirtualStorages = append(healthyVirtualStorages, virtualStorage)
			healthyStorages = append(healthyStorages, storage)
		}
	}

	rows, err := v.db.QueryContext(ctx, `
WITH to_verify AS (
	SELECT repository_id, relative_path, replica_path, virtual_storage, storage
	FROM (
		SELECT repository_id, storage
		FROM storage_repositories
		WHERE ( verified_at IS NULL OR verified_at < now() - $1 * '1 millisecond'::interval )
        AND verification_leased_until IS NULL
		ORDER BY verified_at NULLS FIRST
		FOR NO KEY UPDATE SKIP LOCKED
	) AS need_verification
	JOIN repositories USING (repository_id)
	JOIN (
        SELECT unnest($4::text[]) AS virtual_storage,
	           unnest($5::text[]) AS storage
	) AS healthy_storages USING (virtual_storage, storage)
	LIMIT $2
),

acquire_leases AS (
	UPDATE storage_repositories
    SET verification_leased_until = now() + $3 * '1 millisecond'::interval
	FROM to_verify
	WHERE storage_repositories.repository_id = to_verify.repository_id
	AND   storage_repositories.storage       = to_verify.storage
)

SELECT repository_id, replica_path, virtual_storage, relative_path, storage
FROM to_verify
	`, v.verificationInterval.Milliseconds(), v.batchSize, v.leaseDuration.Milliseconds(), healthyVirtualStorages, healthyStorages)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var jobs []verificationJob
	for rows.Next() {
		var job verificationJob
		if err := rows.Scan(&job.repositoryID, &job.replicaPath, &job.virtualStorage, &job.relativePath, &job.storage); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	return jobs, nil
}

func (v *MetadataVerifier) verify(ctx context.Context, job verificationJob) (bool, error) {
	conn, ok := v.conns[job.virtualStorage][job.storage]
	if !ok {
		return false, fmt.Errorf("no connection to %q/%q", job.virtualStorage, job.storage)
	}

	resp, err := gitalypb.NewRepositoryServiceClient(conn).RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: &gitalypb.Repository{
			StorageName:  job.storage,
			RelativePath: job.replicaPath,
		},
	})
	if err != nil {
		return false, err
	}

	return resp.Exists, nil
}
