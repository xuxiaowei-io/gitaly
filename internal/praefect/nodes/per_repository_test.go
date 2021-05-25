// +build postgres

package nodes

import (
	"context"
	"database/sql"
	"testing"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func setHealthyNodes(t testing.TB, ctx context.Context, db glsql.Querier, healthyNodes map[string]map[string][]string) {
	var praefects, virtualStorages, storages []string
	for praefect, virtualStors := range healthyNodes {
		for virtualStorage, stors := range virtualStors {
			for _, storage := range stors {
				praefects = append(praefects, praefect)
				virtualStorages = append(virtualStorages, virtualStorage)
				storages = append(storages, storage)
			}
		}
	}

	_, err := db.ExecContext(ctx, `
WITH clear_previous_checks AS ( DELETE FROM node_status )

INSERT INTO node_status (praefect_name, shard_name, node_name, last_contact_attempt_at, last_seen_active_at)
SELECT
	unnest($1::text[]) AS praefect_name,
	unnest($2::text[]) AS shard_name,
	unnest($3::text[]) AS node_name,
	NOW() AS last_contact_attempt_at,
	NOW() AS last_seen_active_at
ON CONFLICT (praefect_name, shard_name, node_name) DO UPDATE SET
	last_contact_attempt_at = NOW(),
	last_seen_active_at = NOW()
		`,
		pq.StringArray(praefects),
		pq.StringArray(virtualStorages),
		pq.StringArray(storages),
	)
	require.NoError(t, err)
}

func TestPerRepositoryElector(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	type storageRecord struct {
		generation int
		assigned   bool
	}

	type state map[string]map[string]map[string]storageRecord

	type steps []struct {
		healthyNodes   map[string][]string
		error          error
		primaryIsOneOf []string
	}

	for _, tc := range []struct {
		desc            string
		state           state
		startingPrimary string
		steps           steps
		existingJobs    []datastore.ReplicationEvent
	}{
		{
			desc: "elects the most up to date storage",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-2": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-1"},
				},
			},
		},
		{
			desc: "elects the most up to date healthy storage",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-2": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-2"},
				},
			},
		},
		{
			desc:            "doesn't re-elect if primary is healthy",
			startingPrimary: "gitaly-1",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0},
						"gitaly-2": {generation: 1},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-1"},
				},
			},
		},
		{
			desc: "no valid primary",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					error: ErrNoPrimary,
				},
			},
		},
		{
			desc: "random healthy node on the latest generation",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0},
						"gitaly-2": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-1", "gitaly-2"},
				},
			},
		},
		{
			desc: "fails over to up to date healthy note",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-2": {generation: 1},
						"gitaly-3": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-1"},
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-2"},
				},
			},
		},
		{
			desc: "fails over to most up to date healthy note",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-3": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-1"},
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-3"},
				},
			},
		},
		{
			desc: "fails over only to assigned nodes when assignments are set",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 2, assigned: true},
						"gitaly-2": {generation: 1, assigned: true},
						"gitaly-3": {generation: 2, assigned: false},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-1"},
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-2"},
				},
			},
		},
		{
			desc: "demotes primary when no valid candidates",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1, assigned: true},
						"gitaly-2": {generation: 1, assigned: false},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primaryIsOneOf: []string{"gitaly-1"},
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					error: ErrNoPrimary,
				},
			},
		},
		{
			desc: "doesnt elect replicas with delete_replica in ready state",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					error: ErrNoPrimary,
				},
			},
		},
		{
			desc: "doesnt elect replicas with delete_replica in in_progress state",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateInProgress,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					error: ErrNoPrimary,
				},
			},
		},
		{
			desc: "doesnt elect replicas with delete_replica in failed state",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateFailed,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					error: ErrNoPrimary,
				},
			},
		},
		{
			desc: "irrelevant delete_replica jobs are ignored",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "wrong-virtual-storage",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "wrong-relative-path",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "wrong-storage",
					},
				},
				{
					State: datastore.JobStateCancelled,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateDead,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateCompleted,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					primaryIsOneOf: []string{"gitaly-1"},
				},
			},
		},
		{
			desc: "repository does not exist",
			steps: steps{
				{
					error: commonerr.NewRepositoryNotFoundError("virtual-storage-1", "relative-path-1"),
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db := getDB(t)

			rs := datastore.NewPostgresRepositoryStore(db, nil)
			for virtualStorage, relativePaths := range tc.state {
				for relativePath, storages := range relativePaths {
					var startingPrimary *string
					if tc.startingPrimary != "" {
						startingPrimary = &tc.startingPrimary
					}

					_, err := db.ExecContext(ctx,
						`INSERT INTO repositories (virtual_storage, relative_path, "primary") VALUES ($1, $2, $3)`,
						virtualStorage, relativePath, startingPrimary,
					)
					require.NoError(t, err)

					for storage, record := range storages {
						require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, storage, record.generation))

						if record.assigned {
							_, err := db.ExecContext(ctx, `
								INSERT INTO repository_assignments VALUES ($1, $2, $3)
							`, virtualStorage, relativePath, storage)
							require.NoError(t, err)
						}
					}
				}
			}

			for _, event := range tc.existingJobs {
				_, err := db.ExecContext(ctx,
					"INSERT INTO replication_queue (state, job) VALUES ($1, $2)",
					event.State, event.Job,
				)
				require.NoError(t, err)
			}

			previousPrimary := tc.startingPrimary
			for _, step := range tc.steps {
				runElection := func(tx *sql.Tx) (string, *logrus.Entry) {
					setHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect-0": step.healthyNodes})

					logger, hook := test.NewNullLogger()
					elector := NewPerRepositoryElector(logrus.NewEntry(logger), tx)

					primary, err := elector.GetPrimary(ctx, "virtual-storage-1", "relative-path-1")
					require.Equal(t, step.error, err)
					require.Less(t, len(hook.Entries), 2)

					var entry *logrus.Entry
					if len(hook.Entries) == 1 {
						entry = &hook.Entries[0]
					}

					require.NoError(t, tx.Commit())

					return primary, entry
				}

				// Run every step with two concurrent transactions to ensure two Praefect's running
				// election at the same time do not elect the primary multiple times. We begin both
				// transactions at the same time to ensure they have the same snapshot of the
				// database. The second transaction would be blocked until the first transaction commits.
				// To verify concurrent election runs do not elect the primary multiple times, we assert
				// the second transaction performed no changes and the primary is what the first run elected
				// it to be.
				txFirst, err := db.Begin()
				require.NoError(t, err)
				defer txFirst.Rollback()

				txSecond, err := db.Begin()
				require.NoError(t, err)
				defer txSecond.Rollback()

				primary, logEntry := runElection(txFirst)
				if primary != "" {
					require.Contains(t, step.primaryIsOneOf, primary)
				} else {
					require.Empty(t, step.primaryIsOneOf, "expected no primary but got %q", primary)
				}

				if previousPrimary != primary {
					assert.NotNil(t, logEntry)
					assert.Equal(t, "primary node changed", logEntry.Message)
					assert.Equal(t, logrus.Fields{
						"component":        "PerRepositoryElector",
						"virtual_storage":  "virtual-storage-1",
						"relative_path":    "relative-path-1",
						"current_primary":  primary,
						"previous_primary": previousPrimary,
					}, logEntry.Data)
				} else {
					require.Nil(t, logEntry)
				}

				// Run the second election on the same database snapshot. This should result in no changes.
				// Running this prior to the first transaction committing would block.
				secondPrimary, secondLogEntry := runElection(txSecond)
				require.Equal(t, primary, secondPrimary)
				require.Nil(t, secondLogEntry)

				previousPrimary = primary
			}
		})
	}
}
