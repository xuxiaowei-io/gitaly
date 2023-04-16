package gitaly

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	repo "gitlab.com/gitlab-org/gitaly/v16/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestPartitionManager(t *testing.T) {
	umask := perm.GetUmask()

	t.Parallel()

	ctx := testhelper.Context(t)

	// steps defines execution steps in a test. Each test case can define multiple steps to exercise
	// more complex behavior.
	type steps []any

	// begin calls Begin on the TransactionManager to start a new transaction.
	type begin struct {
		// transactionID is the identifier given to the transaction created. This is used to identify
		// the transaction in later steps.
		transactionID int
		// ctx is the context used when `Begin()` gets invoked.
		ctx context.Context
		// repo is the repository that the transaction belongs to.
		repo repo.GitRepo
		// expectedState contains the expected repositories and their pending transaction count at
		// the end of the step.
		expectedState map[string]uint
		// expectedError is the error expected to be returned when beginning the transaction.
		expectedError error
	}

	// commit calls Commit on a transaction.
	type commit struct {
		// transactionID identifies the transaction to commit.
		transactionID int
		// ctx is the context used when `Commit()` gets invoked.
		ctx context.Context
		// expectedState contains the expected repositories and their pending transaction count at
		// the end of the step.
		expectedState map[string]uint
		// expectedError is the error that is expected to be returned when committing the transaction.
		expectedError error
	}

	// rollback calls Rollback on a transaction.
	type rollback struct {
		// transactionID identifies the transaction to rollback.
		transactionID int
		// expectedState contains the expected repositories and their pending transaction count at
		// the end of the step.
		expectedState map[string]uint
	}

	// stopPartition stops the transaction manager for the specified repository. This is done to
	// simulate failures.
	type stopPartition struct {
		// transactionID identifies the transaction manager associated with the transaction to stop.
		transactionID int
	}

	// finalizeTransaction runs the transaction finalizer for the specified repository. This is used
	// to simulate finalizers executing after a transaction manager has been stopped.
	type finalizeTransaction struct {
		// transactionID identifies the transaction to finalize.
		transactionID int
	}

	// stopManager stops the partition manager. This is done to simulate errors for transactions
	// being processed without a running partition manager.
	type stopManager struct{}

	// blockOnPartitionShutdown checks if the specified partition is currently in the process of
	// shutting down. If it is, the function waits for the shutdown process to complete before
	// continuing. This is required in order to accurately validate partition state.
	blockOnPartitionShutdown := func(t *testing.T, ptn *partition) {
		t.Helper()

		if ptn != nil && ptn.shuttingDown {
			<-ptn.shutdown
		}
	}

	// checkExpectedState validates that the partition manager contains the correct partitions and
	// associated transaction count at the point of execution.
	checkExpectedState := func(t *testing.T, partitionManager *PartitionManager, expectedState map[string]uint) {
		t.Helper()

		require.Equal(t, len(expectedState), len(partitionManager.partitions))
		for k, v := range expectedState {
			partition, ok := partitionManager.partitions[k]
			require.True(t, ok)
			require.Equal(t, v, partition.pendingTransactionCount)
		}
	}

	cfg := testcfg.Build(t)

	setupRepository := func(t *testing.T) repo.GitRepo {
		t.Helper()

		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		return repo
	}

	localRepoFactory := func(repo repo.GitRepo) *localrepo.Repo {
		cmdFactory, clean, err := git.NewExecCommandFactory(cfg)
		require.NoError(t, err)
		t.Cleanup(clean)

		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)

		return localrepo.New(
			config.NewLocator(cfg),
			cmdFactory,
			catfileCache,
			repo,
		)
	}

	// transactionData holds relevant data for each transaction created during a testcase.
	type transactionData struct {
		txn *Transaction
		ptn *partition
	}

	type setupData struct {
		steps steps
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "transaction committed for single repository",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						commit{},
					},
				}
			},
		},
		{
			desc: "two transactions committed for single repository sequentially",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						commit{
							transactionID: 1,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						commit{
							transactionID: 2,
						},
					},
				}
			},
		},
		{
			desc: "two transactions committed for single repository in parallel",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 2,
							},
						},
						commit{
							transactionID: 1,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						commit{
							transactionID: 2,
						},
					},
				}
			},
		},
		{
			desc: "transaction committed for two repositories",
			setup: func(t *testing.T) setupData {
				repoA := setupRepository(t)
				repoB := setupRepository(t)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repoA,
							expectedState: map[string]uint{
								getRepositoryID(repoA): 1,
							},
						},
						begin{
							transactionID: 2,
							repo:          repoB,
							expectedState: map[string]uint{
								getRepositoryID(repoA): 1,
								getRepositoryID(repoB): 1,
							},
						},
						commit{
							transactionID: 1,
							expectedState: map[string]uint{
								getRepositoryID(repoB): 1,
							},
						},
						commit{
							transactionID: 2,
						},
					},
				}
			},
		},
		{
			desc: "transaction rolled back for single repository",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						rollback{},
					},
				}
			},
		},
		{
			desc: "starting transaction failed due to cancelled context",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				stepCtx, cancel := context.WithCancel(ctx)
				cancel()

				return setupData{
					steps: steps{
						begin{
							ctx:           stepCtx,
							repo:          repo,
							expectedError: context.Canceled,
						},
					},
				}
			},
		},
		{
			desc: "committing transaction failed due to cancelled context",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				stepCtx, cancel := context.WithCancel(ctx)
				cancel()

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						commit{
							ctx:           stepCtx,
							expectedError: context.Canceled,
						},
					},
				}
			},
		},
		{
			desc: "committing transaction failed due to stopped transaction manager",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						stopPartition{},
						commit{
							expectedError: ErrTransactionProcessingStopped,
						},
					},
				}
			},
		},
		{
			desc: "transaction from previous transaction manager finalized after new manager started",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						stopPartition{
							transactionID: 1,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[string]uint{
								getRepositoryID(repo): 1,
							},
						},
						finalizeTransaction{
							transactionID: 1,
						},
						commit{
							transactionID: 2,
						},
					},
				}
			},
		},
		{
			desc: "transaction started after partition manager stopped",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						stopManager{},
						begin{
							repo:          repo,
							expectedError: ErrPartitionManagerStopped,
						},
					},
				}
			},
		},
		{
			desc: "multiple transactions started after partition manager stopped",
			setup: func(t *testing.T) setupData {
				repo := setupRepository(t)

				return setupData{
					steps: steps{
						stopManager{},
						begin{
							transactionID: 1,
							repo:          repo,
							expectedError: ErrPartitionManagerStopped,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedError: ErrPartitionManagerStopped,
						},
					},
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			database, err := OpenDatabase(t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			stagingDir := filepath.Join(t.TempDir(), "staging")
			require.NoError(t, os.Mkdir(stagingDir, perm.PrivateDir))

			partitionManager := NewPartitionManager(database, cfg.Storages, localRepoFactory, logrus.StandardLogger(), stagingDir)
			defer func() {
				partitionManager.Stop()
				// Assert all staging directories have been removed.
				testhelper.RequireDirectoryState(t, stagingDir, "", testhelper.DirectoryState{
					"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
				})
			}()

			// openTransactionData holds references to all transactions and its associated partition
			// created during the testcase.
			openTransactionData := map[int]*transactionData{}

			for _, step := range setup.steps {
				switch step := step.(type) {
				case begin:
					require.NotContains(t, openTransactionData, step.transactionID, "test error: transaction id reused in begin")

					beginCtx := ctx
					if step.ctx != nil {
						beginCtx = step.ctx
					}

					txn, err := partitionManager.Begin(beginCtx, step.repo)
					require.Equal(t, step.expectedError, err)

					partitionManager.mu.Lock()
					ptn := partitionManager.partitions[getRepositoryID(step.repo)]
					partitionManager.mu.Unlock()

					blockOnPartitionShutdown(t, ptn)
					checkExpectedState(t, partitionManager, step.expectedState)

					openTransactionData[step.transactionID] = &transactionData{
						txn: txn,
						ptn: ptn,
					}
				case commit:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction committed before being started")

					data := openTransactionData[step.transactionID]

					commitCtx := ctx
					if step.ctx != nil {
						commitCtx = step.ctx
					}

					require.ErrorIs(t, data.txn.Commit(commitCtx), step.expectedError)

					blockOnPartitionShutdown(t, data.ptn)
					checkExpectedState(t, partitionManager, step.expectedState)
				case rollback:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction rolled back before being started")

					data := openTransactionData[step.transactionID]
					require.NoError(t, data.txn.Rollback())

					blockOnPartitionShutdown(t, data.ptn)
					checkExpectedState(t, partitionManager, step.expectedState)
				case stopPartition:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction manager stopped before being started")

					data := openTransactionData[step.transactionID]
					data.ptn.stop()

					blockOnPartitionShutdown(t, data.ptn)
				case finalizeTransaction:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction finalized before being started")

					data := openTransactionData[step.transactionID]
					partitionManager.transactionFinalizerFactory(data.ptn)()
				case stopManager:
					require.False(t, partitionManager.stopped, "test error: partition manager already stopped")

					partitionManager.Stop()
				}
			}
		})
	}
}
