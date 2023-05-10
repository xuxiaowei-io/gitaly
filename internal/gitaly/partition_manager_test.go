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
		// expectedState contains the partitions by their storages and their pending transaction count at
		// the end of the step.
		expectedState map[string]map[string]uint
		// expectedError is the error expected to be returned when beginning the transaction.
		expectedError error
	}

	// commit calls Commit on a transaction.
	type commit struct {
		// transactionID identifies the transaction to commit.
		transactionID int
		// ctx is the context used when `Commit()` gets invoked.
		ctx context.Context
		// expectedState contains the partitions by their storages and their pending transaction count at
		// the end of the step.
		expectedState map[string]map[string]uint
		// expectedError is the error that is expected to be returned when committing the transaction.
		expectedError error
	}

	// rollback calls Rollback on a transaction.
	type rollback struct {
		// transactionID identifies the transaction to rollback.
		transactionID int
		// expectedState contains the partitions by their storages and their pending transaction count at
		// the end of the step.
		expectedState map[string]map[string]uint
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

	// blockOnPartitionShutdown checks if any partitions are currently in the process of
	// shutting down. If some are, the function waits for the shutdown process to complete before
	// continuing. This is required in order to accurately validate partition state.
	blockOnPartitionShutdown := func(t *testing.T, pm *PartitionManager) {
		t.Helper()

		var waitFor []chan struct{}
		for _, sp := range pm.storages {
			sp.mu.Lock()
			for _, ptn := range sp.partitions {
				if ptn.shuttingDown {
					waitFor = append(waitFor, ptn.shutdown)
				}
			}
			sp.mu.Unlock()
		}

		for _, shutdown := range waitFor {
			<-shutdown
		}
	}

	// checkExpectedState validates that the partition manager contains the correct partitions and
	// associated transaction count at the point of execution.
	checkExpectedState := func(t *testing.T, cfg config.Cfg, partitionManager *PartitionManager, expectedState map[string]map[string]uint) {
		t.Helper()

		actualState := map[string]map[string]uint{}
		for storageName, storageMgr := range partitionManager.storages {
			for partitionKey, partition := range storageMgr.partitions {
				if actualState[storageName] == nil {
					actualState[storageName] = map[string]uint{}
				}

				actualState[storageName][partitionKey] = partition.pendingTransactionCount
			}
		}

		if expectedState == nil {
			expectedState = map[string]map[string]uint{}
		}

		require.Equal(t, expectedState, actualState)
	}

	setupRepository := func(t *testing.T, cfg config.Cfg) repo.GitRepo {
		t.Helper()

		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})

		return repo
	}

	// transactionData holds relevant data for each transaction created during a testcase.
	type transactionData struct {
		txn        *Transaction
		storageMgr *storageManager
		ptn        *partition
	}

	type setupData struct {
		steps steps
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, cfg config.Cfg) setupData
	}{
		{
			desc: "transaction committed for single repository",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
							},
						},
						commit{},
					},
				}
			},
		},
		{
			desc: "two transactions committed for single repository sequentially",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
							},
						},
						commit{
							transactionID: 1,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
							},
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 2,
								},
							},
						},
						commit{
							transactionID: 1,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repoA := setupRepository(t, cfg)
				repoB := setupRepository(t, cfg)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repoA,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repoA.GetStorageName(), repoA.GetRelativePath()): 1,
								},
							},
						},
						begin{
							transactionID: 2,
							repo:          repoB,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repoA.GetStorageName(), repoA.GetRelativePath()): 1,
									getPartitionKey(repoB.GetStorageName(), repoB.GetRelativePath()): 1,
								},
							},
						},
						commit{
							transactionID: 1,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repoB.GetStorageName(), repoB.GetRelativePath()): 1,
								},
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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
							},
						},
						rollback{},
					},
				}
			},
		},
		{
			desc: "starting transaction failed due to cancelled context",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

				stepCtx, cancel := context.WithCancel(ctx)
				cancel()

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

				return setupData{
					steps: steps{
						begin{
							repo: repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

				return setupData{
					steps: steps{
						begin{
							transactionID: 1,
							repo:          repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
							},
						},
						stopPartition{
							transactionID: 1,
						},
						begin{
							transactionID: 2,
							repo:          repo,
							expectedState: map[string]map[string]uint{
								"default": {
									getPartitionKey(repo.GetStorageName(), repo.GetRelativePath()): 1,
								},
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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

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
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repo := setupRepository(t, cfg)

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

			cfg := testcfg.Build(t)

			cmdFactory, clean, err := git.NewExecCommandFactory(cfg)
			require.NoError(t, err)
			t.Cleanup(clean)

			catfileCache := catfile.NewCache(cfg)
			t.Cleanup(catfileCache.Stop)

			localRepoFactory := localrepo.NewFactory(config.NewLocator(cfg), cmdFactory, catfileCache)

			setup := tc.setup(t, cfg)

			// Create some existing content in the staging directory so we can assert it gets removed and
			// recreated.
			for _, storage := range cfg.Storages {
				require.NoError(t,
					os.MkdirAll(
						filepath.Join(stagingDirectoryPath(storage.Path), "existing-content"),
						perm.PrivateDir,
					),
				)
			}

			partitionManager, err := NewPartitionManager(cfg.Storages, localRepoFactory, logrus.StandardLogger())
			require.NoError(t, err)
			defer func() {
				partitionManager.Stop()
				for _, storage := range cfg.Storages {
					// Assert all staging directories have been emptied at the end.
					testhelper.RequireDirectoryState(t, storage.Path, "staging", testhelper.DirectoryState{
						"/staging": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
					})
				}
			}()

			for _, storage := range cfg.Storages {
				// Assert the existing content in the staging directory was removed.
				testhelper.RequireDirectoryState(t, storage.Path, "staging", testhelper.DirectoryState{
					"/staging": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
				})
			}

			// openTransactionData holds references to all transactions and its associated partition
			// created during the testcase.
			openTransactionData := map[int]*transactionData{}

			var partitionManagerStopped bool
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

					storageMgr := partitionManager.storages[step.repo.GetStorageName()]
					storageMgr.mu.Lock()
					ptn := storageMgr.partitions[getPartitionKey(step.repo.GetStorageName(), step.repo.GetRelativePath())]
					storageMgr.mu.Unlock()

					blockOnPartitionShutdown(t, partitionManager)
					checkExpectedState(t, cfg, partitionManager, step.expectedState)

					openTransactionData[step.transactionID] = &transactionData{
						txn:        txn,
						storageMgr: storageMgr,
						ptn:        ptn,
					}
				case commit:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction committed before being started")

					data := openTransactionData[step.transactionID]

					commitCtx := ctx
					if step.ctx != nil {
						commitCtx = step.ctx
					}

					require.ErrorIs(t, data.txn.Commit(commitCtx), step.expectedError)

					blockOnPartitionShutdown(t, partitionManager)
					checkExpectedState(t, cfg, partitionManager, step.expectedState)
				case rollback:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction rolled back before being started")

					data := openTransactionData[step.transactionID]
					require.NoError(t, data.txn.Rollback())

					blockOnPartitionShutdown(t, partitionManager)
					checkExpectedState(t, cfg, partitionManager, step.expectedState)
				case stopPartition:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction manager stopped before being started")

					data := openTransactionData[step.transactionID]
					data.ptn.stop()

					blockOnPartitionShutdown(t, partitionManager)
				case finalizeTransaction:
					require.Contains(t, openTransactionData, step.transactionID, "test error: transaction finalized before being started")

					data := openTransactionData[step.transactionID]

					data.storageMgr.transactionFinalizerFactory(data.ptn)()
				case stopManager:
					require.False(t, partitionManagerStopped, "test error: partition manager already stopped")
					partitionManagerStopped = true

					partitionManager.Stop()
				}
			}
		})
	}
}
