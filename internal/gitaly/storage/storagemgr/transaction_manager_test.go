package storagemgr

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func validCustomHooks(tb testing.TB) []byte {
	tb.Helper()

	var hooks bytes.Buffer
	writer := tar.NewWriter(&hooks)
	require.NoError(tb, writer.WriteHeader(&tar.Header{
		Name: "custom_hooks/pre-receive",
		Size: int64(len("hook content")),
		Mode: int64(fs.ModePerm),
	}))
	_, err := writer.Write([]byte("hook content"))
	require.NoError(tb, err)

	require.NoError(tb, writer.WriteHeader(&tar.Header{
		Name: "custom_hooks/private-dir/",
		Mode: int64(perm.PrivateDir),
	}))

	require.NoError(tb, writer.WriteHeader(&tar.Header{
		Name: "custom_hooks/private-dir/private-file",
		Size: int64(len("private content")),
		Mode: int64(perm.PrivateFile),
	}))
	_, err = writer.Write([]byte("private content"))
	require.NoError(tb, err)

	require.NoError(tb, writer.WriteHeader(&tar.Header{Name: "ignored_file"}))
	require.NoError(tb, writer.WriteHeader(&tar.Header{Name: "ignored_dir/ignored_file"}))

	require.NoError(tb, writer.Close())
	return hooks.Bytes()
}

// writePack writes a pack file and its index into the destination.
func writePack(tb testing.TB, cfg config.Cfg, packFile []byte, destinationPack string) {
	tb.Helper()

	require.NoError(tb, os.WriteFile(destinationPack, packFile, fs.ModePerm))
	gittest.ExecOpts(tb, cfg,
		gittest.ExecConfig{Stdin: bytes.NewReader(packFile)},
		"index-pack", "--object-format="+gittest.DefaultObjectHash.Format, destinationPack,
	)
}

// packFileDirectoryEntry returns a DirectoryEntry that parses content as a pack file and asserts that the
// set of objects in the pack file matches the expected objects.
func packFileDirectoryEntry(cfg config.Cfg, expectedObjects []git.ObjectID) testhelper.DirectoryEntry {
	sortObjects := func(objects []git.ObjectID) {
		sort.Slice(objects, func(i, j int) bool {
			return objects[i] < objects[j]
		})
	}

	sortObjects(expectedObjects)

	return testhelper.DirectoryEntry{
		Mode:    perm.SharedReadOnlyFile,
		Content: expectedObjects,
		ParseContent: func(tb testing.TB, path string, content []byte) any {
			tb.Helper()

			tempDir := tb.TempDir()
			// Initialize a temporary repository where to write the pack. The cat file invocation for listing
			// the objects needs to run within a repository, and it would otherwise use the developer's repository.
			// If the object format doesn't match with the pack files in the test, things fail.
			gittest.Exec(tb, cfg, "init", "--object-format="+gittest.DefaultObjectHash.Format, "--bare", tempDir)
			writePack(tb, cfg, content, filepath.Join(tempDir, "objects", "pack", "content.pack"))

			actualObjects := gittest.ListObjects(tb, cfg, tempDir)
			sortObjects(actualObjects)

			return actualObjects
		},
	}
}

// indexFileDirectoryEntry returns a DirectoryEntry that asserts the given pack file index is valid.
func indexFileDirectoryEntry(cfg config.Cfg) testhelper.DirectoryEntry {
	return testhelper.DirectoryEntry{
		Mode: perm.SharedReadOnlyFile,
		ParseContent: func(tb testing.TB, path string, content []byte) any {
			tb.Helper()

			// Verify the index is valid.
			gittest.Exec(tb, cfg, "verify-pack", "--object-format="+gittest.DefaultObjectHash.Format, "-v", path)
			// As we already verified the index is valid, we don't care about the actual contents.
			return nil
		},
	}
}

// reverseIndexFileDirectoryEntry returns a DirectoryEntry that asserts the given pack file reverse index is valid.
func reverseIndexFileDirectoryEntry(cfg config.Cfg) testhelper.DirectoryEntry {
	return testhelper.DirectoryEntry{
		Mode: perm.SharedReadOnlyFile,
		ParseContent: func(tb testing.TB, path string, content []byte) any {
			tb.Helper()

			// We cannot run git-verify-pack(1) directly against a reverse index file, so we validate the
			// header to fail fast when possible. If this passes but the '.rev' file is invalid, the
			// '.idx' check will fail.
			buf := make([]byte, 4)
			f, err := os.Open(path)
			require.NoError(tb, err)
			defer testhelper.MustClose(tb, f)

			_, err = f.Read(buf)
			require.NoError(tb, err)
			require.Equal(tb, []byte{'R', 'I', 'D', 'X'}, buf)

			return nil
		},
	}
}

func TestTransactionManager(t *testing.T) {
	t.Parallel()

	umask := testhelper.Umask()
	ctx := testhelper.Context(t)

	type testCommit struct {
		OID  git.ObjectID
		Pack []byte
	}

	type testCommits struct {
		First     testCommit
		Second    testCommit
		Third     testCommit
		Diverging testCommit
	}

	type testSetup struct {
		Config            config.Cfg
		CommandFactory    git.CommandFactory
		RepositoryFactory localrepo.Factory
		ObjectHash        git.ObjectHash
		NonExistentOID    git.ObjectID
		Commits           testCommits
	}

	setupTest := func(t *testing.T, relativePath string) testSetup {
		t.Helper()

		cfg := testcfg.Build(t)

		repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           relativePath,
		})

		firstCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		secondCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommitOID))
		thirdCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(secondCommitOID))
		divergingCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommitOID), gittest.WithMessage("diverging commit"))

		cmdFactory := gittest.NewCommandFactory(t, cfg)
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)

		logger := testhelper.NewLogger(t)
		locator := config.NewLocator(cfg)
		localRepo := localrepo.New(
			logger,
			locator,
			cmdFactory,
			catfileCache,
			repo,
		)

		objectHash, err := localRepo.ObjectHash(ctx)
		require.NoError(t, err)

		hasher := objectHash.Hash()
		_, err = hasher.Write([]byte("content does not matter"))
		require.NoError(t, err)
		nonExistentOID, err := objectHash.FromHex(hex.EncodeToString(hasher.Sum(nil)))
		require.NoError(t, err)

		packCommit := func(oid git.ObjectID) []byte {
			t.Helper()

			var pack bytes.Buffer
			require.NoError(t,
				localRepo.PackObjects(ctx, strings.NewReader(oid.String()), &pack),
			)

			return pack.Bytes()
		}

		return testSetup{
			Config:            cfg,
			ObjectHash:        objectHash,
			CommandFactory:    cmdFactory,
			RepositoryFactory: localrepo.NewFactory(logger, locator, cmdFactory, catfileCache),
			NonExistentOID:    nonExistentOID,
			Commits: testCommits{
				First: testCommit{
					OID:  firstCommitOID,
					Pack: packCommit(firstCommitOID),
				},
				Second: testCommit{
					OID:  secondCommitOID,
					Pack: packCommit(secondCommitOID),
				},
				Third: testCommit{
					OID:  thirdCommitOID,
					Pack: packCommit(thirdCommitOID),
				},
				Diverging: testCommit{
					OID:  divergingCommitOID,
					Pack: packCommit(divergingCommitOID),
				},
			},
		}
	}

	// A clean repository is setup for each test. We build a setup ahead of the tests here once to
	// get deterministic commit IDs, relative path and object hash we can use to build the declarative
	// test cases.
	relativePath := gittest.NewRepositoryName(t)
	setup := setupTest(t, relativePath)

	// errSimulatedCrash is used in the tests to simulate a crash at a certain point during
	// TransactionManager.Run execution.
	errSimulatedCrash := errors.New("simulated crash")

	// partitionID is the partition ID used in the tests for the TransactionManager.
	const partitionID partitionID = 1

	type testHooks struct {
		// BeforeApplyLogEntry is called before a log entry is applied to the repository.
		BeforeApplyLogEntry hookFunc
		// BeforeAppendLogEntry is called before a log entry is appended to the log.
		BeforeAppendLogEntry hookFunc
		// BeforeDeleteLogEntry is called before a log entry is deleted.
		BeforeDeleteLogEntry hookFunc
		// BeforeReadAppliedLSN is invoked before the applied LSN is read.
		BeforeReadAppliedLSN hookFunc
		// BeforeStoreAppliedLSN is invoked before the applied LSN is stored.
		BeforeStoreAppliedLSN hookFunc
		// WaitForTransactionsWhenClosing waits for a in-flight to finish before returning
		// from Run.
		WaitForTransactionsWhenClosing bool
	}

	// StartManager starts a TransactionManager.
	type StartManager struct {
		// Hooks contains the hook functions that are configured on the TransactionManager. These allow
		// for better synchronization.
		Hooks testHooks
		// ExpectedError is the expected error to be raised from the manager's Run. Panics are converted
		// to errors and asserted to match this as well.
		ExpectedError error
		// ModifyStorage allows modifying the storage prior to the manager starting. This
		// may be necessary to test some states that can be reached from hard crashes
		// but not during the tests.
		ModifyStorage func(tb testing.TB, cfg config.Cfg, storagePath string)
	}

	// CloseManager closes a TransactionManager.
	type CloseManager struct{}

	// AssertManager asserts whether the manager has closed and Run returned. If it has, it asserts the
	// error matched the expected. If the manager has exited with an error, AssertManager must be called
	// or the test case fails.
	type AssertManager struct {
		// ExpectedError is the error TransactionManager's Run method is expected to return.
		ExpectedError error
	}

	// Begin calls Begin on the TransactionManager to start a new transaction.
	type Begin struct {
		// TransactionID is the identifier given to the transaction created. This is used to identify
		// the transaction in later steps.
		TransactionID int
		// RelativePath is the relative path of the repository this transaction is operating on.
		RelativePath string
		// SnapshottedRelativePaths are the relative paths of the repositories to include in the snapshot
		// in addition to the target repository.
		SnapshottedRelativePaths []string
		// ReadOnly indicates whether this is a read-only transaction.
		ReadOnly bool
		// Context is the context to use for the Begin call.
		Context context.Context
		// ExpectedSnapshot is the expected LSN this transaction should read the repsoitory's state at.
		ExpectedSnapshotLSN LSN
		// ExpectedError is the error expected to be returned from the Begin call.
		ExpectedError error
	}

	// CreateRepository creates the transaction's repository..
	type CreateRepository struct {
		// TransactionID is the transaction for which to create the repository.
		TransactionID int
		// DefaultBranch is the default branch to set in the repository.
		DefaultBranch git.ReferenceName
		// References are the references to create in the repository.
		References map[git.ReferenceName]git.ObjectID
		// Packs are the objects that are written into the repository.
		Packs [][]byte
		// CustomHooks are the custom hooks to write into the repository.
		CustomHooks []byte
		// Alternate links the given relative path as the repository's alternate.
		Alternate string
	}

	// Commit calls Commit on a transaction.
	type Commit struct {
		// TransactionID identifies the transaction to commit.
		TransactionID int
		// Context is the context to use for the Commit call.
		Context context.Context
		// ExpectedError is the error that is expected to be returned when committing the transaction.
		// If ExpectedError is a function with signature func(tb testing.TB, actualErr error), it is
		// ran instead to asser the error.
		ExpectedError any

		// SkipVerificationFailures sets the verification failure handling for this commit.
		SkipVerificationFailures bool
		// ReferenceUpdates are the reference updates to commit.
		ReferenceUpdates ReferenceUpdates
		// QuarantinedPacks are the packs to include in the quarantine directory of the transaction.
		QuarantinedPacks [][]byte
		// DefaultBranchUpdate is the default branch update to commit.
		DefaultBranchUpdate *DefaultBranchUpdate
		// CustomHooksUpdate is the custom hooks update to commit.
		CustomHooksUpdate *CustomHooksUpdate
		// CreateRepository creates the repository on commit.
		CreateRepository bool
		// DeleteRepository deletes the repository on commit.
		DeleteRepository bool
		// IncludeObjects includes objects in the transaction's logged pack.
		IncludeObjects []git.ObjectID
		// UpdateAlternate updates the repository's alternate when set.
		UpdateAlternate *alternateUpdate
	}

	// RecordInitialReferenceValues calls RecordInitialReferenceValues on a transaction.
	type RecordInitialReferenceValues struct {
		// TransactionID identifies the transaction to prepare the reference updates on.
		TransactionID int
		// InitialValues are the initial values to record.
		InitialValues map[git.ReferenceName]git.ObjectID
	}

	// UpdateReferences calls UpdateReferences on a transaction.
	type UpdateReferences struct {
		// TransactionID identifies the transaction to update references on.
		TransactionID int
		// ReferenceUpdates are the reference updates to make.
		ReferenceUpdates ReferenceUpdates
	}

	// Rollback calls Rollback on a transaction.
	type Rollback struct {
		// TransactionID identifies the transaction to rollback.
		TransactionID int
		// ExpectedError is the error that is expected to be returned when rolling back the transaction.
		ExpectedError error
	}

	// Prune prunes all unreferenced objects from the repository.
	type Prune struct {
		// ExpectedObjects are the object expected to exist in the repository after pruning.
		ExpectedObjects []git.ObjectID
	}

	// RemoveRepository removes the repository from the disk. It must be run with the TransactionManager
	// closed.
	type RemoveRepository struct{}

	// RepositoryAssertion asserts a given transaction's view of repositories matches the expected.
	type RepositoryAssertion struct {
		// TransactionID identifies the transaction whose snapshot to assert.
		TransactionID int
		// Repositories is the expected state of the repositories the transaction sees. The
		// key is the repository's relative path and the value describes its expected state.
		Repositories RepositoryStates
	}

	// StateAssertions models an assertion of the entire state managed by the TransactionManager.
	type StateAssertion struct {
		// Database is the expected state of the database.
		Database DatabaseState
		// Directory is the expected state of the manager's state directory in the repository.
		Directory testhelper.DirectoryState
		// Repositories is the expected state of the repositories in the storage. The key is
		// the repository's relative path and the value describes its expected state.
		Repositories RepositoryStates
	}

	// steps defines execution steps in a test. Each test case can define multiple steps to exercise
	// more complex behavior.
	type steps []any

	type testCase struct {
		desc          string
		steps         steps
		expectedState StateAssertion
	}

	testCases := []testCase{
		{
			desc: "invalid reference aborts the entire transaction",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/../main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
				},
			},
		},
		{
			desc: "continues processing after aborting due to an invalid reference",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/../main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "create reference",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "create reference with existing reference lock",
			steps: steps{
				StartManager{
					ModifyStorage: func(_ testing.TB, _ config.Cfg, storagePath string) {
						err := os.WriteFile(filepath.Join(storagePath, relativePath, "refs", "heads", "main.lock"), []byte{}, 0o666)
						require.NoError(t, err)
					},
				},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "delete packed reference with existing packed-refs.lock",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				CloseManager{},
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)
						// Pack the reference and create a stale lockfile for it.
						gittest.Exec(tb, cfg, "-C", repoPath, "pack-refs", "--all")

						// Add packed-refs.lock. The reference deletion will fail if this
						// isn't cleaned up.
						require.NoError(t, os.WriteFile(
							filepath.Join(repoPath, "packed-refs.lock"),
							[]byte{},
							perm.PrivateFile,
						))
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
			},
		},
		{
			desc: "delete packed reference with existing packed-refs.new",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				CloseManager{},
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						repoPath := filepath.Join(storagePath, relativePath)
						// Pack the reference and create a stale lockfile for it.
						gittest.Exec(tb, cfg, "-C", repoPath, "pack-refs", "--all")

						// Add packed-refs.new. The reference deletion will fail if this
						// isn't cleaned up.
						require.NoError(t, os.WriteFile(
							filepath.Join(repoPath, "packed-refs.new"),
							[]byte{},
							perm.PrivateFile,
						))
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
			},
		},
		{
			desc: "create a file-directory reference conflict different transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: updateref.FileDirectoryConflictError{
						ExistingReferenceName:    "refs/heads/parent",
						ConflictingReferenceName: "refs/heads/parent/child",
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/parent", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "create a file-directory reference conflict in same transaction",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent":       {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/parent/child": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: updateref.InTransactionConflictError{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
				},
			},
		},
		{
			desc: "file-directory conflict aborts the transaction with verification failures skipped",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":         {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/parent":       {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/parent/child": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: updateref.InTransactionConflictError{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
				},
			},
		},
		{
			desc: "delete file-directory conflict in different transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.ZeroOID},
					},
					ExpectedError: updateref.FileDirectoryConflictError{
						ExistingReferenceName:    "refs/heads/parent/child",
						ConflictingReferenceName: "refs/heads/parent",
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/parent/child", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "delete file-directory conflict in same transaction",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/parent":       {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.ZeroOID},
					},
					ExpectedError: updateref.InTransactionConflictError{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
				},
			},
		},
		{
			desc: "file-directory conflict solved in the same transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				UpdateReferences{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/parent/child", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "create a branch to a non-commit object",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						// The error should abort the entire transaction.
						"refs/heads/branch-1": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/branch-2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.EmptyTreeOID},
					},
					ExpectedError: updateref.NonCommitObjectError{
						ReferenceName: "refs/heads/branch-2",
						ObjectID:      setup.ObjectHash.EmptyTreeOID.String(),
					},
				},
			},
		},
		{
			desc: "create a tag to a non-commit object",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/tags/v1.0.0": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.EmptyTreeOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/tags/v1.0.0", Target: setup.ObjectHash.EmptyTreeOID.String()}},
					},
				},
			},
		},
		{
			desc: "create a reference to non-existent object",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.NonExistentOID},
					},
					ExpectedError: updateref.NonExistentObjectError{
						ReferenceName: "refs/heads/main",
						ObjectID:      setup.NonExistentOID.String(),
					},
				},
			},
		},
		{
			desc: "create reference ignoring verification failure",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:            2,
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/non-conflicting", Target: setup.Commits.Second.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "create reference that already exists",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.ObjectHash.ZeroOID,
						ActualOID:     setup.Commits.First.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "create reference no-op",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.ObjectHash.ZeroOID,
						ActualOID:     setup.Commits.First.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "update reference",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "update reference ignoring verification failures",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:            2,
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.Third.OID},
						"refs/heads/non-conflicting": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Third.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/non-conflicting", Target: setup.Commits.Third.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "update reference with incorrect old tip",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.Third.OID},
						"refs/heads/non-conflicting": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Third.OID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.Second.OID,
						ActualOID:     setup.Commits.First.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/non-conflicting", Target: setup.Commits.First.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "update non-existent reference",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.Third.OID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.Second.OID,
						ActualOID:     setup.ObjectHash.ZeroOID,
					},
				},
			},
		},
		{
			desc: "update reference no-op",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "delete reference",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
			},
		},
		{
			desc: "delete symbolic reference pointing to non-existent reference",
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						gittest.Exec(tb, cfg,
							"-C", filepath.Join(storagePath, relativePath),
							"symbolic-ref", "refs/heads/symbolic", "refs/heads/main",
						)
					},
				},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/symbolic": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
			},
		},
		{
			desc: "delete symbolic reference",
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						gittest.Exec(tb, cfg,
							"-C", filepath.Join(storagePath, relativePath),
							"symbolic-ref", "refs/heads/symbolic", "refs/heads/main",
						)
					},
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/symbolic": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "update symbolic reference",
			steps: steps{
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						gittest.Exec(tb, cfg,
							"-C", filepath.Join(storagePath, relativePath),
							"symbolic-ref", "refs/heads/symbolic", "refs/heads/main",
						)
					},
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/symbolic": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							// The symbolic reference should be converted to a normal reference if it is
							// updated.
							{Name: "refs/heads/symbolic", Target: setup.Commits.Second.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "delete reference ignoring verification failures",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:            2,
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.Commits.Second.OID, NewOID: setup.ObjectHash.ZeroOID},
						"refs/heads/non-conflicting": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "delete reference with incorrect old tip",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.Commits.Second.OID, NewOID: setup.ObjectHash.ZeroOID},
						"refs/heads/non-conflicting": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.Second.OID,
						ActualOID:     setup.Commits.First.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/non-conflicting", Target: setup.Commits.First.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "delete non-existent reference",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.First.OID,
						ActualOID:     setup.ObjectHash.ZeroOID,
					},
				},
			},
		},
		{
			desc: "delete reference no-op",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
			},
		},
		{
			desc: "update reference multiple times successfully in a transaction",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				UpdateReferences{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				UpdateReferences{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "update reference multiple times fails due to wrong initial value",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				UpdateReferences{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
				UpdateReferences{
					ReferenceUpdates: ReferenceUpdates{
						// The old oid should be ignored since there's already a recorded initial value for the
						// reference.
						"refs/heads/main": {NewOID: setup.Commits.Third.OID},
					},
				},
				Commit{
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.First.OID,
						ActualOID:     setup.ObjectHash.ZeroOID,
					},
				},
			},
		},
		{
			desc: "recording initial value of a reference stages no updates",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				RecordInitialReferenceValues{
					InitialValues: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
			},
		},
		{
			desc: "update reference with non-existent initial value",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				RecordInitialReferenceValues{
					InitialValues: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.ObjectHash.ZeroOID,
					},
				},
				UpdateReferences{
					// The old oid is ignored as the references old value was already recorded.
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {NewOID: setup.Commits.First.OID},
					},
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "update reference with the zero oid initial value",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				RecordInitialReferenceValues{
					TransactionID: 2,
					InitialValues: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.ObjectHash.ZeroOID,
					},
				},
				UpdateReferences{
					TransactionID: 2,
					// The old oid is ignored as the references old value was already recorded.
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {NewOID: setup.Commits.Second.OID},
					},
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "update reference with the correct initial value",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				RecordInitialReferenceValues{
					TransactionID: 2,
					InitialValues: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
				},
				UpdateReferences{
					TransactionID: 2,
					// The old oid is ignored as the references old value was already recorded.
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {NewOID: setup.Commits.Second.OID},
					},
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "update reference with the incorrect initial value",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				RecordInitialReferenceValues{
					TransactionID: 2,
					InitialValues: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.Third.OID,
					},
				},
				UpdateReferences{
					TransactionID: 2,
					// The old oid is ignored as the references old value was already recorded.
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {NewOID: setup.Commits.Second.OID},
					},
				},
				Commit{
					TransactionID: 2,
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.Third.OID,
						ActualOID:     setup.Commits.First.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "initial value is set on the first update",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				UpdateReferences{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				RecordInitialReferenceValues{
					InitialValues: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.Third.OID,
					},
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						References: []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "set custom hooks successfully",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:     2,
					CustomHooksUpdate: &CustomHooksUpdate{},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal": {Mode: fs.ModeDir | perm.PrivateDir},
				},
				Repositories: RepositoryStates{
					relativePath: {
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: fs.ModeDir | perm.PrivateDir},
						},
					},
				},
			},
		},
		{
			desc: "rejects invalid custom hooks",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: []byte("corrupted tar"),
					},
					ExpectedError: func(tb testing.TB, actualErr error) {
						require.ErrorContains(tb, actualErr, "stage hooks: extract hooks: waiting for tar command completion: exit status")
					},
				},
			},
		},
		{
			desc: "reapplying custom hooks works",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal": {Mode: fs.ModeDir | perm.PrivateDir},
				},
				Repositories: RepositoryStates{
					relativePath: {
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: fs.ModeDir | perm.PrivateDir},
							"/pre-receive": {
								Mode:    umask.Mask(fs.ModePerm),
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: fs.ModeDir | perm.PrivateDir},
							"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
						},
					},
				},
			},
		},
		{
			desc: "hook index is correctly determined from log and disk",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:     2,
					CustomHooksUpdate: &CustomHooksUpdate{},
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				CloseManager{},
				StartManager{},
				Begin{
					TransactionID:       4,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				Rollback{
					TransactionID: 4,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal": {Mode: fs.ModeDir | perm.PrivateDir},
				},
				Repositories: RepositoryStates{
					relativePath: {
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: fs.ModeDir | perm.PrivateDir},
						},
					},
				},
			},
		},
		{
			desc: "continues processing after reference verification failure",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.First.OID,
						ActualOID:     setup.ObjectHash.ZeroOID,
					},
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "continues processing after a restart",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AssertManager{},
				CloseManager{},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "continues processing after restarting after a reference verification failure",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.First.OID,
						ActualOID:     setup.ObjectHash.ZeroOID,
					},
				},
				CloseManager{},
				StartManager{},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "continues processing after failing to store log index",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookCtx hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "recovers from the write-ahead log on start up",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.closeManager()
						},
					},
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
					},
				},
			},
		},
		{
			desc: "reference verification fails after recovering logged writes",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.closeManager()
						},
					},
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   setup.Commits.Second.OID,
						ActualOID:     setup.Commits.First.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "begin returns if context is canceled before initialization",
			steps: steps{
				Begin{
					RelativePath: relativePath,
					Context: func() context.Context {
						ctx, cancel := context.WithCancel(ctx)
						cancel()
						return ctx
					}(),
					ExpectedError: context.Canceled,
				},
			},
			expectedState: StateAssertion{
				// Manager is not started up so no state is initialized.
				Directory: testhelper.DirectoryState{},
			},
		},
		{
			desc: "commit returns if transaction processing stops before admission",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				CloseManager{},
				Commit{
					ExpectedError: ErrTransactionProcessingStopped,
				},
			},
		},
		func() testCase {
			ctx, cancel := context.WithCancel(ctx)
			return testCase{
				desc: "commit returns if context is canceled after admission",
				steps: steps{
					StartManager{
						Hooks: testHooks{
							BeforeAppendLogEntry: func(hookCtx hookContext) {
								// Cancel the context used in Commit
								cancel()
							},
						},
					},
					Begin{
						RelativePath: relativePath,
					},
					Commit{
						Context: ctx,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						},
						ExpectedError: context.Canceled,
					},
				},
				expectedState: StateAssertion{
					Database: DatabaseState{
						string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
					},
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
							References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
						},
					},
				},
			}
		}(),
		{
			desc: "commit returns if transaction processing stops before transaction acceptance",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeAppendLogEntry: func(hookContext hookContext) { hookContext.closeManager() },
						// This ensures we are testing the context cancellation errors being unwrapped properly
						// to an ErrTransactionProcessingStopped instead of hitting the general case when
						// runDone is closed.
						WaitForTransactionsWhenClosing: true,
					},
				},
				Begin{
					RelativePath: relativePath,
				},
				CloseManager{},
				Commit{
					ExpectedError: ErrTransactionProcessingStopped,
				},
			},
		},
		{
			desc: "commit returns if transaction processing stops after transaction acceptance",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.closeManager()
						},
					},
				},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyLogEntry(partitionID, 1)): &gitalypb.LogEntry{
						RelativePath: relativePath,
						ReferenceTransactions: []*gitalypb.LogEntry_ReferenceTransaction{
							{
								Changes: []*gitalypb.LogEntry_ReferenceTransaction_Change{
									{
										ReferenceName: []byte("refs/heads/main"),
										NewOid:        []byte(setup.Commits.First.OID),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "update default branch with existing branch",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/branch2",
						References: []git.Reference{
							{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "update default branch with new branch created in same transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/main":    {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/branch2",
						References: []git.Reference{
							{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "update default branch with invalid reference name",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/../main",
					},
					ExpectedError: InvalidReferenceFormatError{
						ReferenceName: "refs/heads/../main",
					},
				},
			},
		},
		{
			desc: "update default branch to point to a non-existent reference name",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/non-existent",
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/non-existent",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					},
				},
			},
		},
		{
			desc: "update default branch to point non-refs prefixed reference",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "other/non-existent",
					},
					ExpectedError: InvalidReferenceFormatError{ReferenceName: "other/non-existent"},
				},
			},
		},
		{
			desc: "update default branch to point to reference being deleted in the same transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/branch2",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "update default branch with existing branch and other modifications",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/branch2",
						References: []git.Reference{
							{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "update default branch fails before storing log index",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookCtx hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/branch2",
						References: []git.Reference{
							{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
							{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()},
						},
					},
				},
			},
		},
		{
			desc: "read snapshots include committed data",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				RepositoryAssertion{
					TransactionID: 1,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
						},
					},
				},
				RepositoryAssertion{
					TransactionID: 2,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
						},
					},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				// Transaction 2 is isolated from the changes made by transaction 1. It does not see the
				// committed changes.
				RepositoryAssertion{
					TransactionID: 2,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
						},
					},
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				// Transaction 3 is should see the new changes as it began after transaction 1 was committed.
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
							CustomHooks: testhelper.DirectoryState{
								"/": {Mode: fs.ModeDir | perm.PrivateDir},
								"/pre-receive": {
									Mode:    umask.Mask(fs.ModePerm),
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: fs.ModeDir | perm.PrivateDir},
								"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
							},
						},
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
				Begin{
					TransactionID:       4,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				Rollback{
					TransactionID: 3,
				},
				Begin{
					TransactionID:       5,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 4,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.Third.OID},
					},
					CustomHooksUpdate: &CustomHooksUpdate{},
				},
				Begin{
					TransactionID:       6,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 3,
				},
				Rollback{
					TransactionID: 5,
				},
				Rollback{
					TransactionID: 6,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal": {Mode: fs.ModeDir | perm.PrivateDir},
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.Third.OID.String()},
						},
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: fs.ModeDir | perm.PrivateDir},
						},
					},
				},
			},
		},
		{
			desc: "pack file includes referenced commit",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: updateref.NonExistentObjectError{
						ReferenceName: "refs/heads/main",
						ObjectID:      setup.Commits.First.OID.String(),
					},
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file includes unreachable objects depended upon",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				// Point main to the first commit so the second one is unreachable.
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.First.OID},
					},
				},
				AssertManager{},
				CloseManager{},
				StartManager{
					// Crash the manager before the third transaction is applied. This allows us to
					// prune before it is applied to ensure the pack file contains all necessary commits.
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Third.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Third.Pack},
					ExpectedError:    ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				// Prune so the unreachable commits have been removed prior to the third log entry being
				// applied.
				Prune{
					ExpectedObjects: []git.ObjectID{
						setup.ObjectHash.EmptyTreeOID,
						setup.Commits.First.OID,
					},
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
					"/wal/3":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/3/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/3/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/3/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.Third.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file reapplying works",
			steps: steps{
				Prune{},
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					ExpectedError:    ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file missing referenced commit",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					ExpectedError:    localrepo.BadObjectError{ObjectID: setup.Commits.Second.OID},
				},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "pack file missing intermediate commit",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Third.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Third.Pack},
					ExpectedError:    localrepo.ObjectReadError{ObjectID: setup.Commits.Second.OID},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},

				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/main",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file only",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal": {Mode: fs.ModeDir | perm.PrivateDir},
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "transaction includes an unreachable object with dependencies",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					QuarantinedPacks: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
						setup.Commits.Third.Pack,
					},
					IncludeObjects: []git.ObjectID{setup.Commits.Second.OID},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					},
				},
			},
		},
		{
			desc: "transaction includes multiple unreachable objects",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					QuarantinedPacks: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
						setup.Commits.Diverging.Pack,
					},
					IncludeObjects: []git.ObjectID{setup.Commits.Second.OID, setup.Commits.Diverging.OID},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Diverging.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Diverging.OID,
						},
					},
				},
			},
		},
		{
			desc: "transaction includes a missing object",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					QuarantinedPacks: [][]byte{
						setup.Commits.First.Pack,
					},
					IncludeObjects: []git.ObjectID{setup.Commits.Second.OID},
					ExpectedError: localrepo.BadObjectError{
						ObjectID: setup.Commits.Second.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "pack file with deletions",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
				},
			},
		},
		{
			desc: "pack file applies with dependency concurrently deleted",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
				AssertManager{},
				Prune{},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/dependant": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
					// The transaction fails to apply as we are not yet maintaining internal references
					// to the old tips of concurrently deleted references. This causes the prune step to
					// remove the object this the pack file depends on.
					//
					// For now, keep the test case to assert the behavior. We'll fix this in a later MR.
					ExpectedError: localrepo.ObjectReadError{
						ObjectID: setup.Commits.First.OID,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "pack files without log entries are cleaned up after a crash",
			steps: steps{
				StartManager{
					// The manager cleans up pack files if a committing fails. Since we can't
					// hard kill the manager and it will still run the deferred clean up functions,
					// we have to test the behavior by manually creating a stale pack here.
					//
					// The Manager starts up and we expect the pack file to be gone at the end of the test.
					ModifyStorage: func(_ testing.TB, _ config.Cfg, storagePath string) {
						packFilePath := packFilePath(walFilesPathForLSN(filepath.Join(storagePath, relativePath), 1))
						require.NoError(t, os.MkdirAll(filepath.Dir(packFilePath), perm.PrivateDir))
						require.NoError(t, os.WriteFile(
							packFilePath,
							[]byte("invalid pack"),
							perm.PrivateDir,
						))
					},
				},
			},
		},
		{
			desc: "repository is successfully deleted",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				RepositoryAssertion{
					TransactionID: 2,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "repository deletion fails if repository is deleted",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
					ExpectedError:    ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "custom hooks update fails if repository is deleted",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID:     2,
					CustomHooksUpdate: &CustomHooksUpdate{validCustomHooks(t)},
					ExpectedError:     ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "reference updates fail if repository is deleted",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "default branch update fails if repository is deleted",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				Commit{
					TransactionID: 2,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/new-default",
					},
					ExpectedError: ErrRepositoryNotFound,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "logged repository deletions are considered after restart",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
					ExpectedError:    ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				RepositoryAssertion{
					TransactionID: 2,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "reapplying repository deletion works",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
					ExpectedError:    ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				RepositoryAssertion{
					TransactionID: 2,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "non-existent repository is correctly handled",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				RepositoryAssertion{
					TransactionID: 1,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 1,
				},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{},
			},
		},
		{
			// This is a serialization violation as the outcome would be different
			// if the transactions were applied in different order.
			desc: "deletion succeeds with concurrent writes to repository",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch",
					},
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal": {Mode: fs.ModeDir | perm.PrivateDir},
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "deletion waits until other transactions are done",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID:    1,
					DeleteRepository: true,
				},
				// The concurrent transaction should be able to read the
				// repository despite the committed deletion.
				RepositoryAssertion{
					TransactionID: 2,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
								setup.Commits.Diverging.OID,
							},
						},
					},
				},
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "failing initialization prevents transaction beginning",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeReadAppliedLSN: func(hookContext) {
							// Raise a panic when the manager is about to read the applied log
							// index when initializing. In reality this would crash the server but
							// in tests it serves as a way to abort the initialization in correct
							// location.
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					RelativePath:  relativePath,
					ExpectedError: errInitializationFailed,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
			},
			expectedState: StateAssertion{
				// The test case fails before the partition's state directory is created.
				Directory: testhelper.DirectoryState{},
			},
		},
		{
			desc: "transaction rollbacked after already being rollbacked",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Rollback{},
				Rollback{
					ExpectedError: ErrTransactionAlreadyRollbacked,
				},
			},
		},
		{
			desc: "transaction rollbacked after already being committed",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{},
				Rollback{
					ExpectedError: ErrTransactionAlreadyCommitted,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
			},
		},
		{
			desc: "transaction committed after already being committed",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{},
				Commit{
					ExpectedError: ErrTransactionAlreadyCommitted,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
			},
		},
		{
			desc: "transaction committed after already being rollbacked",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Rollback{},
				Commit{
					ExpectedError: ErrTransactionAlreadyRollbacked,
				},
			},
		},
		{
			desc: "read-only transaction doesn't commit a log entry",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
					ReadOnly:     true,
				},
				Commit{},
			},
		},
		{
			desc: "read-only transaction fails with reference updates staged",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
					ReadOnly:     true,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: errReadOnlyReferenceUpdates,
				},
			},
		},
		{
			desc: "read-only transaction fails with default branch update staged",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
					ReadOnly:     true,
				},
				Commit{
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/main",
					},
					ExpectedError: errReadOnlyDefaultBranchUpdate,
				},
			},
		},
		{
			desc: "read-only transaction fails with custom hooks update staged",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
					ReadOnly:     true,
				},
				Commit{
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
					ExpectedError: errReadOnlyCustomHooksUpdate,
				},
			},
		},
		{
			desc: "read-only transaction fails with repository deletion staged",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
					ReadOnly:     true,
				},
				Commit{
					DeleteRepository: true,
					ExpectedError:    errReadOnlyRepositoryDeletion,
				},
			},
		},
		{
			desc: "read-only transaction fails with objects staged",
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
					ReadOnly:     true,
				},
				Commit{
					IncludeObjects: []git.ObjectID{setup.Commits.First.OID},
					ExpectedError:  errReadOnlyObjectsIncluded,
				},
			},
		},
		{
			desc: "transactions are snapshot isolated from concurrent updates",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 2,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/new-head",
					},
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				// This transaction was started before the commit, so it should see the original state.
				RepositoryAssertion{
					TransactionID: 1,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
						},
					},
				},
				// This transaction was started after the commit, so it should see the new state.
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/new-head",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							CustomHooks: testhelper.DirectoryState{
								"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/pre-receive": {
									Mode:    umask.Mask(fs.ModePerm),
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
							},
						},
					},
				},
				Rollback{
					TransactionID: 1,
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/new-head",
						References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/pre-receive": {
								Mode:    umask.Mask(fs.ModePerm),
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
						},
					},
				},
			},
		},
		{
			desc: "transactions are snapshot isolated from concurrent deletions",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Commit{
					TransactionID: 1,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/new-head",
					},
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
				},
				// This transaction was started before the deletion, so it should see the old state regardless
				// of the repository being deleted.
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/new-head",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							CustomHooks: testhelper.DirectoryState{
								"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/pre-receive": {
									Mode:    umask.Mask(fs.ModePerm),
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
							},
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
				Begin{
					TransactionID:       4,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 4,
					Repositories:  RepositoryStates{},
				},
				Rollback{
					TransactionID: 4,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "create repository when it doesn't exist",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				CreateRepository{},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "create repository when it already exists",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				Begin{
					TransactionID: 2,
					RelativePath:  relativePath,
				},
				CreateRepository{
					TransactionID: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 2,
					ExpectedError: ErrRepositoryAlreadyExists,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "create repository again after deletion",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:    3,
					DeleteRepository: true,
				},
				Begin{
					TransactionID:       4,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 3,
				},
				CreateRepository{
					TransactionID: 4,
				},
				Commit{
					TransactionID: 4,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(4).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/2":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/2/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/2/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/2/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
				Repositories: RepositoryStates{
					relativePath: {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "create repository with full state",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				CreateRepository{
					DefaultBranch: "refs/heads/branch",
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main":   setup.Commits.First.OID,
						"refs/heads/branch": setup.Commits.Second.OID,
					},
					Packs:       [][]byte{setup.Commits.First.Pack, setup.Commits.Second.Pack},
					CustomHooks: validCustomHooks(t),
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/branch",
						References: []git.Reference{
							{Name: "refs/heads/branch", Target: setup.Commits.Second.OID.String()},
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/pre-receive": {
								Mode:    umask.Mask(fs.ModePerm),
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
						},
					},
				},
			},
		},
		{
			desc: "transactions are snapshot isolated from concurrent creations",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs:       [][]byte{setup.Commits.First.Pack},
					CustomHooks: validCustomHooks(t),
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:    3,
					DeleteRepository: true,
				},
				Begin{
					TransactionID:       4,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 2,
				},
				CreateRepository{
					TransactionID: 4,
					DefaultBranch: "refs/heads/other",
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/other": setup.Commits.Second.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack, setup.Commits.Second.Pack},
				},
				Commit{
					TransactionID: 4,
				},
				// Transaction 2 has been open through out the repository deletion and creation. It should
				// still see the original state of the repository before the deletion.
				RepositoryAssertion{
					TransactionID: 2,
					Repositories: RepositoryStates{
						relativePath: {
							DefaultBranch: "refs/heads/main",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							CustomHooks: testhelper.DirectoryState{
								"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/pre-receive": {
									Mode:    umask.Mask(fs.ModePerm),
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
							},
						},
					},
				},
				Rollback{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/3":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/3/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/3/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
					"/wal/3/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/other",
						References: []git.Reference{
							{Name: "refs/heads/other", Target: setup.Commits.Second.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					},
				},
			},
		},
		{
			desc: "logged repository creation is respected",
			steps: steps{
				RemoveRepository{},
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID:       2,
					RelativePath:        relativePath,
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:    2,
					DeleteRepository: true,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "reapplying repository creation works",
			steps: steps{
				RemoveRepository{},
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
					RelativePath:  relativePath,
				},
				CreateRepository{
					TransactionID: 1,
					DefaultBranch: "refs/heads/branch",
					Packs:         [][]byte{setup.Commits.First.Pack},
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					CustomHooks: validCustomHooks(t),
				},
				Commit{
					TransactionID: 1,
					ExpectedError: ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
				Repositories: RepositoryStates{
					relativePath: {
						DefaultBranch: "refs/heads/branch",
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/pre-receive": {
								Mode:    umask.Mask(fs.ModePerm),
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
						},
						Objects: []git.ObjectID{
							setup.Commits.First.OID,
							setup.ObjectHash.EmptyTreeOID,
						},
					},
				},
			},
		},
		{
			desc: "commit without creating a repository",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "two repositories created in different transactions",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "repository-1",
				},
				Begin{
					TransactionID: 2,
					RelativePath:  "repository-2",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs:       [][]byte{setup.Commits.First.Pack},
					CustomHooks: validCustomHooks(t),
				},
				CreateRepository{
					TransactionID: 2,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/branch": setup.Commits.Third.OID,
					},
					DefaultBranch: "refs/heads/branch",
					Packs: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
						setup.Commits.Third.Pack,
					},
				},
				Commit{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					"repository-1": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/pre-receive": {
								Mode:    umask.Mask(fs.ModePerm),
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
						},
					},
					"repository-2": {
						DefaultBranch: "refs/heads/branch",
						References: []git.Reference{
							{Name: "refs/heads/branch", Target: setup.Commits.Third.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/2":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/2/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/2/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
					),
					"/wal/2/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "start transaction with empty relative path",
			steps: steps{
				StartManager{},
				Begin{
					ExpectedError: errRelativePathNotSet,
				},
			},
		},
		{
			desc: "repository is linked to alternate on creation",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
					"member": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "repository is linked to an alternate after creation",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{relativePath: "pool"},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
					"member": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "repository is disconnected from alternate",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				CloseManager{},
				StartManager{
					ModifyStorage: func(tb testing.TB, cfg config.Cfg, storagePath string) {
						// Transactions write objects always as packs into the repository. To test
						// scenarios where repositories may have existing loose objects, manually
						// unpack the objects to the repository.
						gittest.ExecOpts(tb, cfg,
							gittest.ExecConfig{Stdin: bytes.NewReader(setup.Commits.Second.Pack)},
							"-C", filepath.Join(storagePath, "pool"), "unpack-objects",
						)
					},
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					},
					"member": {
						// The objects should have been copied over to the repository when it was
						// disconnected from the alternate.
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "repository's alternate must be pointed to a git repository",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "repository",
				},
				CreateRepository{
					TransactionID: 1,
					Alternate:     "../..",
				},
				Commit{
					TransactionID: 1,
					ExpectedError: storage.InvalidGitDirectoryError{MissingEntry: "objects"},
				},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "repository's alternate must not point to repository itself",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "repository",
				},
				CreateRepository{
					TransactionID: 1,
					Alternate:     "../objects",
				},
				Commit{
					TransactionID: 1,
					ExpectedError: errAlternatePointsToSelf,
				},
			},
			expectedState: StateAssertion{
				Repositories: RepositoryStates{},
			},
		},
		{
			desc: "repository's alternate can't have an alternate itself",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "recursive-member",
					ExpectedSnapshotLSN: 2,
				},
				CreateRepository{
					TransactionID: 3,
					Alternate:     "../../member/objects",
				},
				Commit{
					TransactionID: 3,
					ExpectedError: errAlternateHasAlternate,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects:   []git.ObjectID{},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository can't be linked multiple times",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{relativePath: "pool"},
				},
				Begin{
					TransactionID:       4,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 3,
				},
				Commit{
					TransactionID:   4,
					UpdateAlternate: &alternateUpdate{relativePath: "pool"},
					ExpectedError:   errAlternateAlreadyLinked,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects:   []git.ObjectID{},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository can't be linked concurrently multiple times",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Begin{
					TransactionID:       4,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{relativePath: "pool"},
				},
				Commit{
					TransactionID:   4,
					UpdateAlternate: &alternateUpdate{relativePath: "pool"},
					ExpectedError:   errAlternateAlreadyLinked,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects:   []git.ObjectID{},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "repository without an alternate can't be disconnected",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "repository",
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "repository",
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID:   2,
					UpdateAlternate: &alternateUpdate{},
					ExpectedError:   errNoAlternate,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(1).toProto(),
				},
				Repositories: RepositoryStates{
					"repository": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "repository can't be disconnected concurrently multiple times",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Begin{
					TransactionID:       4,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{},
				},
				Commit{
					TransactionID:   4,
					UpdateAlternate: &alternateUpdate{},
					ExpectedError:   errNoAlternate,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "reapplying alternate linking works",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
				},
				CloseManager{},
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"member": {
							DefaultBranch: "refs/heads/main",
						},
					},
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{relativePath: "pool"},
					ExpectedError:   ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects:   []git.ObjectID{},
						Alternate: "../../pool/objects",
					},
				},
			},
		},
		{
			desc: "reapplying alternate disconnection works",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				CloseManager{},
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLSN: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
							DefaultBranch: "refs/heads/main",
						},
						"member": {
							DefaultBranch: "refs/heads/main",
							Alternate:     "../../pool/objects",
						},
					},
				},
				Commit{
					TransactionID:   3,
					UpdateAlternate: &alternateUpdate{},
					ExpectedError:   ErrTransactionProcessingStopped,
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						Objects: []git.ObjectID{},
					},
					"member": {
						Objects: []git.ObjectID{},
					},
				},
			},
		},
		{
			desc: "point reference to an object in an alternate",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
					"member": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "point reference to new object with dependencies in an alternate",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Second.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(3).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
					"member": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/3":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/3/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/3/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							// The pack should only contain the new object 'second' as the
							// rest of the objects exist in the alternate. We're still including
							// all unreachable objects in the logged pack until we can compute
							// the pack files dependencies.
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
					"/wal/3/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "repository's alternate is automatically snapshotted",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					Alternate:     "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
							DefaultBranch: "refs/heads/main",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
						},
						"member": {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							Alternate: "../../pool/objects",
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
					"member": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "multiple repositories can be included in transaction's snapshot",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "repository-1",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs:       [][]byte{setup.Commits.First.Pack},
					CustomHooks: validCustomHooks(t),
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "repository-2",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/branch": setup.Commits.Third.OID,
					},
					DefaultBranch: "refs/heads/branch",
					Packs: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
						setup.Commits.Third.Pack,
					},
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID:       3,
					RelativePath:        "repository-3",
					ExpectedSnapshotLSN: 2,
				},
				CreateRepository{
					TransactionID: 3,
					// Set repository-2 as repository-3's alternate to assert the
					// snasphotted repositories' alternates are also included.
					Alternate: "../../repository-2/objects",
				},
				Commit{
					TransactionID: 3,
				},
				Begin{
					TransactionID: 4,
					// Create a repository that is not snapshotted to assert it's not included
					// in the snapshot.
					RelativePath:        "repository-4",
					ExpectedSnapshotLSN: 3,
				},
				CreateRepository{
					TransactionID: 4,
				},
				Commit{
					TransactionID: 4,
				},
				Begin{
					TransactionID:            5,
					RelativePath:             "repository-1",
					SnapshottedRelativePaths: []string{"repository-3"},
					ExpectedSnapshotLSN:      4,
				},
				RepositoryAssertion{
					TransactionID: 5,
					Repositories: RepositoryStates{
						"repository-1": {
							DefaultBranch: "refs/heads/main",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
							CustomHooks: testhelper.DirectoryState{
								"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/pre-receive": {
									Mode:    umask.Mask(fs.ModePerm),
									Content: []byte("hook content"),
								},
								"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
								"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
							},
						},
						"repository-2": {
							DefaultBranch: "refs/heads/branch",
							References: []git.Reference{
								{Name: "refs/heads/branch", Target: setup.Commits.Third.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
							},
						},
						"repository-3": {
							DefaultBranch: "refs/heads/main",
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
								setup.Commits.Third.OID,
							},
							Alternate: "../../repository-2/objects",
						},
					},
				},
				Rollback{
					TransactionID: 5,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(4).toProto(),
				},
				Repositories: RepositoryStates{
					"repository-1": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
						CustomHooks: testhelper.DirectoryState{
							"/": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/pre-receive": {
								Mode:    umask.Mask(fs.ModePerm),
								Content: []byte("hook content"),
							},
							"/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
							"/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
						},
					},
					"repository-2": {
						DefaultBranch: "refs/heads/branch",
						References: []git.Reference{
							{Name: "refs/heads/branch", Target: setup.Commits.Third.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
					},
					"repository-3": {
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
						Alternate: "../../repository-2/objects",
					},
					"repository-4": {
						Objects: []git.ObjectID{},
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/2":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/2/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/2/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
					),
					"/wal/2/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "additional repository is included in the snapshot explicitly and implicitly",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/branch": setup.Commits.Second.OID,
					},
					DefaultBranch: "refs/heads/branch",
					Packs: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
					},
					Alternate: "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID: 3,
					RelativePath:  "member",
					// The pool is included explicitly here, and also implicitly through
					// the alternate link of member.
					SnapshottedRelativePaths: []string{"pool"},
					ExpectedSnapshotLSN:      2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
							DefaultBranch: "refs/heads/main",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
						},
						"member": {
							DefaultBranch: "refs/heads/branch",
							References: []git.Reference{
								{Name: "refs/heads/branch", Target: setup.Commits.Second.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
							},
							Alternate: "../../pool/objects",
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
					"member": {
						DefaultBranch: "refs/heads/branch",
						References: []git.Reference{
							{Name: "refs/heads/branch", Target: setup.Commits.Second.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/2":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/2/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/2/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
					"/wal/2/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
		{
			desc: "target repository is included in the snapshot explicitly and implicitly",
			steps: steps{
				RemoveRepository{},
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePath:  "pool",
				},
				CreateRepository{
					TransactionID: 1,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/main": setup.Commits.First.OID,
					},
					Packs: [][]byte{setup.Commits.First.Pack},
				},
				Commit{
					TransactionID: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePath:        "member",
					ExpectedSnapshotLSN: 1,
				},
				CreateRepository{
					TransactionID: 2,
					References: map[git.ReferenceName]git.ObjectID{
						"refs/heads/branch": setup.Commits.Second.OID,
					},
					DefaultBranch: "refs/heads/branch",
					Packs: [][]byte{
						setup.Commits.First.Pack,
						setup.Commits.Second.Pack,
					},
					Alternate: "../../pool/objects",
				},
				Commit{
					TransactionID: 2,
				},
				Begin{
					TransactionID: 3,
					// The pool is targeted, and also implicitly included through
					// the alternate link of member.
					RelativePath:             "pool",
					SnapshottedRelativePaths: []string{"member"},
					ExpectedSnapshotLSN:      2,
				},
				RepositoryAssertion{
					TransactionID: 3,
					Repositories: RepositoryStates{
						"pool": {
							DefaultBranch: "refs/heads/main",
							References: []git.Reference{
								{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
							},
						},
						"member": {
							DefaultBranch: "refs/heads/branch",
							References: []git.Reference{
								{Name: "refs/heads/branch", Target: setup.Commits.Second.OID.String()},
							},
							Objects: []git.ObjectID{
								setup.ObjectHash.EmptyTreeOID,
								setup.Commits.First.OID,
								setup.Commits.Second.OID,
							},
							Alternate: "../../pool/objects",
						},
					},
				},
				Rollback{
					TransactionID: 3,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN(partitionID)): LSN(2).toProto(),
				},
				Repositories: RepositoryStates{
					"pool": {
						References: []git.Reference{
							{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					},
					"member": {
						DefaultBranch: "refs/heads/branch",
						References: []git.Reference{
							{Name: "refs/heads/branch", Target: setup.Commits.Second.OID.String()},
						},
						Objects: []git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
						Alternate: "../../pool/objects",
					},
				},
				Directory: testhelper.DirectoryState{
					"/":                  {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal":               {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/1/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/1/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
					"/wal/1/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
					"/wal/2":             {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal/2/objects.idx": indexFileDirectoryEntry(setup.Config),
					"/wal/2/objects.pack": packFileDirectoryEntry(
						setup.Config,
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
					"/wal/2/objects.rev": reverseIndexFileDirectoryEntry(setup.Config),
				},
			},
		},
	}

	type invalidReferenceTestCase struct {
		desc          string
		referenceName git.ReferenceName
	}

	appendInvalidReferenceTestCase := func(tc invalidReferenceTestCase) {
		testCases = append(testCases, testCase{
			desc: fmt.Sprintf("invalid reference %s", tc.desc),
			steps: steps{
				StartManager{},
				Begin{
					RelativePath: relativePath,
				},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						tc.referenceName: {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: InvalidReferenceFormatError{ReferenceName: tc.referenceName},
				},
			},
		})
	}

	// Generate test cases for the reference format rules according to https://git-scm.com/docs/git-check-ref-format.
	// This is to ensure the references are correctly validated prior to logging so they are guaranteed to apply later.
	// We also have two levels for catching invalid refs, the first is part of the transaction_manager, the second is
	// the errors thrown by git-update-ref(1) itself.
	for _, tc := range []invalidReferenceTestCase{
		// 1. They can include slash / for hierarchical (directory) grouping, but no slash-separated
		// component can begin with a dot . or end with the sequence .lock.
		{"starting with a period", ".refs/heads/main"},
		{"subcomponent starting with a period", "refs/heads/.main"},
		{"ending in .lock", "refs/heads/main.lock"},
		{"subcomponent ending in .lock", "refs/heads/main.lock/main"},
		// 2. They must contain at least one /. This enforces the presence of a category like heads/,
		// tags/ etc. but the actual names are not restricted.
		{"without a /", "one-level"},
		{"with refs without a /", "refs"},
		// We restrict this further by requiring a 'refs/' prefix to ensure loose references only end up
		// in the 'refs/' folder.
		{"without refs/ prefix ", "nonrefs/main"},
		// 3. They cannot have two consecutive dots .. anywhere.
		{"containing two consecutive dots", "refs/heads/../main"},
		// 4. They cannot have ASCII control characters ... (\177 DEL), space, tilde ~, caret ^, or colon : anywhere.
		//
		// Tests for control characters < \040 generated further down.
		{"containing DEL", "refs/heads/ma\177in"},
		{"containing space", "refs/heads/ma in"},
		{"containing ~", "refs/heads/ma~in"},
		{"containing ^", "refs/heads/ma^in"},
		{"containing :", "refs/heads/ma:in"},
		// 5. They cannot have question-mark ?, asterisk *, or open bracket [ anywhere.
		{"containing ?", "refs/heads/ma?in"},
		{"containing *", "refs/heads/ma*in"},
		{"containing [", "refs/heads/ma[in"},
		// 6. They cannot begin or end with a slash / or contain multiple consecutive slashes
		{"begins with /", "/refs/heads/main"},
		{"ends with /", "refs/heads/main/"},
		{"contains consecutive /", "refs/heads//main"},
		// 7. They cannot end with a dot.
		{"ending in a dot", "refs/heads/main."},
		// 8. They cannot contain a sequence @{.
		{"invalid reference contains @{", "refs/heads/m@{n"},
		// 9. They cannot be the single character @.
		{"is a single character @", "@"},
		// 10. They cannot contain a \.
		{`containing \`, `refs/heads\main`},
	} {
		appendInvalidReferenceTestCase(tc)
	}

	// Rule 4. They cannot have ASCII control characters i.e. bytes whose values are lower than \040,
	for i := byte(0); i < '\040'; i++ {
		appendInvalidReferenceTestCase(invalidReferenceTestCase{
			desc:          fmt.Sprintf(`containing ASCII control character %d`, i),
			referenceName: git.ReferenceName(fmt.Sprintf("refs/heads/ma%sin", []byte{i})),
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// Setup the repository with the exact same state as what was used to build the test cases.
			setup := setupTest(t, relativePath)
			logger := testhelper.NewLogger(t)

			storageScopedFactory, err := setup.RepositoryFactory.ScopeByStorage(setup.Config.Storages[0].Name)
			require.NoError(t, err)
			repo := storageScopedFactory.Build(relativePath)

			repoPath, err := repo.Path()
			require.NoError(t, err)

			database, err := OpenDatabase(testhelper.SharedLogger(t), t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			txManager := transaction.NewManager(setup.Config, logger, backchannel.NewRegistry())
			housekeepingManager := housekeeping.NewManager(setup.Config.Prometheus, logger, txManager)

			storagePath := setup.Config.Storages[0].Path
			stateDir := filepath.Join(storagePath, "state")

			stagingDir := filepath.Join(storagePath, "staging")
			require.NoError(t, os.Mkdir(stagingDir, perm.PrivateDir))

			var (
				// managerRunning tracks whether the manager is running or closed.
				managerRunning bool
				// transactionManager is the current TransactionManager instance.
				transactionManager = NewTransactionManager(partitionID, logger, database, storagePath, stateDir, stagingDir, setup.CommandFactory, housekeepingManager, storageScopedFactory)
				// managerErr is used for synchronizing manager closing and returning
				// the error from Run.
				managerErr chan error
				// inflightTransactions tracks the number of on going transactions calls. It is used to synchronize
				// the database hooks with transactions.
				inflightTransactions sync.WaitGroup
			)

			// closeManager closes the manager. It waits until the manager's Run method has exited.
			closeManager := func() {
				t.Helper()

				transactionManager.Close()
				managerRunning, err = checkManagerError(t, ctx, managerErr, transactionManager)
				require.NoError(t, err)
				require.False(t, managerRunning)
			}

			// openTransactions holds references to all of the transactions that have been
			// began in a test case.
			openTransactions := map[int]*Transaction{}

			// Close the manager if it is running at the end of the test.
			defer func() {
				if managerRunning {
					closeManager()
				}
			}()
			for _, step := range tc.steps {
				switch step := step.(type) {
				case StartManager:
					require.False(t, managerRunning, "test error: manager started while it was already running")

					if step.ModifyStorage != nil {
						step.ModifyStorage(t, setup.Config, storagePath)
					}

					managerRunning = true
					managerErr = make(chan error)

					// The PartitionManager deletes and recreates the staging directory prior to starting a TransactionManager
					// to clean up any stale state leftover by crashes. Do that here as well so the tests don't fail if we don't
					// finish transactions after crash simulations.
					require.NoError(t, os.RemoveAll(stagingDir))
					require.NoError(t, os.Mkdir(stagingDir, perm.PrivateDir))

					transactionManager = NewTransactionManager(partitionID, logger, database, storagePath, stateDir, stagingDir, setup.CommandFactory, housekeepingManager, storageScopedFactory)
					installHooks(t, transactionManager, database, hooks{
						beforeReadLogEntry:  step.Hooks.BeforeApplyLogEntry,
						beforeStoreLogEntry: step.Hooks.BeforeAppendLogEntry,
						beforeDeferredClose: func(hookContext) {
							if step.Hooks.WaitForTransactionsWhenClosing {
								inflightTransactions.Wait()
							}
						},
						beforeDeleteLogEntry:  step.Hooks.BeforeDeleteLogEntry,
						beforeReadAppliedLSN:  step.Hooks.BeforeReadAppliedLSN,
						beforeStoreAppliedLSN: step.Hooks.BeforeStoreAppliedLSN,
					})

					go func() {
						defer func() {
							if r := recover(); r != nil {
								err, ok := r.(error)
								if !ok {
									panic(r)
								}
								assert.ErrorIs(t, err, step.ExpectedError)
								managerErr <- err
							}
						}()

						managerErr <- transactionManager.Run()
					}()
				case CloseManager:
					require.True(t, managerRunning, "test error: manager closed while it was already closed")
					closeManager()
				case AssertManager:
					require.True(t, managerRunning, "test error: manager must be running for syncing")
					managerRunning, err = checkManagerError(t, ctx, managerErr, transactionManager)
					require.ErrorIs(t, err, step.ExpectedError)
				case Begin:
					require.NotContains(t, openTransactions, step.TransactionID, "test error: transaction id reused in begin")

					beginCtx := ctx
					if step.Context != nil {
						beginCtx = step.Context
					}

					transaction, err := transactionManager.Begin(beginCtx, step.RelativePath, step.SnapshottedRelativePaths, step.ReadOnly)
					require.Equal(t, step.ExpectedError, err)
					if err == nil {
						require.Equal(t, step.ExpectedSnapshotLSN, transaction.SnapshotLSN())
					}

					if step.ReadOnly {
						require.Empty(t,
							transaction.quarantineDirectory,
							"read-only transaction should not have a quarantine directory",
						)
					}

					openTransactions[step.TransactionID] = transaction
				case Commit:
					require.Contains(t, openTransactions, step.TransactionID, "test error: transaction committed before beginning it")

					transaction := openTransactions[step.TransactionID]
					if step.SkipVerificationFailures {
						transaction.SkipVerificationFailures()
					}

					if step.UpdateAlternate != nil {
						transaction.UpdateAlternate(step.UpdateAlternate.relativePath)
					}

					if step.ReferenceUpdates != nil {
						transaction.UpdateReferences(step.ReferenceUpdates)
					}

					if step.DefaultBranchUpdate != nil {
						transaction.SetDefaultBranch(step.DefaultBranchUpdate.Reference)
					}

					if step.CustomHooksUpdate != nil {
						transaction.SetCustomHooks(step.CustomHooksUpdate.CustomHooksTAR)
					}

					if step.QuarantinedPacks != nil {
						for _, dir := range []string{
							transaction.stagingDirectory,
							transaction.quarantineDirectory,
						} {
							const expectedPerm = perm.PrivateDir
							stat, err := os.Stat(dir)
							require.NoError(t, err)
							require.Equal(t, stat.Mode().Perm(), umask.Mask(expectedPerm),
								"%q had %q permission but expected %q", dir, stat.Mode().Perm().String(), expectedPerm,
							)
						}

						rewrittenRepo := setup.RepositoryFactory.Build(
							transaction.RewriteRepository(&gitalypb.Repository{
								StorageName:  setup.Config.Storages[0].Name,
								RelativePath: transaction.relativePath,
							}),
						)

						for _, pack := range step.QuarantinedPacks {
							require.NoError(t, rewrittenRepo.UnpackObjects(ctx, bytes.NewReader(pack)))
						}
					}

					if step.DeleteRepository {
						transaction.DeleteRepository()
					}

					for _, objectID := range step.IncludeObjects {
						transaction.IncludeObject(objectID)
					}

					commitCtx := ctx
					if step.Context != nil {
						commitCtx = step.Context
					}

					commitErr := transaction.Commit(commitCtx)
					switch expectedErr := step.ExpectedError.(type) {
					case func(testing.TB, error):
						expectedErr(t, commitErr)
					case error:
						require.ErrorIs(t, commitErr, expectedErr)
					case nil:
						require.NoError(t, commitErr)
					default:
						t.Fatalf("unexpected error type: %T", expectedErr)
					}
				case RecordInitialReferenceValues:
					require.Contains(t, openTransactions, step.TransactionID, "test error: record initial reference value on transaction before beginning it")

					transaction := openTransactions[step.TransactionID]
					require.NoError(t, transaction.RecordInitialReferenceValues(ctx, step.InitialValues))
				case UpdateReferences:
					require.Contains(t, openTransactions, step.TransactionID, "test error: reference updates aborted on committed before beginning it")

					transaction := openTransactions[step.TransactionID]
					transaction.UpdateReferences(step.ReferenceUpdates)
				case Rollback:
					require.Contains(t, openTransactions, step.TransactionID, "test error: transaction rollbacked before beginning it")
					require.Equal(t, step.ExpectedError, openTransactions[step.TransactionID].Rollback())
				case Prune:
					// Repack all objects into a single pack and remove all other packs to remove all
					// unreachable objects from the packs.
					gittest.Exec(t, setup.Config, "-C", repoPath, "repack", "-ad")
					// Prune all unreachable loose objects in the repository.
					gittest.Exec(t, setup.Config, "-C", repoPath, "prune")

					require.ElementsMatch(t, step.ExpectedObjects, gittest.ListObjects(t, setup.Config, repoPath))
				case RemoveRepository:
					require.NoError(t, os.RemoveAll(repoPath))
				case CreateRepository:
					require.Contains(t, openTransactions, step.TransactionID, "test error: repository created in transaction before beginning it")

					transaction := openTransactions[step.TransactionID]
					require.NoError(t, repoutil.Create(
						ctx,
						logger,
						config.NewLocator(setup.Config),
						setup.CommandFactory,
						nil,
						counter.NewRepositoryCounter(setup.Config.Storages),
						transaction.RewriteRepository(&gitalypb.Repository{
							StorageName:  setup.Config.Storages[0].Name,
							RelativePath: transaction.relativePath,
						}),
						func(repoProto *gitalypb.Repository) error {
							repo := setup.RepositoryFactory.Build(repoProto)

							if step.DefaultBranch != "" {
								require.NoError(t, repo.SetDefaultBranch(ctx, nil, step.DefaultBranch))
							}

							for _, pack := range step.Packs {
								require.NoError(t, repo.UnpackObjects(ctx, bytes.NewReader(pack)))
							}

							for name, oid := range step.References {
								require.NoError(t, repo.UpdateRef(ctx, name, oid, setup.ObjectHash.ZeroOID))
							}

							if step.CustomHooks != nil {
								require.NoError(t,
									repoutil.SetCustomHooks(ctx, logger, config.NewLocator(setup.Config), nil, bytes.NewReader(step.CustomHooks), repo),
								)
							}

							if step.Alternate != "" {
								repoPath, err := repo.Path()
								require.NoError(t, err)

								require.NoError(t, os.WriteFile(stats.AlternatesFilePath(repoPath), []byte(step.Alternate), fs.ModePerm))
							}

							return nil
						},
						repoutil.WithObjectHash(setup.ObjectHash),
					))
				case RepositoryAssertion:
					require.Contains(t, openTransactions, step.TransactionID, "test error: transaction's snapshot asserted before beginning it")
					transaction := openTransactions[step.TransactionID]

					RequireRepositories(t, ctx, setup.Config,
						// Assert the contents of the transaction's snapshot.
						filepath.Join(setup.Config.Storages[0].Path, transaction.snapshot.prefix),
						// Rewrite all of the repositories to point to their snapshots.
						func(relativePath string) *localrepo.Repo {
							return setup.RepositoryFactory.Build(
								transaction.RewriteRepository(&gitalypb.Repository{
									StorageName:  setup.Config.Storages[0].Name,
									RelativePath: relativePath,
								}),
							)
						}, step.Repositories)
				default:
					t.Fatalf("unhandled step type: %T", step)
				}
			}

			if managerRunning {
				managerRunning, err = checkManagerError(t, ctx, managerErr, transactionManager)
				require.NoError(t, err)
			}

			RequireDatabase(t, ctx, database, tc.expectedState.Database)

			expectedRepositories := tc.expectedState.Repositories
			if expectedRepositories == nil {
				expectedRepositories = RepositoryStates{
					relativePath: {},
				}
			}

			for relativePath, state := range expectedRepositories {
				if state.Objects == nil {
					state.Objects = []git.ObjectID{
						setup.ObjectHash.EmptyTreeOID,
						setup.Commits.First.OID,
						setup.Commits.Second.OID,
						setup.Commits.Third.OID,
						setup.Commits.Diverging.OID,
					}
				}

				if state.DefaultBranch == "" {
					state.DefaultBranch = git.DefaultRef
				}

				expectedRepositories[relativePath] = state
			}

			RequireRepositories(t, ctx, setup.Config, setup.Config.Storages[0].Path, storageScopedFactory.Build, expectedRepositories)

			expectedDirectory := tc.expectedState.Directory
			if expectedDirectory == nil {
				// Set the base state as the default so we don't have to repeat it in every test case but it
				// gets asserted.
				expectedDirectory = testhelper.DirectoryState{
					"/":    {Mode: fs.ModeDir | perm.PrivateDir},
					"/wal": {Mode: fs.ModeDir | perm.PrivateDir},
				}
			}

			testhelper.RequireDirectoryState(t, stateDir, "", expectedDirectory)

			entries, err := os.ReadDir(stagingDir)
			require.NoError(t, err)
			require.Empty(t, entries, "staging directory was not cleaned up")
		})
	}
}

func checkManagerError(t *testing.T, ctx context.Context, managerErrChannel chan error, mgr *TransactionManager) (bool, error) {
	t.Helper()

	testTransaction := &Transaction{
		referenceUpdates: []ReferenceUpdates{{"sentinel": {}}},
		result:           make(chan error, 1),
		finish:           func() error { return nil },
	}

	var (
		// managerErr is the error returned from the TransactionManager's Run method.
		managerErr error
		// closeChannel determines whether the channel was still open. If so, we need to close it
		// so further calls of checkManagerError do not block as they won't manage to receive an err
		// as it was already received and won't be able to send as the manager is no longer running.
		closeChannel bool
	)

	select {
	case managerErr, closeChannel = <-managerErrChannel:
	case mgr.admissionQueue <- testTransaction:
		// If the error channel doesn't receive, we don't know whether it is because the manager is still running
		// or we are still waiting for it to return. We test whether the manager is running or not here by queueing a
		// a transaction that will error. If the manager processes it, we know it is still running.
		//
		// If the manager was closed, it might manage to admit the testTransaction but not process it. To determine
		// whether that was the case, we also keep waiting on the managerErr channel.
		select {
		case err := <-testTransaction.result:
			require.Error(t, err, "test transaction is expected to error out")

			// Begin a transaction to wait until the manager has applied all log entries currently
			// committed. This ensures the disk state assertions run with all log entries fully applied
			// to the repository.
			tx, err := mgr.Begin(ctx, "non-existent", nil, false)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())

			return true, nil
		case managerErr, closeChannel = <-managerErrChannel:
		}
	}

	if closeChannel {
		close(managerErrChannel)
	}

	return false, managerErr
}

// BenchmarkTransactionManager benchmarks the transaction throughput of the TransactionManager at various levels
// of concurrency and transaction sizes.
func BenchmarkTransactionManager(b *testing.B) {
	for _, tc := range []struct {
		// numberOfRepositories sets the number of repositories that are updating the references. Each repository has
		// its own TransactionManager. Setting this to 1 allows for testing throughput of a single repository while
		// setting this higher allows for testing parallel throughput of multiple repositories. This mostly serves
		// to determine the impact of the shared resources such as the database.
		numberOfRepositories int
		// concurrentUpdaters sets the number of goroutines that are calling committing transactions for a repository.
		// Each of the updaters work on their own references so they don't block each other. Setting this to 1 allows
		// for testing sequential update throughput of a repository. Setting this higher allows for testing reference
		// update throughput when multiple references are being updated concurrently.
		concurrentUpdaters int
		// transactionSize sets the number of references that are updated in each transaction.
		transactionSize int
	}{
		{
			numberOfRepositories: 1,
			concurrentUpdaters:   1,
			transactionSize:      1,
		},
		{
			numberOfRepositories: 10,
			concurrentUpdaters:   1,
			transactionSize:      1,
		},
		{
			numberOfRepositories: 1,
			concurrentUpdaters:   10,
			transactionSize:      1,
		},
		{
			numberOfRepositories: 1,
			concurrentUpdaters:   1,
			transactionSize:      10,
		},
		{
			numberOfRepositories: 10,
			concurrentUpdaters:   1,
			transactionSize:      10,
		},
	} {
		desc := fmt.Sprintf("%d repositories/%d updaters/%d transaction size",
			tc.numberOfRepositories,
			tc.concurrentUpdaters,
			tc.transactionSize,
		)
		b.Run(desc, func(b *testing.B) {
			ctx := testhelper.Context(b)

			cfg := testcfg.Build(b)
			logger := testhelper.NewLogger(b)

			cmdFactory := gittest.NewCommandFactory(b, cfg)
			cache := catfile.NewCache(cfg)
			defer cache.Stop()

			database, err := OpenDatabase(testhelper.SharedLogger(b), b.TempDir())
			require.NoError(b, err)
			defer testhelper.MustClose(b, database)

			txManager := transaction.NewManager(cfg, logger, backchannel.NewRegistry())
			housekeepingManager := housekeeping.NewManager(cfg.Prometheus, logger, txManager)

			var (
				// managerWG records the running TransactionManager.Run goroutines.
				managerWG sync.WaitGroup
				managers  []*TransactionManager

				// The references are updated back and forth between commit1 and commit2.
				commit1 git.ObjectID
				commit2 git.ObjectID
			)

			// getReferenceUpdates builds a ReferenceUpdates with unique branches for the updater.
			getReferenceUpdates := func(updaterID int, old, new git.ObjectID) ReferenceUpdates {
				referenceUpdates := make(ReferenceUpdates, tc.transactionSize)
				for i := 0; i < tc.transactionSize; i++ {
					referenceUpdates[git.ReferenceName(fmt.Sprintf("refs/heads/updater-%d-branch-%d", updaterID, i))] = ReferenceUpdate{
						OldOID: old,
						NewOID: new,
					}
				}

				return referenceUpdates
			}

			repositoryFactory, err := localrepo.NewFactory(
				logger, config.NewLocator(cfg), cmdFactory, cache,
			).ScopeByStorage(cfg.Storages[0].Name)
			require.NoError(b, err)

			// Set up the repositories and start their TransactionManagers.
			var relativePaths []string
			for i := 0; i < tc.numberOfRepositories; i++ {
				repo, repoPath := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				relativePaths = append(relativePaths, repo.RelativePath)

				// Set up two commits that the updaters update their references back and forth.
				// The commit IDs are the same across all repositories as the parameters used to
				// create them are the same. We thus simply override the commit IDs here across
				// repositories.
				commit1 = gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents())
				commit2 = gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents(commit1))

				storagePath := cfg.Storages[0].Path

				stateDir := filepath.Join(storagePath, "state", strconv.Itoa(i))
				require.NoError(b, os.MkdirAll(stateDir, perm.PrivateDir))

				stagingDir := filepath.Join(storagePath, "staging", strconv.Itoa(i))
				require.NoError(b, os.MkdirAll(stagingDir, perm.PrivateDir))

				// Valid partition IDs are >=1.
				partitionID := partitionID(i + 1)
				manager := NewTransactionManager(partitionID, logger, database, storagePath, stateDir, stagingDir, cmdFactory, housekeepingManager, repositoryFactory)

				managers = append(managers, manager)

				managerWG.Add(1)
				go func() {
					defer managerWG.Done()
					assert.NoError(b, manager.Run())
				}()

				objectHash, err := repositoryFactory.Build(repo.RelativePath).ObjectHash(ctx)
				require.NoError(b, err)

				for j := 0; j < tc.concurrentUpdaters; j++ {
					transaction, err := manager.Begin(ctx, repo.RelativePath, nil, false)
					require.NoError(b, err)
					transaction.UpdateReferences(getReferenceUpdates(j, objectHash.ZeroOID, commit1))
					require.NoError(b, transaction.Commit(ctx))
				}
			}

			// transactionWG tracks the number of on going transaction.
			var transactionWG sync.WaitGroup
			transactionChan := make(chan struct{})

			for i, manager := range managers {
				manager := manager
				relativePath := relativePaths[i]
				for i := 0; i < tc.concurrentUpdaters; i++ {

					// Build the reference updates that this updater will go back and forth with.
					currentReferences := getReferenceUpdates(i, commit1, commit2)
					nextReferences := getReferenceUpdates(i, commit2, commit1)

					transaction, err := manager.Begin(ctx, relativePath, nil, false)
					require.NoError(b, err)
					transaction.UpdateReferences(currentReferences)

					// Setup the starting state so the references start at the expected old tip.
					require.NoError(b, transaction.Commit(ctx))

					transactionWG.Add(1)
					go func() {
						defer transactionWG.Done()

						for range transactionChan {
							transaction, err := manager.Begin(ctx, relativePath, nil, false)
							require.NoError(b, err)
							transaction.UpdateReferences(nextReferences)
							assert.NoError(b, transaction.Commit(ctx))
							currentReferences, nextReferences = nextReferences, currentReferences
						}
					}()
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			began := time.Now()
			for n := 0; n < b.N; n++ {
				transactionChan <- struct{}{}
			}
			close(transactionChan)

			transactionWG.Wait()
			b.StopTimer()

			b.ReportMetric(float64(b.N*tc.transactionSize)/time.Since(began).Seconds(), "reference_updates/s")

			for _, manager := range managers {
				manager.Close()
			}

			managerWG.Wait()
		})
	}
}
