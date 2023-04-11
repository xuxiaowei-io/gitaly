package gitaly

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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
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

// noopTransactionFinalizer is a transaction finalizer for testing TransactionManager that does not
// call back to PartitionManager.
func noopTransactionFinalizer() {}

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
func packFileDirectoryEntry(cfg config.Cfg, mode fs.FileMode, expectedObjects []git.ObjectID) testhelper.DirectoryEntry {
	sortObjects := func(objects []git.ObjectID) {
		sort.Slice(objects, func(i, j int) bool {
			return objects[i] < objects[j]
		})
	}

	sortObjects(expectedObjects)

	return testhelper.DirectoryEntry{
		Mode:    mode,
		Content: expectedObjects,
		ParseContent: func(tb testing.TB, content []byte) any {
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

func TestTransactionManager(t *testing.T) {
	umask := perm.GetUmask()

	t.Parallel()

	ctx := testhelper.Context(t)

	type testCommit struct {
		OID  git.ObjectID
		Pack []byte
	}

	type testCommits struct {
		First  testCommit
		Second testCommit
		Third  testCommit
	}

	type testSetup struct {
		Config            config.Cfg
		RepositoryFactory localrepo.StorageScopedFactory
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

		cmdFactory, clean, err := git.NewExecCommandFactory(cfg)
		require.NoError(t, err)
		t.Cleanup(clean)

		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)

		locator := config.NewLocator(cfg)
		localRepo := localrepo.New(
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

		repositoryFactory, err := localrepo.NewFactory(
			locator, cmdFactory, catfileCache,
		).ScopeByStorage(cfg.Storages[0].Name)
		require.NoError(t, err)

		return testSetup{
			Config:            cfg,
			ObjectHash:        objectHash,
			RepositoryFactory: repositoryFactory,
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

	type testHooks struct {
		// BeforeApplyLogEntry is called before a log entry is applied to the repository.
		BeforeApplyLogEntry hookFunc
		// BeforeAppendLogEntry is called before a log entry is appended to the log.
		BeforeAppendLogEntry hookFunc
		// BeforeDeleteLogEntry is called before a log entry is deleted.
		BeforeDeleteLogEntry hookFunc
		// beforeStoreAppliedLogIndex is invoked before a the applied log index is stored.
		BeforeStoreAppliedLogIndex hookFunc
		// WaitForTransactionsWhenStopping waits for a in-flight to finish before returning
		// from Run.
		WaitForTransactionsWhenStopping bool
	}

	// StartManager starts a TransactionManager.
	type StartManager struct {
		// Hooks contains the hook functions that are configured on the TransactionManager. These allow
		// for better synchronization.
		Hooks testHooks
		// ExpectedError is the expected error to be raised from the manager's Run. Panics are converted
		// to errors and asserted to match this as well.
		ExpectedError error
		// ModifyRepository allows for running modifying the repository prior the manager starting. This
		// may be necessary to test some states that can be reached from hard crashes but not during the
		// tests.
		ModifyRepository func(tb testing.TB, repoPath string)
	}

	// StopManager stops a TransactionManager.
	type StopManager struct{}

	// AssertManager asserts whether the manager has stopped and Run returned. If it has, it asserts the
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
		// Context is the context to use for the Begin call.
		Context context.Context
		// ExpectedSnapshot is the expected snapshot of the transaction.
		ExpectedSnapshot Snapshot
		// ExpectedError is the error expected to be returned from the Begin call.
		ExpectedError error
	}

	// Commit calls Commit on a transaction.
	type Commit struct {
		// TransactionID identifies the transaction to commit.
		TransactionID int
		// Context is the context to use for the Commit call.
		Context context.Context
		// ExpectedError is the error that is expected to be returned when committing the transaction.
		ExpectedError error

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
	}

	// Rollback calls Rollback on a transaction.
	type Rollback struct {
		// TransactionID identifies the transaction to rollback.
		TransactionID int
	}

	// Prune prunes all unreferenced objects from the repository.
	type Prune struct {
		// ExpectedObjects are the object expected to exist in the repository after pruning.
		ExpectedObjects []git.ObjectID
	}

	// StateAssertions models an assertion of the entire state managed by the TransactionManager.
	type StateAssertion struct {
		// DefaultBranch is the expected refname that HEAD points to.
		DefaultBranch git.ReferenceName
		// References is the expected state of references.
		References []git.Reference
		// Database is the expected state of the database.
		Database DatabaseState
		// Directory is the expected state of the manager's state directory in the repository.
		Directory testhelper.DirectoryState
		// Objects are the objects that are expected to exist in the repository.
		Objects []git.ObjectID
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
				Begin{},
				Commit{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/../main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: updateref.InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
				},
			},
		},
		{
			desc: "continues processing after aborting due to an invalid reference",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/../main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: updateref.InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
				},
				Begin{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "create reference",
			steps: steps{
				StartManager{},
				Begin{},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "create a file-directory reference conflict different transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/parent",
				References:    []git.Reference{{Name: "refs/heads/parent", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "create a file-directory reference conflict in same transaction",
			steps: steps{
				StartManager{},
				Begin{},
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
				Begin{},
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
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/parent/child",
				References:    []git.Reference{{Name: "refs/heads/parent/child", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "delete file-directory conflict in same transaction",
			steps: steps{
				StartManager{},
				Begin{},
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
			desc: "create a branch to a non-commit object",
			steps: steps{
				StartManager{},
				Begin{},
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
				Begin{},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/tags/v1.0.0": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.EmptyTreeOID},
					},
				},
			},
			expectedState: StateAssertion{
				References: []git.Reference{{Name: "refs/tags/v1.0.0", Target: setup.ObjectHash.EmptyTreeOID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "create a reference to non-existent object",
			steps: steps{
				StartManager{},
				Begin{},
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
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/non-conflicting", Target: setup.Commits.Second.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "create reference that already exists",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "create reference no-op",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "update reference",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "update reference ignoring verification failures",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/non-conflicting", Target: setup.Commits.Third.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "update reference with incorrect old tip",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/non-conflicting", Target: setup.Commits.First.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "update non-existent reference",
			steps: steps{
				StartManager{},
				Begin{},
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
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "delete reference",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "delete reference ignoring verification failures",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "delete reference with incorrect old tip",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/non-conflicting": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/non-conflicting", Target: setup.Commits.First.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "delete non-existent reference",
			steps: steps{
				StartManager{},
				Begin{},
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
				Begin{},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.ObjectHash.ZeroOID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "set custom hooks successfully",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
						HookIndex: 1,
					},
				},
				Commit{
					TransactionID:     2,
					CustomHooksUpdate: &CustomHooksUpdate{},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":         {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs":   {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks":   {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1/pre-receive": {
						Mode:    umask.Mask(fs.ModePerm),
						Content: []byte("hook content"),
					},
					"/wal/hooks/1/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
					"/wal/hooks/1/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
					"/wal/hooks/2":                          {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				},
			},
		},
		{
			desc: "reapplying custom hooks works",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLogIndex: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":         {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs":   {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks":   {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1/pre-receive": {
						Mode:    umask.Mask(fs.ModePerm),
						Content: []byte("hook content"),
					},
					"/wal/hooks/1/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
					"/wal/hooks/1/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
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
				},
				Commit{
					TransactionID: 1,
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
						HookIndex: 1,
					},
				},
				Commit{
					TransactionID:     2,
					CustomHooksUpdate: &CustomHooksUpdate{},
				},
				Begin{
					TransactionID: 3,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 2,
						HookIndex: 2,
					},
				},
				StopManager{},
				StartManager{},
				Begin{
					TransactionID: 4,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 2,
						HookIndex: 2,
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":         {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks":   {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1/pre-receive": {
						Mode:    umask.Mask(fs.ModePerm),
						Content: []byte("hook content"),
					},
					"/wal/hooks/1/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
					"/wal/hooks/1/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
					"/wal/hooks/2":                          {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs":                            {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				},
			},
		},
		{
			desc: "continues processing after reference verification failure",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
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
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "continues processing after a restart",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				StopManager{},
				StartManager{},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "continues processing after restarting after a reference verification failure",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
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
				StopManager{},
				StartManager{},
				Begin{
					TransactionID: 2,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "continues processing after failing to store log index",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLogIndex: func(hookCtx hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "recovers from the write-ahead log on start up",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
				},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AssertManager{},
				StartManager{},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "reference verification fails after recovering logged writes",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
				},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				AssertManager{},
				StartManager{},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "begin returns if context is canceled before initialization",
			steps: steps{
				Begin{
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
				Begin{},
				StopManager{},
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
					Begin{},
					Commit{
						Context: ctx,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						},
						ExpectedError: context.Canceled,
					},
				},
				expectedState: StateAssertion{
					DefaultBranch: "refs/heads/main",
					References:    []git.Reference{{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()}},
					Database: DatabaseState{
						string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
					},
				},
			}
		}(),
		{
			desc: "commit returns if transaction processing stops before transaction acceptance",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeAppendLogEntry: func(hookContext hookContext) { hookContext.stopManager() },
						// This ensures we are testing the context cancellation errors being unwrapped properly
						// to an ErrTransactionProcessingStopped instead of hitting the general case when
						// runDone is closed.
						WaitForTransactionsWhenStopping: true,
					},
				},
				Begin{},
				StopManager{},
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
							hookCtx.stopManager()
						},
					},
				},
				Begin{},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyLogEntry(relativePath, 1)): &gitalypb.LogEntry{
						ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
							{
								ReferenceName: []byte("refs/heads/main"),
								NewOid:        []byte(setup.Commits.First.OID),
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
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/branch2",
				References: []git.Reference{
					{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "update default branch with new branch created in same transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/branch2",
				References: []git.Reference{
					{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "update default branch with invalid reference name",
			steps: steps{
				StartManager{},
				Begin{},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/../main",
					},
					// For branch updates, we don't really verify the refname schematics, we take a shortcut
					// and rely on it being either a verified new reference name or a reference name which
					// exists on the repo already.
					ExpectedError: git.ErrReferenceNotFound,
				},
			},
		},
		{
			desc: "update default branch to point to a non-existent reference name",
			steps: steps{
				StartManager{},
				Begin{},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/yoda",
					},
					// For branch updates, we don't really verify the refname schematics, we take a shortcut
					// and rely on it being either a verified new reference name or a reference name which
					// exists on the repo already.
					ExpectedError: git.ErrReferenceNotFound,
				},
			},
		},
		{
			desc: "update default branch to point to reference being deleted in the same transaction",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: setup.Commits.First.OID, NewOID: setup.ObjectHash.ZeroOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
					ExpectedError: ReferenceToBeDeletedError{ReferenceName: "refs/heads/branch2"},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
			},
		},
		{
			desc: "update default branch with existing branch and other modifications",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
						"refs/heads/main":    {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/branch2",
				References: []git.Reference{
					{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "update default branch fails before storing log index",
			steps: steps{
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLogIndex: func(hookCtx hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
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
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/branch2",
				References: []git.Reference{
					{Name: "refs/heads/branch2", Target: setup.Commits.First.OID.String()},
					{Name: "refs/heads/main", Target: setup.Commits.Second.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
			},
		},
		{
			desc: "read snapshots include committed data",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Begin{
					TransactionID: 2,
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
				Begin{
					TransactionID: 3,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
						HookIndex: 1,
					},
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
				Begin{
					TransactionID: 4,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 2,
						HookIndex: 1,
					},
				},
				Rollback{
					TransactionID: 3,
				},
				Begin{
					TransactionID: 5,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 2,
						HookIndex: 1,
					},
				},
				Commit{
					TransactionID: 4,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.Third.OID},
					},
					CustomHooksUpdate: &CustomHooksUpdate{},
				},
				Begin{
					TransactionID: 6,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 3,
						HookIndex: 3,
					},
				},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.Third.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(3).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":         {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs":   {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks":   {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks/1/pre-receive": {
						Mode:    umask.Mask(fs.ModePerm),
						Content: []byte("hook content"),
					},
					"/wal/hooks/1/private-dir":              {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
					"/wal/hooks/1/private-dir/private-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("private content")},
					"/wal/hooks/3":                          {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
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
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs/1.pack": packFileDirectoryEntry(
						setup.Config,
						umask.Mask(perm.PrivateFile),
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Objects: []git.ObjectID{
					setup.ObjectHash.EmptyTreeOID,
					setup.Commits.First.OID,
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
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				// Point main to the first commit so the second one is unreachable.
				Commit{
					TransactionID: 2,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.Second.OID, NewOID: setup.Commits.First.OID},
					},
				},
				AssertManager{},
				StopManager{},
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
					TransactionID: 3,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 2,
					},
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Third.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.Third.Pack},
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
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.Third.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(3).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs/1.pack": packFileDirectoryEntry(
						setup.Config,
						umask.Mask(perm.PrivateFile),
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
							setup.Commits.Second.OID,
						},
					),
					"/wal/packs/3.pack": packFileDirectoryEntry(
						setup.Config,
						umask.Mask(perm.PrivateFile),
						[]git.ObjectID{
							setup.Commits.Second.OID,
							setup.Commits.Third.OID,
						},
					),
				},
				Objects: []git.ObjectID{
					setup.ObjectHash.EmptyTreeOID,
					setup.Commits.First.OID,
					setup.Commits.Second.OID,
					setup.Commits.Third.OID,
				},
			},
		},
		{
			desc: "pack file reapplying works",
			steps: steps{
				Prune{},
				StartManager{
					Hooks: testHooks{
						BeforeStoreAppliedLogIndex: func(hookContext) {
							panic(errSimulatedCrash)
						},
					},
					ExpectedError: errSimulatedCrash,
				},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				AssertManager{
					ExpectedError: errSimulatedCrash,
				},
				StartManager{},
			},
			expectedState: StateAssertion{
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs/1.pack": packFileDirectoryEntry(
						setup.Config,
						umask.Mask(perm.PrivateFile),
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Objects: []git.ObjectID{
					setup.ObjectHash.EmptyTreeOID,
					setup.Commits.First.OID,
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
				Objects: []git.ObjectID{},
			},
		},
		{
			desc: "pack file missing intermediate commit",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
				DefaultBranch: "refs/heads/main",
				References: []git.Reference{
					{Name: "refs/heads/main", Target: setup.Commits.First.OID.String()},
				},
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs/1.pack": packFileDirectoryEntry(
						setup.Config,
						umask.Mask(perm.PrivateFile),
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Objects: []git.ObjectID{
					setup.ObjectHash.EmptyTreeOID,
					setup.Commits.First.OID,
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
				},
				Commit{
					TransactionID:    1,
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLogIndex(relativePath)): LogIndex(1).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				},
				Objects: []git.ObjectID{},
			},
		},
		{
			desc: "pack file with deletions",
			steps: steps{
				Prune{},
				StartManager{},
				Begin{
					TransactionID: 1,
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs/1.pack": packFileDirectoryEntry(
						setup.Config,
						umask.Mask(perm.PrivateFile),
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Objects: []git.ObjectID{
					setup.ObjectHash.EmptyTreeOID,
					setup.Commits.First.OID,
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
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					QuarantinedPacks: [][]byte{setup.Commits.First.Pack},
				},
				Begin{
					TransactionID: 2,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
				},
				Begin{
					TransactionID: 3,
					ExpectedSnapshot: Snapshot{
						ReadIndex: 1,
					},
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
					string(keyAppliedLogIndex(relativePath)): LogIndex(2).toProto(),
				},
				Directory: testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs/1.pack": packFileDirectoryEntry(
						setup.Config,
						umask.Mask(perm.PrivateFile),
						[]git.ObjectID{
							setup.ObjectHash.EmptyTreeOID,
							setup.Commits.First.OID,
						},
					),
				},
				Objects: []git.ObjectID{},
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
					ModifyRepository: func(tb testing.TB, repoPath string) {
						packFilePath := packFilePathForLogIndex(repoPath, 1)
						require.NoError(t, os.MkdirAll(filepath.Dir(packFilePath), fs.ModePerm))
						require.NoError(t, os.WriteFile(
							packFilePath,
							[]byte("invalid pack"),
							fs.ModePerm,
						))
					},
				},
			},
		},
	}

	type invalidReferenceTestCase struct {
		desc             string
		referenceName    git.ReferenceName
		invalidReference git.ReferenceName
		updateRefError   bool
	}

	appendInvalidReferenceTestCase := func(tc invalidReferenceTestCase) {
		invalidReference := tc.invalidReference
		if invalidReference == "" {
			invalidReference = tc.referenceName
		}

		var err error
		err = InvalidReferenceFormatError{ReferenceName: invalidReference}
		if tc.updateRefError {
			err = updateref.InvalidReferenceFormatError{ReferenceName: invalidReference.String()}
		}

		testCases = append(testCases, testCase{
			desc: fmt.Sprintf("invalid reference %s", tc.desc),
			steps: steps{
				StartManager{},
				Begin{},
				Commit{
					ReferenceUpdates: ReferenceUpdates{
						tc.referenceName: {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
					ExpectedError: err,
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
		{"starting with a period", ".refs/heads/main", "", false},
		{"subcomponent starting with a period", "refs/heads/.main", "", true},
		{"ending in .lock", "refs/heads/main.lock", "", true},
		{"subcomponent ending in .lock", "refs/heads/main.lock/main", "", true},
		// 2. They must contain at least one /. This enforces the presence of a category like heads/,
		// tags/ etc. but the actual names are not restricted.
		{"without a /", "one-level", "", false},
		{"with refs without a /", "refs", "", false},
		// We restrict this further by requiring a 'refs/' prefix to ensure loose references only end up
		// in the 'refs/' folder.
		{"without refs/ prefix ", "nonrefs/main", "", false},
		// 3. They cannot have two consecutive dots .. anywhere.
		{"containing two consecutive dots", "refs/heads/../main", "", true},
		// 4. They cannot have ASCII control characters ... (\177 DEL), space, tilde ~, caret ^, or colon : anywhere.
		//
		// Tests for control characters < \040 generated further down.
		{"containing DEL", "refs/heads/ma\177in", "refs/heads/ma?in", true},
		{"containing space", "refs/heads/ma in", "", true},
		{"containing ~", "refs/heads/ma~in", "", true},
		{"containing ^", "refs/heads/ma^in", "", true},
		{"containing :", "refs/heads/ma:in", "", true},
		// 5. They cannot have question-mark ?, asterisk *, or open bracket [ anywhere.
		{"containing ?", "refs/heads/ma?in", "", true},
		{"containing *", "refs/heads/ma*in", "", true},
		{"containing [", "refs/heads/ma[in", "", true},
		// 6. They cannot begin or end with a slash / or contain multiple consecutive slashes
		{"begins with /", "/refs/heads/main", "", false},
		{"ends with /", "refs/heads/main/", "", true},
		{"contains consecutive /", "refs/heads//main", "", true},
		// 7. They cannot end with a dot.
		{"ending in a dot", "refs/heads/main.", "", true},
		// 8. They cannot contain a sequence @{.
		{"invalid reference contains @{", "refs/heads/m@{n", "", true},
		// 9. They cannot be the single character @.
		{"is a single character @", "@", "", false},
		// 10. They cannot contain a \.
		{`containing \`, `refs/heads\main`, `refs/heads\main`, true},
	} {
		appendInvalidReferenceTestCase(tc)
	}

	// Rule 4. They cannot have ASCII control characters i.e. bytes whose values are lower than \040,
	for i := byte(0); i < '\040'; i++ {
		invalidReference := fmt.Sprintf("refs/heads/ma%sin", []byte{i})

		// For now, the reference format is checked by 'git update-ref'. Most of the control characters
		// are substituted by a '?' by Git when reporting the error.
		//
		// '\t' is reported without substitution. '\n' and '\000' are checked by the TransactionManager
		// separately so it can report a better error than what Git produces.
		//
		// This is temporarily more complicated than it needs to be. Later iteration will move reference
		// format checking into Gitaly at which point we can return proper errors without substituting
		// the problematic characters.
		var expectedSubstitute string
		switch i {
		case '\000', '\n', '\t':
			expectedSubstitute = string(i)
		default:
			expectedSubstitute = "?"
		}

		updateRefError := true
		if i == '\000' || i == '\n' {
			updateRefError = false
		}

		appendInvalidReferenceTestCase(invalidReferenceTestCase{
			desc:             fmt.Sprintf(`containing ASCII control character %d`, i),
			referenceName:    git.ReferenceName(invalidReference),
			invalidReference: git.ReferenceName(fmt.Sprintf("refs/heads/ma%sin", expectedSubstitute)),
			updateRefError:   updateRefError,
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// Setup the repository with the exact same state as what was used to build the test cases.
			setup := setupTest(t, relativePath)

			repo := setup.RepositoryFactory.Build(relativePath)
			repoPath, err := repo.Path()
			require.NoError(t, err)

			database, err := OpenDatabase(t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			stagingDir := t.TempDir()
			storagePath := setup.Config.Storages[0].Path

			var (
				// managerRunning tracks whether the manager is running or stopped.
				managerRunning bool
				// transactionManager is the current TransactionManager instance.
				transactionManager = NewTransactionManager(database, storagePath, relativePath, stagingDir, setup.RepositoryFactory, noopTransactionFinalizer)
				// managerErr is used for synchronizing manager stopping and returning
				// the error from Run.
				managerErr chan error
				// inflightTransactions tracks the number of on going transactions calls. It is used to synchronize
				// the database hooks with transactions.
				inflightTransactions sync.WaitGroup
			)

			// stopManager stops the manager. It waits until the manager's Run method has exited.
			stopManager := func() {
				t.Helper()

				transactionManager.Stop()
				managerRunning, err = checkManagerError(t, managerErr, transactionManager)
				require.NoError(t, err)
				require.False(t, managerRunning)
			}

			// openTransactions holds references to all of the transactions that have been
			// began in a test case.
			openTransactions := map[int]*Transaction{}

			// Stop the manager if it is running at the end of the test.
			defer func() {
				if managerRunning {
					stopManager()
				}
			}()
			for _, step := range tc.steps {
				switch step := step.(type) {
				case StartManager:
					require.False(t, managerRunning, "test error: manager started while it was already running")

					if step.ModifyRepository != nil {
						step.ModifyRepository(t, repoPath)
					}

					managerRunning = true
					managerErr = make(chan error)

					transactionManager = NewTransactionManager(database, storagePath, relativePath, stagingDir, setup.RepositoryFactory, noopTransactionFinalizer)
					installHooks(t, transactionManager, database, hooks{
						beforeReadLogEntry:    step.Hooks.BeforeApplyLogEntry,
						beforeResolveRevision: step.Hooks.BeforeAppendLogEntry,
						beforeDeferredStop: func(hookContext) {
							if step.Hooks.WaitForTransactionsWhenStopping {
								inflightTransactions.Wait()
							}
						},
						beforeDeleteLogEntry:       step.Hooks.BeforeDeleteLogEntry,
						beforeStoreAppliedLogIndex: step.Hooks.BeforeStoreAppliedLogIndex,
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
				case StopManager:
					require.True(t, managerRunning, "test error: manager stopped while it was already stopped")
					stopManager()
				case AssertManager:
					require.True(t, managerRunning, "test error: manager must be running for syncing")
					managerRunning, err = checkManagerError(t, managerErr, transactionManager)
					require.ErrorIs(t, err, step.ExpectedError)
				case Begin:
					require.NotContains(t, openTransactions, step.TransactionID, "test error: transaction id reused in begin")

					beginCtx := ctx
					if step.Context != nil {
						beginCtx = step.Context
					}

					transaction, err := transactionManager.Begin(beginCtx)
					require.Equal(t, step.ExpectedError, err)
					if err == nil {
						require.Equal(t, step.ExpectedSnapshot, transaction.Snapshot())
					}
					openTransactions[step.TransactionID] = transaction
				case Commit:
					require.Contains(t, openTransactions, step.TransactionID, "test error: transaction committed before beginning it")

					transaction := openTransactions[step.TransactionID]
					if step.SkipVerificationFailures {
						transaction.SkipVerificationFailures()
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
						quarantineDirectory, err := transaction.QuarantineDirectory()
						require.NoError(t, err)

						for _, dir := range []string{
							transaction.stagingDirectory,
							quarantineDirectory,
						} {
							const expectedPerm = perm.PrivateDir
							stat, err := os.Stat(dir)
							require.NoError(t, err)
							require.Equal(t, stat.Mode().Perm(), umask.Mask(expectedPerm),
								"%q had %q permission but expected %q", dir, stat.Mode().Perm().String(), expectedPerm,
							)
						}

						for i, pack := range step.QuarantinedPacks {
							writePack(t, setup.Config, pack, filepath.Join(quarantineDirectory, "pack", fmt.Sprintf("%d.pack", i)))
						}

					}

					commitCtx := ctx
					if step.Context != nil {
						commitCtx = step.Context
					}

					require.ErrorIs(t, transaction.Commit(commitCtx), step.ExpectedError)
				case Rollback:
					require.Contains(t, openTransactions, step.TransactionID, "test error: transaction rollbacked before beginning it")
					require.NoError(t, openTransactions[step.TransactionID].Rollback())
				case Prune:
					gittest.Exec(t, setup.Config, "-C", repoPath, "prune")
					require.ElementsMatch(t, step.ExpectedObjects, gittest.ListObjects(t, setup.Config, repoPath))
				default:
					t.Fatalf("unhandled step type: %T", step)
				}
			}

			if managerRunning {
				managerRunning, err = checkManagerError(t, managerErr, transactionManager)
				require.NoError(t, err)
			}

			RequireReferences(t, ctx, repo, tc.expectedState.References)
			RequireDefaultBranch(t, ctx, repo, tc.expectedState.DefaultBranch)
			RequireDatabase(t, ctx, database, tc.expectedState.Database)

			expectedDirectory := tc.expectedState.Directory
			if expectedDirectory == nil {
				// Set the base state as the default so we don't have to repeat it in every test case but it
				// gets asserted.
				expectedDirectory = testhelper.DirectoryState{
					"/wal":       {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/packs": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
					"/wal/hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				}
			}

			testhelper.RequireDirectoryState(t, repoPath, "wal", expectedDirectory)

			expectedObjects := tc.expectedState.Objects
			if expectedObjects == nil {
				expectedObjects = []git.ObjectID{
					setup.ObjectHash.EmptyTreeOID,
					setup.Commits.First.OID,
					setup.Commits.Second.OID,
					setup.Commits.Third.OID,
				}
			}

			require.ElementsMatch(t, expectedObjects, gittest.ListObjects(t, setup.Config, repoPath))

			entries, err := os.ReadDir(stagingDir)
			require.NoError(t, err)
			require.Empty(t, entries, "staging directory was not cleaned up")
		})
	}
}

func checkManagerError(t *testing.T, managerErrChannel chan error, mgr *TransactionManager) (bool, error) {
	t.Helper()

	testTransaction := &Transaction{
		referenceUpdates: ReferenceUpdates{"sentinel": {}},
		result:           make(chan error, 1),
		finalize:         func() {},
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
		// If the manager was stopped, it might manage to admit the testTransaction but not process it. To determine
		// whether that was the case, we also keep waiting on the managerErr channel.
		select {
		case err := <-testTransaction.result:
			require.Error(t, err, "test transaction is expected to error out")
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

			cmdFactory, clean, err := git.NewExecCommandFactory(cfg)
			require.NoError(b, err)
			defer clean()

			cache := catfile.NewCache(cfg)
			defer cache.Stop()

			database, err := OpenDatabase(b.TempDir())
			require.NoError(b, err)
			defer testhelper.MustClose(b, database)

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
				config.NewLocator(cfg), cmdFactory, cache,
			).ScopeByStorage(cfg.Storages[0].Name)
			require.NoError(b, err)

			// Set up the repositories and start their TransactionManagers.
			for i := 0; i < tc.numberOfRepositories; i++ {
				repo, repoPath := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				// Set up two commits that the updaters update their references back and forth.
				// The commit IDs are the same across all repositories as the parameters used to
				// create them are the same. We thus simply override the commit IDs here across
				// repositories.
				commit1 = gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents())
				commit2 = gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents(commit1))

				manager := NewTransactionManager(database, cfg.Storages[0].Path, repo.RelativePath, b.TempDir(), repositoryFactory, noopTransactionFinalizer)
				managers = append(managers, manager)

				managerWG.Add(1)
				go func() {
					defer managerWG.Done()
					assert.NoError(b, manager.Run())
				}()

				objectHash, err := repositoryFactory.Build(repo.RelativePath).ObjectHash(ctx)
				require.NoError(b, err)

				for j := 0; j < tc.concurrentUpdaters; j++ {
					transaction, err := manager.Begin(ctx)
					require.NoError(b, err)
					transaction.UpdateReferences(getReferenceUpdates(j, objectHash.ZeroOID, commit1))
					require.NoError(b, transaction.Commit(ctx))
				}
			}

			// transactionWG tracks the number of on going transaction.
			var transactionWG sync.WaitGroup
			transactionChan := make(chan struct{})

			for _, manager := range managers {
				manager := manager
				for i := 0; i < tc.concurrentUpdaters; i++ {

					// Build the reference updates that this updater will go back and forth with.
					currentReferences := getReferenceUpdates(i, commit1, commit2)
					nextReferences := getReferenceUpdates(i, commit2, commit1)

					transaction, err := manager.Begin(ctx)
					require.NoError(b, err)
					transaction.UpdateReferences(currentReferences)

					// Setup the starting state so the references start at the expected old tip.
					require.NoError(b, transaction.Commit(ctx))

					transactionWG.Add(1)
					go func() {
						defer transactionWG.Done()

						for range transactionChan {
							transaction, err := manager.Begin(ctx)
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
				manager.Stop()
			}

			managerWG.Wait()
		})
	}
}
