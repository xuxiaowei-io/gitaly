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
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// errSimulatedCrash is used in the tests to simulate a crash at a certain point during
// TransactionManager.Run execution.
var errSimulatedCrash = errors.New("simulated crash")

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

	// partitionID is the partition ID used in the tests for the TransactionManager.
	const partitionID partitionID = 1

	setupTest := func(t *testing.T, relativePath string) testTransactionSetup {
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

		return testTransactionSetup{
			PartitionID:       partitionID,
			RelativePath:      relativePath,
			RepositoryPath:    repoPath,
			Repo:              localRepo,
			Config:            cfg,
			ObjectHash:        objectHash,
			CommandFactory:    cmdFactory,
			RepositoryFactory: localrepo.NewFactory(logger, locator, cmdFactory, catfileCache),
			NonExistentOID:    nonExistentOID,
			Commits: testTransactionCommits{
				First: testTransactionCommit{
					OID:  firstCommitOID,
					Pack: packCommit(firstCommitOID),
				},
				Second: testTransactionCommit{
					OID:  secondCommitOID,
					Pack: packCommit(secondCommitOID),
				},
				Third: testTransactionCommit{
					OID:  thirdCommitOID,
					Pack: packCommit(thirdCommitOID),
				},
				Diverging: testTransactionCommit{
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

	testCases := []transactionTestCase{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
		func() transactionTestCase {
			ctx, cancel := context.WithCancel(ctx)
			return transactionTestCase{
				desc: "commit returns if context is canceled after admission",
				steps: steps{
					StartManager{
						Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
					Hooks: testTransactionHooks{
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
		{
			desc: "non-git directories are not snapshotted",
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					// Try to snapshot the parent directory, which is no a valid Git directory.
					RelativePath:  filepath.Dir(relativePath),
					ExpectedError: storage.InvalidGitDirectoryError{MissingEntry: "objects"},
				},
			},
		},
	}

	subTests := [][]transactionTestCase{
		generateInvalidReferencesTests(t, setup),
		generateModifyReferencesTests(t, setup),
	}
	for _, subCases := range subTests {
		testCases = append(testCases, subCases...)
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// Setup the repository with the exact same state as what was used to build the test cases.
			setup := setupTest(t, relativePath)
			runTransactionTest(t, ctx, tc, setup)
		})
	}
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
