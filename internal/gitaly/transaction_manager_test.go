package gitaly

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
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

func TestTransactionManager(t *testing.T) {
	umask := perm.GetUmask()

	t.Parallel()

	ctx := testhelper.Context(t)

	// A clean repository is setup for each test. We build a repository ahead of the tests here once to
	// get deterministic commit IDs, relative path and object hash we can use to build the declarative
	// test cases.
	relativePath := gittest.NewRepositoryName(t)
	setupRepository := func(t *testing.T) (*localrepo.Repo, git.ObjectID, git.ObjectID, git.ObjectID) {
		t.Helper()

		cfg := testcfg.Build(t)

		repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           relativePath,
		})

		rootCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		secondCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(rootCommitOID))
		thirdCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(secondCommitOID))

		cmdFactory, clean, err := git.NewExecCommandFactory(cfg)
		require.NoError(t, err)
		t.Cleanup(clean)

		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)

		localRepo := localrepo.New(
			config.NewLocator(cfg),
			cmdFactory,
			catfileCache,
			repo,
		)

		return localRepo, rootCommitOID, secondCommitOID, thirdCommitOID
	}

	// Collect commit OIDs and the object has so we can define the test cases with them.
	repo, rootCommitOID, secondCommitOID, thirdCommitOID := setupRepository(t)
	objectHash, err := repo.ObjectHash(ctx)
	require.NoError(t, err)

	hasher := objectHash.Hash()
	_, err = hasher.Write([]byte("content does not matter"))
	require.NoError(t, err)
	nonExistentOID, err := objectHash.FromHex(hex.EncodeToString(hasher.Sum(nil)))
	require.NoError(t, err)

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

	// Step defines a single execution step in a test. Each test case can define multiple steps to setup exercise
	// more complex behavior and to assert the state after each step.
	type steps []struct {
		// StopManager stops the manager in the beginning of the step.
		StopManager bool
		// RestartManager can be used to start the manager again after stopping it.
		RestartManager bool
		// SkipStartManager can be used to skip starting the manager at the start of the step.
		SkipStartManager bool
		// CommitContext is the context to use for the Commit call of the step.
		CommitContext context.Context
		// SkipVerificationFailures sets the verification failure handling for this step.
		SkipVerificationFailures bool
		// ReferenceUpdates are the reference updates to perform in this step.
		ReferenceUpdates ReferenceUpdates
		// DefaultBranchUpdate is the default branch update to perform in this step.
		DefaultBranchUpdate *DefaultBranchUpdate
		// CustomHooksUpdate is the custom hooks update to perform in this step.
		CustomHooksUpdate *CustomHooksUpdate
		// Hooks contains the hook functions that are configured on the TransactionManager. These allow
		// for better synchronization.
		Hooks testHooks
		// ExpectedRunError is the expected error to be returned from Run from this step.
		ExpectedRunError bool
		// ExpectedCommitError is the error that is expected to be returned when committing the transaction in this step.
		ExpectedCommitError error
		// ExpectedHooks is the expected state of the hooks at the end of this step.
		ExpectedHooks testhelper.DirectoryState
		// ExpectedReferences is the expected state of references at the end of this step.
		ExpectedReferences []git.Reference
		// ExpectedDefaultBranch is the expected refname that HEAD points to.
		ExpectedDefaultBranch git.ReferenceName
		// ExpectedDatabase is the expected state of the database at the end of this step.
		ExpectedDatabase DatabaseState
		// ExpectedPanic is used to denote the panic, if any. Panics are triggered to stop the transaction
		// manager and the database/disk writes.
		ExpectedPanic any
	}

	type testCase struct {
		desc  string
		steps steps
	}

	testCases := []testCase{
		{
			desc: "invalid reference aborts the entire transaction",
			steps: steps{
				{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/../main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError:   updateref.InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "continues processing after aborting due to an invalid reference",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/../main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError:   updateref.InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
					ExpectedDefaultBranch: "",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "create reference",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "create a file-directory reference conflict different transaction",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/parent"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/parent",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError: updateref.FileDirectoryConflictError{
						ExistingReferenceName:    "refs/heads/parent",
						ConflictingReferenceName: "refs/heads/parent/child",
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/parent",
				},
			},
		},
		{
			desc: "create a file-directory reference conflict in same transaction",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError: updateref.InTransactionConflictError{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "file-directory conflict aborts the transaction with verification failures skipped",
			steps: steps{
				{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":         {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError: updateref.InTransactionConflictError{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "delete file-directory conflict in different transaction",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent/child", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/parent/child"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/parent/child",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent": {OldOID: objectHash.ZeroOID, NewOID: objectHash.ZeroOID},
					},
					ExpectedCommitError: updateref.FileDirectoryConflictError{
						ExistingReferenceName:    "refs/heads/parent/child",
						ConflictingReferenceName: "refs/heads/parent",
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent/child", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/parent/child",
				},
			},
		},
		{
			desc: "delete file-directory conflict in same transaction",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: objectHash.ZeroOID},
					},
					ExpectedCommitError: updateref.InTransactionConflictError{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "create a branch to a non-commit object",
			steps: steps{
				{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						// The error should abort the entire transaction.
						"refs/heads/branch-1": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/branch-2": {OldOID: objectHash.ZeroOID, NewOID: objectHash.EmptyTreeOID},
					},
					ExpectedCommitError: updateref.NonCommitObjectError{
						ReferenceName: "refs/heads/branch-2",
						ObjectID:      objectHash.EmptyTreeOID.String(),
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "create a tag to a non-commit object",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/tags/v1.0.0": {OldOID: objectHash.ZeroOID, NewOID: objectHash.EmptyTreeOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/tags/v1.0.0", Target: objectHash.EmptyTreeOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/tags/v1.0.0"),
											NewOid:        []byte(objectHash.EmptyTreeOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "create a reference to non-existent object",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: nonExistentOID},
					},
					ExpectedCommitError: updateref.NonExistentObjectError{
						ReferenceName: "refs/heads/main",
						ObjectID:      nonExistentOID.String(),
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "create reference ignoring verification failure",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: secondCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/non-conflicting"),
											NewOid:        []byte(secondCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "create reference that already exists",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   objectHash.ZeroOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "create reference no-op",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   objectHash.ZeroOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "update reference",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(secondCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "update reference ignoring verification failures",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/non-conflicting"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: secondCommitOID, NewOID: thirdCommitOID},
						"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: thirdCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: thirdCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/non-conflicting"),
											NewOid:        []byte(thirdCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "update reference with incorrect old tip",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/non-conflicting"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: secondCommitOID, NewOID: thirdCommitOID},
						"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: thirdCommitOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "update non-existent reference",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: secondCommitOID, NewOID: thirdCommitOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "update reference no-op",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "delete reference",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(objectHash.ZeroOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "delete reference ignoring verification failures",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/non-conflicting"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					SkipVerificationFailures: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: secondCommitOID, NewOID: objectHash.ZeroOID},
						"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/non-conflicting"),
											NewOid:        []byte(objectHash.ZeroOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "delete reference with incorrect old tip",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/non-conflicting"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":            {OldOID: secondCommitOID, NewOID: objectHash.ZeroOID},
						"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "delete non-existent reference",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "delete reference no-op",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: objectHash.ZeroOID},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(objectHash.ZeroOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "set custom hooks successfully",
			steps: steps{
				{
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									CustomHooksUpdate: &gitalypb.LogEntry_CustomHooksUpdate{
										CustomHooksTar: validCustomHooks(t),
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedHooks: testhelper.DirectoryState{
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
				{
					CustomHooksUpdate: &CustomHooksUpdate{},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									CustomHooksUpdate: &gitalypb.LogEntry_CustomHooksUpdate{},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedHooks: testhelper.DirectoryState{
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
		},
		{
			desc: "reapplying custom hooks works",
			steps: steps{
				{
					CustomHooksUpdate: &CustomHooksUpdate{
						CustomHooksTAR: validCustomHooks(t),
					},
					Hooks: testHooks{
						BeforeStoreAppliedLogIndex: func(hookContext) {
							panic("crash")
						},
					},
					ExpectedPanic: "crash",
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							CustomHooksUpdate: &gitalypb.LogEntry_CustomHooksUpdate{
								CustomHooksTar: validCustomHooks(t),
							},
						},
					},
					ExpectedRunError:    true,
					ExpectedCommitError: ErrTransactionProcessingStopped,
					ExpectedHooks: testhelper.DirectoryState{
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
				{
					// Empty transaction used here to just check the log entry is applied and the
					// applied index incremented.
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedHooks: testhelper.DirectoryState{
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
		},
		{
			desc: "continues processing after reference verification failure",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
					ExpectedDefaultBranch: "",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(secondCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "continues processing after a restart",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					StopManager:    true,
					RestartManager: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(secondCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "continues processing after restarting after a reference verification failure",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
					ExpectedDefaultBranch: "",
				},
				{
					StopManager:    true,
					RestartManager: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(secondCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "continues processing after failing to store log index",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					Hooks: testHooks{
						BeforeStoreAppliedLogIndex: func(hookCtx hookContext) {
							panic("node failure simulation")
						},
					},
					ExpectedPanic:       "node failure simulation",
					ExpectedCommitError: ErrTransactionProcessingStopped,
					ExpectedRunError:    true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
					ExpectedReferences:    []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "recovers from the write-ahead log on start up",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedCommitError: ErrTransactionProcessingStopped,
					ExpectedRunError:    true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
					ExpectedDefaultBranch: "",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					// Notice that here we can't check state of database before deletion because
					// the `applyLogEntry` function is called twice in this step. Once for logEntry:1 and once
					// for logEntry:2. Our current code only allows us to define one `BeforeDeleteLogEntry`
					// which would be common for both.
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "reference verification fails after recovering logged writes",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedCommitError: ErrTransactionProcessingStopped,
					ExpectedRunError:    true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
					ExpectedDefaultBranch: "",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: secondCommitOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "commit returns if context is canceled before admission",
			steps: steps{
				{
					CommitContext: func() context.Context {
						ctx, cancel := context.WithCancel(ctx)
						cancel()
						return ctx
					}(),
					SkipStartManager: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError:   context.Canceled,
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "commit returns if transaction processing stops before admission",
			steps: steps{
				{
					StopManager: true,
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError:   ErrTransactionProcessingStopped,
					ExpectedDefaultBranch: "",
				},
			},
		},
		func() testCase {
			ctx, cancel := context.WithCancel(ctx)
			return testCase{
				desc: "commit returns if context is canceled after admission",
				steps: steps{
					{
						CommitContext: ctx,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
						Hooks: testHooks{
							BeforeApplyLogEntry: func(hookCtx hookContext) {
								// Cancel the context used in Commit
								cancel()
							},
							BeforeDeleteLogEntry: func(hookCtx hookContext) {
								RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
									string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
									string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
										ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
											{
												ReferenceName: []byte("refs/heads/main"),
												NewOid:        []byte(rootCommitOID),
											},
										},
									},
								})
							},
						},
						ExpectedCommitError: context.Canceled,
						ExpectedReferences:  []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
						ExpectedDatabase: DatabaseState{
							string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
						},
						ExpectedDefaultBranch: "refs/heads/main",
					},
				},
			}
		}(),
		{
			desc: "commit returns if transaction processing stops before transaction acceptance",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					Hooks: testHooks{
						BeforeAppendLogEntry: func(hookContext hookContext) { hookContext.stopManager() },
						// This ensures we are testing the context cancellation errors being unwrapped properly
						// to an ErrTransactionProcessingStopped instead of hitting the general case when
						// runDone is closed.
						WaitForTransactionsWhenStopping: true,
					},
					ExpectedCommitError: ErrTransactionProcessingStopped,
				},
			},
		},
		{
			desc: "commit returns if transaction processing stops after transaction acceptance",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedCommitError: ErrTransactionProcessingStopped,
					ExpectedRunError:    true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
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
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/main":    {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/branch2"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									DefaultBranchUpdate: &gitalypb.LogEntry_DefaultBranchUpdate{
										ReferenceName: []byte("refs/heads/branch2"),
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/branch2",
				},
			},
		},
		{
			desc: "update default branch with new branch created in same transaction",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/main":    {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: secondCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/branch2"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(secondCommitOID),
										},
									},
									DefaultBranchUpdate: &gitalypb.LogEntry_DefaultBranchUpdate{
										ReferenceName: []byte("refs/heads/branch2"),
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/branch2",
				},
			},
		},
		{
			desc: "update default branch with invalid reference name",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/../main",
					},
					// For branch updates, we don't really verify the refname schematics, we take a shortcut
					// and rely on it being either a verified new reference name or a reference name which
					// exists on the repo already.
					ExpectedCommitError:   git.ErrReferenceNotFound,
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "update default branch to point to a non-existent reference name",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/yoda",
					},
					// For branch updates, we don't really verify the refname schematics, we take a shortcut
					// and rely on it being either a verified new reference name or a reference name which
					// exists on the repo already.
					ExpectedCommitError:   git.ErrReferenceNotFound,
					ExpectedDefaultBranch: "",
				},
			},
		},
		{
			desc: "update default branch to point to reference being deleted in the same transaction",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/branch2": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/branch2"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
					ExpectedCommitError: ReferenceToBeDeletedError{ReferenceName: "refs/heads/branch2"},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
			},
		},
		{
			desc: "update default branch with existing branch and other modifications",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/branch2": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/main":    {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/branch2"),
											NewOid:        []byte(rootCommitOID),
										},
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(rootCommitOID),
										},
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/main",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: secondCommitOID.String()},
					},
					Hooks: testHooks{
						BeforeDeleteLogEntry: func(hookCtx hookContext) {
							RequireDatabase(hookCtx.tb, ctx, hookCtx.database, DatabaseState{
								string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
								string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
									ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
										{
											ReferenceName: []byte("refs/heads/main"),
											NewOid:        []byte(secondCommitOID),
										},
									},
									DefaultBranchUpdate: &gitalypb.LogEntry_DefaultBranchUpdate{
										ReferenceName: []byte("refs/heads/branch2"),
									},
								},
							})
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/branch2",
				},
			},
		},
		{
			desc: "update default branch fails before storing log index",
			steps: steps{
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main":    {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						"refs/heads/branch2": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					DefaultBranchUpdate: &DefaultBranchUpdate{
						Reference: "refs/heads/branch2",
					},
					Hooks: testHooks{
						BeforeStoreAppliedLogIndex: func(hookCtx hookContext) {
							panic("node failure simulation")
						},
					},
					ExpectedPanic:       "node failure simulation",
					ExpectedCommitError: ErrTransactionProcessingStopped,
					ExpectedRunError:    true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/branch2"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
							DefaultBranchUpdate: &gitalypb.LogEntry_DefaultBranchUpdate{
								ReferenceName: []byte("refs/heads/branch2"),
							},
						},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
					},
					ExpectedDefaultBranch: "refs/heads/branch2",
				},
				{
					ReferenceUpdates: ReferenceUpdates{
						"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
					},

					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/branch2", Target: rootCommitOID.String()},
						{Name: "refs/heads/main", Target: secondCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
					ExpectedDefaultBranch: "refs/heads/branch2",
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
				{
					ReferenceUpdates: ReferenceUpdates{
						tc.referenceName: {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
					},
					ExpectedCommitError: err,
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
			repository, _, _, _ := setupRepository(t)

			database, err := OpenDatabase(t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			var (
				// managerRunning tracks whether the manager is running or stopped.
				managerRunning bool
				// transactionManager is the current TransactionManager instance.
				transactionManager *TransactionManager
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

			applyHooks := func(tb testing.TB, testHooks testHooks) {
				installHooks(tb, transactionManager, database, repository, hooks{
					beforeReadLogEntry:    testHooks.BeforeApplyLogEntry,
					beforeResolveRevision: testHooks.BeforeAppendLogEntry,
					beforeDeferredStop: func(hookContext) {
						if testHooks.WaitForTransactionsWhenStopping {
							inflightTransactions.Wait()
						}
					},
					beforeDeleteLogEntry:       testHooks.BeforeDeleteLogEntry,
					beforeStoreAppliedLogIndex: testHooks.BeforeStoreAppliedLogIndex,
				})
			}

			// startManager starts fresh manager and applies hooks into it.
			startManager := func(testHooks testHooks, expectedPanic any) {
				t.Helper()

				require.False(t, managerRunning, "manager started while it was already running")
				managerRunning = true
				managerErr = make(chan error)

				transactionManager = NewTransactionManager(database, repository)
				applyHooks(t, testHooks)

				go func() {
					defer func() {
						r := recover()
						assert.Equal(t, expectedPanic, r)
						if r != nil {
							managerErr <- fmt.Errorf("received panic: %s", r.(string))
						}
					}()

					managerErr <- transactionManager.Run()
				}()
			}

			// Stop the manager if it is running at the end of the test.
			defer func() {
				if managerRunning {
					stopManager()
				}
			}()
			for _, step := range tc.steps {
				switch {
				case step.SkipStartManager:
					transactionManager = NewTransactionManager(database, repository)
				case !managerRunning:
					// Unless explicitly skipped, ensure every step starts with
					// the manager running.
					startManager(step.Hooks, step.ExpectedPanic)
				default:
					// Apply the hooks for this step if the manager is running
					// already to ensure the steps hooks are in place.
					applyHooks(t, step.Hooks)
				}

				if step.StopManager {
					require.True(t, managerRunning, "manager stopped while it was already stopped")
					stopManager()
				}

				if step.RestartManager {
					startManager(step.Hooks, step.ExpectedPanic)
				}

				func() {
					inflightTransactions.Add(1)
					defer inflightTransactions.Done()

					transaction, err := transactionManager.Begin(ctx)
					require.NoError(t, err)
					defer transaction.Rollback()

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

					commitCtx := ctx
					if step.CommitContext != nil {
						commitCtx = step.CommitContext
					}

					require.ErrorIs(t, transaction.Commit(commitCtx), step.ExpectedCommitError)
				}()

				if !step.SkipStartManager {
					if managerRunning, err = checkManagerError(t, managerErr, transactionManager); step.ExpectedRunError {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}

				RequireReferences(t, ctx, repository, step.ExpectedReferences)
				RequireDefaultBranch(t, ctx, repository, step.ExpectedDefaultBranch)
				RequireDatabase(t, ctx, database, step.ExpectedDatabase)
				RequireHooks(t, repository, step.ExpectedHooks)
			}
		})
	}
}

func checkManagerError(t *testing.T, managerErrChannel chan error, mgr *TransactionManager) (bool, error) {
	t.Helper()

	testTransaction := &Transaction{
		referenceUpdates: ReferenceUpdates{"sentinel": {}},
		result:           make(chan error, 1),
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

			// Set up the repositories and start their TransactionManagers.
			for i := 0; i < tc.numberOfRepositories; i++ {
				repo, repoPath := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				localRepo := localrepo.New(
					config.NewLocator(cfg),
					cmdFactory,
					cache,
					repo,
				)

				// Set up two commits that the updaters update their references back and forth.
				// The commit IDs are the same across all repositories as the parameters used to
				// create them are the same. We thus simply override the commit IDs here across
				// repositories.
				commit1 = gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents())
				commit2 = gittest.WriteCommit(b, cfg, repoPath, gittest.WithParents(commit1))

				manager := NewTransactionManager(database, localRepo)
				managers = append(managers, manager)

				managerWG.Add(1)
				go func() {
					defer managerWG.Done()
					assert.NoError(b, manager.Run())
				}()

				objectHash, err := localRepo.ObjectHash(ctx)
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
