package gitaly

import (
	"context"
	"encoding/hex"
	"fmt"
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
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestTransactionManager(t *testing.T) {
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
		// WaitForTransactionsWhenStopping waits for a in-flight to finish before returning
		// from Run.
		WaitForTransactionsWhenStopping bool
	}

	// Step defines a single execution step in a test. Each test case can define multiple steps to setup exercise
	// more complex behavior and to assert the state after each step.
	type steps []struct {
		// StopManager stops the manager in the beginning of the step.
		StopManager bool
		// StartManager can be used to start the manager again after stopping it.
		StartManager bool
		// Context is the context to use for the Propose call of the step.
		Context context.Context
		// Transaction is the transaction that is proposed in this step.
		Transaction Transaction
		// Hooks contains the hook functions that are configured on the TransactionManager. These allow
		// for better synchronization.
		Hooks testHooks
		// ExpectedRunError is the expected error to be returned from Run from this step.
		ExpectedRunError bool
		// ExpectedProposeError is the error that is expected to be returned when proposing the transaction in this step.
		ExpectedProposeError error
		// ExpectedReferences is the expected state of references at the end of this step.
		ExpectedReferences []git.Reference
		// ExpectedDatabase is the expected state of the database at the end of this step.
		ExpectedDatabase DatabaseState
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
					Transaction: Transaction{
						SkipVerificationFailures: true,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":    {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/../main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: updateref.ErrInvalidReferenceFormat{ReferenceName: "refs/heads/../main"},
				},
			},
		},
		{
			desc: "continues processing after aborting due to an invalid reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/../main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: updateref.ErrInvalidReferenceFormat{ReferenceName: "refs/heads/../main"},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
			},
		},
		{
			desc: "create reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
			},
		},
		{
			desc: "create a file-directory reference conflict different transaction",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: updateref.ErrFileDirectoryConflict{
						ExistingReferenceName:    "refs/heads/parent",
						ConflictingReferenceName: "refs/heads/parent/child",
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
				},
			},
		},
		{
			desc: "create a file-directory reference conflict in same transaction",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: updateref.ErrInTransactionConflict{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
				},
			},
		},
		{
			desc: "file-directory conflict aborts the transaction with verification failures skipped",
			steps: steps{
				{
					Transaction: Transaction{
						SkipVerificationFailures: true,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":         {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: updateref.ErrInTransactionConflict{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
				},
			},
		},
		{
			desc: "delete file-directory conflict in different transaction",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent": {OldOID: objectHash.ZeroOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedProposeError: updateref.ErrFileDirectoryConflict{
						ExistingReferenceName:    "refs/heads/parent/child",
						ConflictingReferenceName: "refs/heads/parent",
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent/child", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
				},
			},
		},
		{
			desc: "delete file-directory conflict in same transaction",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedProposeError: updateref.ErrInTransactionConflict{
						FirstReferenceName:  "refs/heads/parent",
						SecondReferenceName: "refs/heads/parent/child",
					},
				},
			},
		},
		{
			desc: "create a branch to a non-commit object",
			steps: steps{
				{
					Transaction: Transaction{
						SkipVerificationFailures: true,
						ReferenceUpdates: ReferenceUpdates{
							// The error should abort the entire transaction.
							"refs/heads/branch-1": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/branch-2": {OldOID: objectHash.ZeroOID, NewOID: objectHash.EmptyTreeOID},
						},
					},
					ExpectedProposeError: updateref.NonCommitObjectError{
						ReferenceName: "refs/heads/branch-2",
						ObjectID:      objectHash.EmptyTreeOID.String(),
					},
				},
			},
		},
		{
			desc: "create a tag to a non-commit object",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/tags/v1.0.0": {OldOID: objectHash.ZeroOID, NewOID: objectHash.EmptyTreeOID},
						},
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
				},
			},
		},
		{
			desc: "create a reference to non-existent object",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: nonExistentOID},
						},
					},
					ExpectedProposeError: updateref.NonExistentObjectError{
						ReferenceName: "refs/heads/main",
						ObjectID:      nonExistentOID.String(),
					},
				},
			},
		},
		{
			desc: "create reference ignoring verification failure",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						SkipVerificationFailures: true,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
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
				},
			},
		},
		{
			desc: "create reference that already exists",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
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
				},
			},
		},
		{
			desc: "create reference no-op",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   objectHash.ZeroOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
					},
				},
			},
		},
		{
			desc: "update reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
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
				},
			},
		},
		{
			desc: "update reference ignoring verification failures",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						SkipVerificationFailures: true,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: thirdCommitOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: thirdCommitOID},
						},
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
				},
			},
		},
		{
			desc: "update reference with incorrect old tip",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: thirdCommitOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: thirdCommitOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
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
				},
			},
		},
		{
			desc: "update non-existent reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: secondCommitOID, NewOID: thirdCommitOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
			},
		},
		{
			desc: "update reference no-op",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: rootCommitOID},
						},
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
				},
			},
		},
		{
			desc: "delete reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
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
				},
			},
		},
		{
			desc: "delete reference ignoring verification failures",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						SkipVerificationFailures: true,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: objectHash.ZeroOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
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
				},
			},
		},
		{
			desc: "delete reference with incorrect old tip",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: objectHash.ZeroOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
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
				},
			},
		},
		{
			desc: "delete non-existent reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
			},
		},
		{
			desc: "delete reference no-op",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: objectHash.ZeroOID},
						},
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
				},
			},
		},
		{
			desc: "continues processing after reference verification failure",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
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
				},
			},
		},
		{
			desc: "continues processing after a restart",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
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
				},
				{
					StopManager:  true,
					StartManager: true,
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
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
				},
			},
		},
		{
			desc: "continues processing after restarting after a reference verification failure",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
				{
					StopManager:  true,
					StartManager: true,
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
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
				},
			},
		},
		{
			desc: "recovers from the write-ahead log on start up",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedProposeError: ErrTransactionProcessingStopped,
					ExpectedRunError:     true,
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
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					// Notice that here we can't check state of database before deletion because
					// the `applyLogEntry` function is called twice in this step. Once for logEntry:1 and once
					// for logEntry:2. Our current code only allows us to define one `BeforeDeleteLogEntry`
					// which would be common for both.
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(2).toProto(),
					},
				},
			},
		},
		{
			desc: "reference verification fails after recovering logged writes",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedProposeError: ErrTransactionProcessingStopped,
					ExpectedRunError:     true,
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
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: secondCommitOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: ReferenceVerificationError{
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
				},
			},
		},
		{
			desc: "propose returns if context is canceled before admission",
			steps: steps{
				{
					Context: func() context.Context {
						ctx, cancel := context.WithCancel(ctx)
						cancel()
						return ctx
					}(),
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: context.Canceled,
				},
			},
		},
		{
			desc: "propose returns if transaction processing stops before admission",
			steps: steps{
				{
					StopManager: true,
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: ErrTransactionProcessingStopped,
				},
			},
		},
		func() testCase {
			ctx, cancel := context.WithCancel(ctx)
			return testCase{
				desc: "propose returns if context is canceled after admission",
				steps: steps{
					{
						Context: ctx,
						Transaction: Transaction{
							ReferenceUpdates: ReferenceUpdates{
								"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							},
						},
						Hooks: testHooks{
							BeforeApplyLogEntry: func(hookCtx hookContext) {
								// Cancel the context used in Propose
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
						ExpectedProposeError: context.Canceled,
						ExpectedReferences:   []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
						ExpectedDatabase: DatabaseState{
							string(keyAppliedLogIndex(getRepositoryID(repo))): LogIndex(1).toProto(),
						},
					},
				},
			}
		}(),
		{
			desc: "propose returns if transaction processing stops before transaction acceptance",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeAppendLogEntry: func(hookContext hookContext) { hookContext.stopManager() },
						// This ensures we are testing the context cancellation errors being unwrapped properly
						// to an ErrTransactionProcessingStopped instead of hitting the general case when
						// runDone is closed.
						WaitForTransactionsWhenStopping: true,
					},
					ExpectedProposeError: ErrTransactionProcessingStopped,
				},
			},
		},
		{
			desc: "propose returns if transaction processing stops after transaction acceptance",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedProposeError: ErrTransactionProcessingStopped,
					ExpectedRunError:     true,
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
			err = updateref.ErrInvalidReferenceFormat{ReferenceName: invalidReference.String()}
		}

		testCases = append(testCases, testCase{
			desc: fmt.Sprintf("invalid reference %s", tc.desc),
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							tc.referenceName: {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedProposeError: err,
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

			testhelper.SkipQuarantinedTest(t,
				"https://gitlab.com/gitlab-org/gitaly/-/issues/4794",
				"TestTransactionManager/propose_returns_if_context_is_canceled_before_admission",
			)

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
				// inflightTransactions tracks the number of on going propose calls. It is used to synchronize
				// the database hooks with propose calls.
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
					beforeDeleteLogEntry: testHooks.BeforeDeleteLogEntry,
				})
			}

			// startManager starts fresh manager and applies hooks into it.
			startManager := func(testHooks testHooks) {
				t.Helper()

				require.False(t, managerRunning, "manager started while it was already running")
				managerRunning = true
				managerErr = make(chan error)

				transactionManager = NewTransactionManager(database, repository)
				applyHooks(t, testHooks)

				go func() { managerErr <- transactionManager.Run() }()
			}

			// Stop the manager if it is running at the end of the test.
			defer stopManager()
			for _, step := range tc.steps {
				// Ensure every step starts with the manager running.
				if !managerRunning {
					startManager(step.Hooks)
				} else {
					// Apply the hooks for this step if the manager is running already to ensure the
					// steps hooks are in place.
					applyHooks(t, step.Hooks)
				}

				if step.StopManager {
					require.True(t, managerRunning, "manager stopped while it was already stopped")
					stopManager()
				}

				if step.StartManager {
					startManager(step.Hooks)
				}

				func() {
					inflightTransactions.Add(1)
					defer inflightTransactions.Done()

					proposeCtx := ctx
					if step.Context != nil {
						proposeCtx = step.Context
					}

					require.ErrorIs(t, transactionManager.Propose(proposeCtx, step.Transaction), step.ExpectedProposeError)
				}()

				if managerRunning, err = checkManagerError(t, managerErr, transactionManager); step.ExpectedRunError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				RequireReferences(t, ctx, repository, step.ExpectedReferences)
				RequireDatabase(t, ctx, database, step.ExpectedDatabase)
			}
		})
	}
}

func checkManagerError(t *testing.T, managerErrChannel chan error, mgr *TransactionManager) (bool, error) {
	t.Helper()

	testTransaction := transactionFuture{
		transaction: Transaction{ReferenceUpdates: ReferenceUpdates{"sentinel": {}}},
		result:      make(resultChannel, 1),
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
		// a proposal that will error. If the manager processes it, we know it is still running.
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
		// concurrentUpdaters sets the number of goroutines that are calling Propose for a repository. Each of the
		// updaters work on their own references so they don't block each other. Setting this to 1 allows for testing
		// sequential update throughput of a repository. Setting this higher allows for testing reference update
		// throughput when multiple references are being updated concurrently.
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
					require.NoError(b, manager.Propose(ctx, Transaction{
						ReferenceUpdates: getReferenceUpdates(j, objectHash.ZeroOID, commit1),
					}))
				}
			}

			// proposeWG tracks the number of on going Propose calls.
			var proposeWG sync.WaitGroup
			transactionChan := make(chan struct{})

			for _, manager := range managers {
				manager := manager
				for i := 0; i < tc.concurrentUpdaters; i++ {

					// Build the reference updates that this updater will go back and forth with.
					currentReferences := getReferenceUpdates(i, commit1, commit2)
					nextReferences := getReferenceUpdates(i, commit2, commit1)

					// Setup the starting state so the references start at the expected old tip.
					require.NoError(b, manager.Propose(ctx, Transaction{
						ReferenceUpdates: currentReferences,
					}))

					proposeWG.Add(1)
					go func() {
						defer proposeWG.Done()

						for range transactionChan {
							assert.NoError(b, manager.Propose(ctx, Transaction{ReferenceUpdates: nextReferences}))
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

			proposeWG.Wait()
			b.StopTimer()

			b.ReportMetric(float64(b.N*tc.transactionSize)/time.Since(began).Seconds(), "reference_updates/s")

			for _, manager := range managers {
				manager.Stop()
			}

			managerWG.Wait()
		})
	}
}
