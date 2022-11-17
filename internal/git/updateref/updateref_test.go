package updateref

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func setupUpdater(t *testing.T, ctx context.Context) (config.Cfg, *localrepo.Repo, string, *Updater) {
	t.Helper()

	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto, git.WithSkipHooks())

	updater, err := New(ctx, repo)
	require.NoError(t, err)
	t.Cleanup(func() {
		// This is just to clean up so we ignore the error here. Cancel may or may not
		// return an error depending on the test.
		_ = updater.Close()
	})

	return cfg, repo, repoPath, updater
}

func TestUpdater_create(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, _, repoPath, updater := setupUpdater(t, ctx)
	defer testhelper.MustClose(t, updater)

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	// Create the first branch.
	require.NoError(t, updater.Start())
	require.NoError(t, updater.Create("refs/heads/_create", commitID))
	require.NoError(t, updater.Commit())

	// Verify that the reference was created as expected and that it points to the correct
	// commit.
	require.Equal(t, gittest.ResolveRevision(t, cfg, repoPath, "refs/heads/_create"), commitID)
}

func TestUpdater_invalidStateTransitions(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath, updater := setupUpdater(t, ctx)
	defer testhelper.MustClose(t, updater)

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	require.Equal(t, invalidStateTransitionError{expected: stateStarted, actual: stateIdle}, updater.Update("refs/heads/main", commitID, ""))
	require.Equal(t, invalidStateTransitionError{expected: stateStarted, actual: stateClosed}, updater.Create("refs/heads/main", commitID))
	require.Equal(t, invalidStateTransitionError{expected: stateStarted, actual: stateClosed}, updater.Delete("refs/heads/main"))
	require.Equal(t, invalidStateTransitionError{expected: stateStarted, actual: stateClosed}, updater.Prepare())
	require.Equal(t, invalidStateTransitionError{expected: stateStarted, actual: stateClosed}, updater.Commit())
	require.Equal(t, invalidStateTransitionError{expected: stateIdle, actual: stateClosed}, updater.Start())
	require.NoError(t, updater.Close())

	// Verify no references were created.
	refs, err := repo.GetReferences(ctx)
	require.NoError(t, err)
	require.Empty(t, refs)
}

func TestUpdater_update(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, _, repoPath, updater := setupUpdater(t, ctx)

	// The updater cancel should fail at the end of the test as the final operation is an error,
	// which results in closing the updater.
	defer func() { require.ErrorContains(t, updater.Close(), "closing updater: exit status 128") }()

	oldCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	newCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(oldCommitID))
	otherCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("other"))

	// Check that we can force-update the reference when we don't give an old object ID.
	require.NoError(t, updater.Start())
	require.NoError(t, updater.Update("refs/heads/main", newCommitID, ""))
	require.NoError(t, updater.Commit())
	require.Equal(t, gittest.ResolveRevision(t, cfg, repoPath, "refs/heads/main"), newCommitID)

	// Check that we can update with safety guards when giving an old commit ID.
	require.NoError(t, updater.Start())
	require.NoError(t, updater.Update("refs/heads/main", oldCommitID, newCommitID))
	require.NoError(t, updater.Commit())
	require.Equal(t, gittest.ResolveRevision(t, cfg, repoPath, "refs/heads/main"), oldCommitID)

	// And finally assert that we fail to update the reference in case we're trying to update
	// when the old commit ID doesn't match.
	require.NoError(t, updater.Start())
	require.NoError(t, updater.Update("refs/heads/main", newCommitID, otherCommitID))
	require.ErrorContains(t, updater.Commit(), fmt.Sprintf("fatal: commit: cannot lock ref 'refs/heads/main': is at %s but expected %s", oldCommitID, otherCommitID))
	require.Equal(t, invalidStateTransitionError{expected: stateIdle, actual: stateClosed}, updater.Start())

	require.Equal(t, gittest.ResolveRevision(t, cfg, repoPath, "refs/heads/main"), oldCommitID)
}

func TestUpdater_delete(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath, updater := setupUpdater(t, ctx)
	defer testhelper.MustClose(t, updater)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	require.NoError(t, updater.Start())
	require.NoError(t, updater.Delete("refs/heads/main"))
	require.NoError(t, updater.Commit())

	// Check that the reference was removed.
	_, err := repo.ReadCommit(ctx, "refs/heads/main")
	require.Equal(t, localrepo.ErrObjectNotFound, err)
}

func TestUpdater_prepareLocksTransaction(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, _, repoPath, updater := setupUpdater(t, ctx)
	defer testhelper.MustClose(t, updater)

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	require.NoError(t, updater.Start())
	require.NoError(t, updater.Update("refs/heads/feature", commitID, ""))
	require.NoError(t, updater.Prepare())

	require.Equal(t, invalidStateTransitionError{expected: stateStarted, actual: statePrepared}, updater.Update("refs/heads/feature", commitID, ""))
	require.Equal(t, invalidStateTransitionError{expected: stateStarted, actual: stateClosed}, updater.Commit())
}

func TestUpdater_invalidReferenceName(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto, git.WithSkipHooks())
	commitID := gittest.WriteCommit(t, cfg, repoPath)

	updater, err := New(ctx, repo)
	require.NoError(t, err)
	defer func() { require.ErrorContains(t, updater.Close(), "closing updater: exit status 128") }()

	const referenceName = `refs/heads\master`
	require.NoError(t, updater.Start())
	require.NoError(t, updater.Update(referenceName, commitID, ""))
	require.Equal(t, ErrInvalidReferenceFormat{ReferenceName: referenceName}, updater.Prepare())
}

func TestUpdater_concurrentLocking(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto, git.WithSkipHooks())

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	// Create the first updater that prepares the reference transaction so that the reference
	// we're about to update is locked.
	firstUpdater, err := New(ctx, repo)
	require.NoError(t, err)
	require.NoError(t, firstUpdater.Start())
	require.NoError(t, firstUpdater.Update("refs/heads/master", commitID, ""))
	require.NoError(t, firstUpdater.Prepare())

	// Now we create a second updater that tries to update the same reference.
	secondUpdater, err := New(ctx, repo)
	require.NoError(t, err)
	require.NoError(t, secondUpdater.Start())
	require.NoError(t, secondUpdater.Update("refs/heads/master", commitID, ""))

	// Preparing this second updater should fail though because we notice that the reference is
	// locked.
	err = secondUpdater.Prepare()
	var errAlreadyLocked *ErrAlreadyLocked
	require.ErrorAs(t, err, &errAlreadyLocked)
	require.Equal(t, err, &ErrAlreadyLocked{
		Ref: "refs/heads/master",
	})

	// Whereas committing the first transaction should succeed.
	require.NoError(t, firstUpdater.Commit())
}

func TestUpdater_bulkOperation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath, updater := setupUpdater(t, ctx)
	defer testhelper.MustClose(t, updater)

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	var expectedRefs []git.Reference
	for i := 0; i < 2; i++ {
		require.NoError(t, updater.Start())

		for j := 0; j < 2; j++ {
			reference := git.Reference{
				Name:   git.ReferenceName(fmt.Sprintf("refs/head/test_%d_%d", j, i)),
				Target: commitID.String(),
			}

			require.NoError(t, updater.Create(reference.Name, commitID))
			expectedRefs = append(expectedRefs, reference)
		}

		require.NoError(t, updater.Commit())
	}

	refs, err := repo.GetReferences(ctx, "refs/")
	require.NoError(t, err)
	require.ElementsMatch(t, expectedRefs, refs)
}

func TestUpdater_contextCancellation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	childCtx, childCancel := context.WithCancel(ctx)

	cfg, repoProto, repoPath, _ := setupUpdater(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	updater, err := New(childCtx, repo)
	require.NoError(t, err)

	require.NoError(t, updater.Start())
	require.NoError(t, updater.Create("refs/heads/main", commitID))

	// Force the update-ref process to terminate early by cancelling the context.
	childCancel()

	// We should see that committing the update fails now.
	require.Error(t, updater.Commit())

	// And the reference should not have been created.
	_, err = repo.ReadCommit(ctx, "refs/heads/main")
	require.Equal(t, localrepo.ErrObjectNotFound, err)
}

func TestUpdater_cancel(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath, firstUpdater := setupUpdater(t, ctx)
	defer testhelper.MustClose(t, firstUpdater)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	// Queue the branch for deletion and lock it.
	require.NoError(t, firstUpdater.Start())
	require.NoError(t, firstUpdater.Delete(git.ReferenceName("refs/heads/main")))
	require.NoError(t, firstUpdater.Prepare())

	// Try to delete the same reference via a concurrent updater. This should not be allowed
	// because the reference is locked already.
	failingUpdater, err := New(ctx, repo)
	require.NoError(t, err)
	defer func() { require.ErrorContains(t, failingUpdater.Close(), "closing updater: exit status 128") }()

	require.NoError(t, failingUpdater.Start())
	require.NoError(t, failingUpdater.Delete(git.ReferenceName("refs/heads/main")))
	require.ErrorContains(t, failingUpdater.Commit(), "fatal: commit: cannot lock ref 'refs/heads/main'")

	// We now cancel the initial updater. Afterwards, it should be possible again to update the
	// ref because locks should have been released.
	require.NoError(t, firstUpdater.Close())

	secondUpdater, err := New(ctx, repo)
	require.NoError(t, err)
	defer testhelper.MustClose(t, secondUpdater)

	require.NoError(t, secondUpdater.Start())
	require.NoError(t, secondUpdater.Delete(git.ReferenceName("refs/heads/main")))
	require.NoError(t, secondUpdater.Commit())
}

func TestUpdater_closingStdinAbortsChanges(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath, updater := setupUpdater(t, ctx)
	defer testhelper.MustClose(t, updater)

	commitID := gittest.WriteCommit(t, cfg, repoPath)

	require.NoError(t, updater.Start())
	require.NoError(t, updater.Create("refs/heads/main", commitID))

	// Note that we call `Wait()` on the command, not on the updater. This
	// circumvents our usual semantics of sending "commit" and thus
	// emulates that the command somehow terminates correctly without us
	// terminating it intentionally. Previous to our use of the "start"
	// verb, this would've caused the reference to be created...
	require.NoError(t, updater.cmd.Wait())

	// ... but as we now use explicit transactional behaviour, this is no
	// longer the case.
	_, err := repo.ReadCommit(ctx, "refs/heads/main")
	require.Equal(t, localrepo.ErrObjectNotFound, err, "expected 'not found' error got %v", err)
}

func TestUpdater_capturesStderr(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	_, _, _, updater := setupUpdater(t, ctx)
	defer func() { require.ErrorContains(t, updater.Close(), "closing updater: exit status 128") }()

	newValue := git.ObjectID(strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen()))
	oldValue := gittest.DefaultObjectHash.ZeroOID

	require.NoError(t, updater.Start())
	require.NoError(t, updater.Update("refs/heads/main", newValue, oldValue))

	expectedErr := fmt.Sprintf("state update to \"commit\" failed: EOF, stderr: \"fatal: commit: cannot update ref '%[1]s': "+
		"trying to write ref '%[1]s' with nonexistent object %[2]s\\n\"", "refs/heads/main", newValue)

	require.ErrorContains(t, updater.Commit(), expectedErr)
}
