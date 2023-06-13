package operations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserRebaseConfirmable(stream gitalypb.OperationService_UserRebaseConfirmableServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return structerr.NewInvalidArgument("empty UserRebaseConfirmableRequest.Header")
	}

	if err := validateUserRebaseConfirmableHeader(header); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	ctx := stream.Context()

	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, header.GetRepository())
	if err != nil {
		return structerr.NewInternal("creating repo quarantine: %w", err)
	}

	repoPath, err := quarantineRepo.Path()
	if err != nil {
		return err
	}

	branch := git.NewReferenceNameFromBranchName(string(header.Branch))
	oldrev, err := git.ObjectHashSHA1.FromHex(header.BranchSha)
	if err != nil {
		return structerr.NewNotFound("%w", err)
	}

	remoteFetch := rebaseRemoteFetch{header: header}
	startRevision, err := s.fetchStartRevision(ctx, quarantineRepo, remoteFetch)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	committer := git2go.NewSignature(string(header.User.Name), string(header.User.Email), time.Now())
	if header.Timestamp != nil {
		committer.When = header.Timestamp.AsTime()
	}

	newrev, err := s.git2goExecutor.Rebase(ctx, quarantineRepo, git2go.RebaseCommand{
		Repository:       repoPath,
		Committer:        committer,
		CommitID:         oldrev,
		UpstreamCommitID: startRevision,
		SkipEmptyCommits: true,
	})
	if err != nil {
		var conflictErr git2go.ConflictingFilesError
		if errors.As(err, &conflictErr) {
			conflictingFiles := make([][]byte, 0, len(conflictErr.ConflictingFiles))
			for _, conflictingFile := range conflictErr.ConflictingFiles {
				conflictingFiles = append(conflictingFiles, []byte(conflictingFile))
			}

			return structerr.NewFailedPrecondition("rebasing commits: %w", err).WithDetail(
				&gitalypb.UserRebaseConfirmableError{
					Error: &gitalypb.UserRebaseConfirmableError_RebaseConflict{
						RebaseConflict: &gitalypb.MergeConflictError{
							ConflictingFiles: conflictingFiles,
							ConflictingCommitIds: []string{
								startRevision.String(),
								oldrev.String(),
							},
						},
					},
				},
			)
		}

		return structerr.NewInternal("rebasing commits: %w", err)
	}

	if err := stream.Send(&gitalypb.UserRebaseConfirmableResponse{
		UserRebaseConfirmableResponsePayload: &gitalypb.UserRebaseConfirmableResponse_RebaseSha{
			RebaseSha: newrev.String(),
		},
	}); err != nil {
		return structerr.NewInternal("send rebase sha: %w", err)
	}

	secondRequest, err := stream.Recv()
	if err != nil {
		return structerr.NewInternal("recv: %w", err)
	}

	if !secondRequest.GetApply() {
		return structerr.NewFailedPrecondition("rebase aborted by client")
	}

	if err := s.updateReferenceWithHooks(
		ctx,
		header.GetRepository(),
		header.User,
		quarantineDir,
		branch,
		newrev,
		oldrev,
		header.GitPushOptions...,
	); err != nil {
		var customHookErr updateref.CustomHookError
		switch {
		case errors.As(err, &customHookErr):
			//nolint:gitaly-linters
			return structerr.NewPermissionDenied("access check: %q", err).WithDetail(
				&gitalypb.UserRebaseConfirmableError{
					Error: &gitalypb.UserRebaseConfirmableError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: customHookErr.Error(),
						},
					},
				},
			)
		case errors.Is(err, git2go.ErrInvalidArgument):
			return fmt.Errorf("update ref: %w", err)
		}

		return structerr.NewInternal("updating ref with hooks: %w", err)
	}

	return stream.Send(&gitalypb.UserRebaseConfirmableResponse{
		UserRebaseConfirmableResponsePayload: &gitalypb.UserRebaseConfirmableResponse_RebaseApplied{
			RebaseApplied: true,
		},
	})
}

// ErrInvalidBranch indicates a branch name is invalid
var ErrInvalidBranch = errors.New("invalid branch name")

func validateUserRebaseConfirmableHeader(header *gitalypb.UserRebaseConfirmableRequest_Header) error {
	if err := service.ValidateRepository(header.GetRepository()); err != nil {
		return err
	}

	if header.GetUser() == nil {
		return errors.New("empty User")
	}

	if header.GetBranch() == nil {
		return errors.New("empty Branch")
	}

	if header.GetBranchSha() == "" {
		return errors.New("empty BranchSha")
	}

	if header.GetRemoteRepository() == nil {
		return errors.New("empty RemoteRepository")
	}

	if header.GetRemoteBranch() == nil {
		return errors.New("empty RemoteBranch")
	}

	if err := git.ValidateRevision(header.GetRemoteBranch()); err != nil {
		return ErrInvalidBranch
	}

	return nil
}

// rebaseRemoteFetch is an intermediate type that implements the
// `requestFetchingStartRevision` interface. This allows us to use
// `fetchStartRevision` to get the revision to rebase onto.
type rebaseRemoteFetch struct {
	header *gitalypb.UserRebaseConfirmableRequest_Header
}

func (r rebaseRemoteFetch) GetRepository() *gitalypb.Repository {
	return r.header.GetRepository()
}

func (r rebaseRemoteFetch) GetBranchName() []byte {
	return r.header.GetBranch()
}

func (r rebaseRemoteFetch) GetStartRepository() *gitalypb.Repository {
	return r.header.GetRemoteRepository()
}

func (r rebaseRemoteFetch) GetStartBranchName() []byte {
	return r.header.GetRemoteBranch()
}

func validateUserRebaseToRefRequest(in *gitalypb.UserRebaseToRefRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}

	if len(in.FirstParentRef) == 0 {
		return errors.New("empty first_parent_ref")
	}

	if in.User == nil {
		return errors.New("empty user")
	}

	if in.SourceSha == "" {
		return errors.New("empty source_sha")
	}

	if len(in.TargetRef) == 0 {
		return errors.New("empty target_ref")
	}

	if !strings.HasPrefix(string(in.TargetRef), "refs/merge-requests") {
		return errors.New("invalid target_ref")
	}

	return nil
}

// UserRebaseToRef overwrites the given TargetRef with the result of rebasing
// SourceSHA on top of FirstParentRef, and returns the SHA of the HEAD of
// TargetRef.
func (s *Server) UserRebaseToRef(ctx context.Context, request *gitalypb.UserRebaseToRefRequest) (*gitalypb.UserRebaseToRefResponse, error) {
	if err := validateUserRebaseToRefRequest(request); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return nil, err
	}

	repo := s.localrepo(request.GetRepository())
	revision := git.Revision(request.FirstParentRef)

	oid, err := repo.ResolveRevision(ctx, revision)
	if err != nil {
		return nil, structerr.NewInvalidArgument("Invalid first_parent_ref")
	}

	sourceOID, err := repo.ResolveRevision(ctx, git.Revision(request.SourceSha))
	if err != nil {
		return nil, structerr.NewInvalidArgument("Invalid source_sha")
	}

	// Resolve the current state of the target reference. We do not care whether it
	// exists or not, but what we do want to assert is that the target reference doesn't
	// change while we compute the merge commit as a small protection against races.
	var oldTargetOID git.ObjectID
	if targetRef, err := repo.GetReference(ctx, git.ReferenceName(request.TargetRef)); err == nil {
		if targetRef.IsSymbolic {
			return nil, structerr.NewFailedPrecondition("Target reference is symbolic: %q", request.TargetRef)
		}

		oid, err := git.ObjectHashSHA1.FromHex(targetRef.Target)
		if err != nil {
			return nil, structerr.NewInternal("invalid target revision: %w", err)
		}

		oldTargetOID = oid
	} else if errors.Is(err, git.ErrReferenceNotFound) {
		oldTargetOID = git.ObjectHashSHA1.ZeroOID
	} else {
		return nil, structerr.NewInternal("could not read target reference: %w", err)
	}

	committer := git2go.NewSignature(string(request.User.Name), string(request.User.Email), time.Now())
	if request.Timestamp != nil {
		committer.When = request.Timestamp.AsTime()
	}

	rebasedOID, err := s.git2goExecutor.Rebase(ctx, repo, git2go.RebaseCommand{
		Repository:       repoPath,
		Committer:        committer,
		CommitID:         sourceOID,
		UpstreamCommitID: oid,
		SkipEmptyCommits: true,
	})

	if err != nil {
		var conflictErr git2go.ConflictingFilesError
		if errors.As(err, &conflictErr) {
			return nil, structerr.NewFailedPrecondition("Failed to rebase %s on %s while preparing %s due to conflict",
				sourceOID, oid, string(request.TargetRef))
		}

		return nil, structerr.NewInternal("rebasing commits: %w", err)
	}

	// ... and move branch from target ref to the merge commit. The Ruby
	// implementation doesn't invoke hooks, so we don't either.
	if err := repo.UpdateRef(ctx, git.ReferenceName(request.TargetRef), rebasedOID, oldTargetOID); err != nil {
		return nil, structerr.NewFailedPrecondition("Could not update %s. Please refresh and try again", string(request.TargetRef))
	}

	return &gitalypb.UserRebaseToRefResponse{
		CommitId: rebasedOID.String(),
	}, nil
}
