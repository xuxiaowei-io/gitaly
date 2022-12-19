package operations

import (
	"errors"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
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
