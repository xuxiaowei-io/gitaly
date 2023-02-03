package operations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// UserCherryPick tries to perform a cherry-pick of a given commit onto a
// branch. See the protobuf documentation for details.
func (s *Server) UserCherryPick(ctx context.Context, req *gitalypb.UserCherryPickRequest) (*gitalypb.UserCherryPickResponse, error) {
	if err := validateCherryPickOrRevertRequest(req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	startRevision, err := s.fetchStartRevision(ctx, quarantineRepo, req)
	if err != nil {
		return nil, err
	}

	repoHadBranches, err := quarantineRepo.HasBranches(ctx)
	if err != nil {
		return nil, structerr.NewInternal("has branches: %w", err)
	}

	repoPath, err := quarantineRepo.Path()
	if err != nil {
		return nil, err
	}

	var mainline uint
	if len(req.Commit.ParentIds) > 1 {
		mainline = 1
	}

	committerDate := time.Now()
	if req.Timestamp != nil {
		committerDate = req.Timestamp.AsTime()
	}

	newrev, err := s.git2goExecutor.CherryPick(ctx, quarantineRepo, git2go.CherryPickCommand{
		Repository:    repoPath,
		CommitterName: string(req.User.Name),
		CommitterMail: string(req.User.Email),
		CommitterDate: committerDate,
		Message:       string(req.Message),
		Commit:        req.Commit.Id,
		Ours:          startRevision.String(),
		Mainline:      mainline,
	})
	if err != nil {
		var conflictErr git2go.ConflictingFilesError
		var emptyErr git2go.EmptyError

		switch {
		case errors.As(err, &conflictErr):
			conflictingFiles := make([][]byte, 0, len(conflictErr.ConflictingFiles))
			for _, conflictingFile := range conflictErr.ConflictingFiles {
				conflictingFiles = append(conflictingFiles, []byte(conflictingFile))
			}

			return nil, structerr.NewFailedPrecondition("cherry pick: %w", err).WithDetail(
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_CherryPickConflict{
						CherryPickConflict: &gitalypb.MergeConflictError{
							ConflictingFiles: conflictingFiles,
						},
					},
				},
			)
		case errors.As(err, &emptyErr):
			return nil, structerr.NewFailedPrecondition("%w", err).WithDetail(
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_ChangesAlreadyApplied{},
				},
			)
		case errors.As(err, &git2go.CommitNotFoundError{}):
			return nil, structerr.NewNotFound("%w", err)
		case errors.Is(err, git2go.ErrInvalidArgument):
			return nil, structerr.NewInvalidArgument("%w", err)
		default:
			return nil, structerr.NewInternal("cherry-pick command: %w", err)
		}
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.BranchName))
	branchCreated := false
	var oldrev git.ObjectID
	if expectedOldOID := req.GetExpectedOldOid(); expectedOldOID != "" {
		oldrev, err = git.ObjectHashSHA1.FromHex(expectedOldOID)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
		oldrev, err = s.localrepo(req.GetRepository()).ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", oldrev)),
		)
		if err != nil {
			return nil, structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		oldrev, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision()+"^{commit}")
		if errors.Is(err, git.ErrReferenceNotFound) {
			branchCreated = true
			oldrev = git.ObjectHashSHA1.ZeroOID
		} else if err != nil {
			return nil, structerr.NewInvalidArgument("resolve ref: %w", err)
		}
	}

	if req.DryRun {
		newrev = startRevision
	}

	if !branchCreated {
		ancestor, err := quarantineRepo.IsAncestor(ctx, oldrev.Revision(), newrev.Revision())
		if err != nil {
			return nil, structerr.NewInternal("checking for ancestry: %w", err)
		}
		if !ancestor {
			return nil, structerr.NewFailedPrecondition("cherry-pick: branch diverged").WithDetail(
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_TargetBranchDiverged{
						TargetBranchDiverged: &gitalypb.NotAncestorError{
							ParentRevision: []byte(oldrev.Revision()),
							ChildRevision:  []byte(newrev),
						},
					},
				},
			)
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.User, quarantineDir, referenceName, newrev, oldrev); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return nil, structerr.NewFailedPrecondition("access check failed").WithDetail(
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: strings.TrimSuffix(customHookErr.Error(), "\n"),
						},
					},
				},
			)
		}

		return nil, structerr.NewInternal("update reference with hooks: %w", err)
	}

	return &gitalypb.UserCherryPickResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      newrev.String(),
			BranchCreated: branchCreated,
			RepoCreated:   !repoHadBranches,
		},
	}, nil
}
