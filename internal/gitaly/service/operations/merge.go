package operations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func validateMergeBranchRequest(locator storage.Locator, request *gitalypb.UserMergeBranchRequest) error {
	if err := locator.ValidateRepository(request.GetRepository()); err != nil {
		return err
	}

	if request.User == nil {
		return errors.New("empty user")
	}

	if len(request.User.Email) == 0 {
		return errors.New("empty user email")
	}

	if len(request.User.Name) == 0 {
		return errors.New("empty user name")
	}

	if len(request.Branch) == 0 {
		return errors.New("empty branch name")
	}

	if request.CommitId == "" {
		return errors.New("empty commit ID")
	}

	if len(request.Message) == 0 {
		return errors.New("empty message")
	}

	return nil
}

func (s *Server) merge(
	ctx context.Context,
	quarantineRepo *localrepo.Repo,
	authorName string,
	authorMail string,
	authorDate time.Time,
	committerName string,
	committerMail string,
	committerDate time.Time,
	message string,
	ours string,
	theirs string,
	squash bool,
	sign bool,
) (string, error) {
	treeOID, err := quarantineRepo.MergeTree(ctx, ours, theirs, localrepo.WithAllowUnrelatedHistories(), localrepo.WithConflictingFileNamesOnly())
	if err != nil {
		return "", err
	}

	parents := []git.ObjectID{git.ObjectID(ours)}
	if !squash {
		parents = append(parents, git.ObjectID(theirs))
	}

	cfg := localrepo.WriteCommitConfig{
		TreeID:         treeOID,
		Message:        message,
		Parents:        parents,
		AuthorName:     authorName,
		AuthorEmail:    authorMail,
		AuthorDate:     authorDate,
		CommitterName:  committerName,
		CommitterEmail: committerMail,
		CommitterDate:  committerDate,
	}
	if sign {
		cfg.SigningKey = s.signingKey
	}
	c, err := quarantineRepo.WriteCommit(
		ctx,
		cfg,
	)
	if err != nil {
		return "", fmt.Errorf("create commit from tree: %w", err)
	}

	return string(c), nil
}

// UserMergeBranch is a two stage streaming RPC that will merge two commits together and
// create a merge commit
func (s *Server) UserMergeBranch(stream gitalypb.OperationService_UserMergeBranchServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateMergeBranchRequest(s.locator, firstRequest); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	referenceName := git.NewReferenceNameFromBranchName(string(firstRequest.Branch))

	var revision git.ObjectID
	if expectedOldOID := firstRequest.GetExpectedOldOid(); expectedOldOID != "" {
		revision, err = git.ObjectHashSHA1.FromHex(expectedOldOID)
		if err != nil {
			return structerr.NewInvalidArgument("invalid expected old object ID: %w", err).WithMetadata("old_object_id", expectedOldOID)
		}
		revision, err = quarantineRepo.ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", revision)),
		)
		if err != nil {
			return structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		revision, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
		if err != nil {
			if errors.Is(err, git.ErrReferenceNotFound) {
				return structerr.NewNotFound("%w", err)
			}
			return structerr.NewInternal("branch resolution: %w", err)
		}
	}

	authorDate, err := dateFromProto(firstRequest)
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	mergeCommitID, err := s.merge(ctx, quarantineRepo,
		string(firstRequest.User.Name),
		string(firstRequest.User.Email),
		authorDate,
		string(firstRequest.User.Name),
		string(firstRequest.User.Email),
		authorDate,
		string(firstRequest.Message),
		revision.String(),
		firstRequest.CommitId,
		false,
		true,
	)
	if err != nil {
		var conflictErr *localrepo.MergeTreeConflictError
		if errors.As(err, &conflictErr) {
			conflictingFiles := make([][]byte, 0, len(conflictErr.ConflictingFileInfo))
			for _, conflictingFileInfo := range conflictErr.ConflictingFileInfo {
				conflictingFiles = append(conflictingFiles, []byte(conflictingFileInfo.FileName))
			}

			return structerr.NewFailedPrecondition("merging commits: %w", err).
				WithDetail(
					&gitalypb.UserMergeBranchError{
						Error: &gitalypb.UserMergeBranchError_MergeConflict{
							MergeConflict: &gitalypb.MergeConflictError{
								ConflictingFiles: conflictingFiles,
								ConflictingCommitIds: []string{
									revision.String(),
									firstRequest.CommitId,
								},
							},
						},
					},
				)
		}

		return structerr.NewInternal("merge: %w", err)
	}

	mergeOID, err := git.ObjectHashSHA1.FromHex(mergeCommitID)
	if err != nil {
		return structerr.NewInternal("could not parse merge ID: %w", err)
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		CommitId: mergeOID.String(),
	}); err != nil {
		return err
	}

	secondRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	if !secondRequest.Apply {
		return structerr.NewFailedPrecondition("merge aborted by client")
	}

	if err := s.updateReferenceWithHooks(ctx, firstRequest.GetRepository(), firstRequest.User, quarantineDir, referenceName, mergeOID, revision); err != nil {
		var notAllowedError hook.NotAllowedError
		var customHookErr updateref.CustomHookError
		var updateRefError updateref.Error

		if errors.As(err, &notAllowedError) {
			return structerr.NewPermissionDenied("%w", notAllowedError).WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: notAllowedError.Message,
							UserId:       notAllowedError.UserID,
							Protocol:     notAllowedError.Protocol,
							Changes:      notAllowedError.Changes,
						},
					},
				},
			)
		} else if errors.As(err, &customHookErr) {
			// When an error happens updating the reference, e.g. because of a
			// race with another update, then we should tell the user that a
			// precondition failed. A retry may fix this.
			return structerr.NewPermissionDenied("%w", customHookErr).WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				},
			)
		} else if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a
			// race with another update, then we should tell the user that a
			// precondition failed. A retry may fix this.
			return structerr.NewFailedPrecondition("%w", updateRefError).WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
						ReferenceUpdate: &gitalypb.ReferenceUpdateError{
							ReferenceName: []byte(updateRefError.Reference.String()),
							OldOid:        updateRefError.OldOID.String(),
							NewOid:        updateRefError.NewOID.String(),
						},
					},
				},
			)
		}

		return structerr.NewInternal("target update: %w", err)
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      mergeOID.String(),
			RepoCreated:   false,
			BranchCreated: false,
		},
	}); err != nil {
		return err
	}

	return nil
}

func validateFFRequest(locator storage.Locator, in *gitalypb.UserFFBranchRequest) error {
	if err := locator.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}

	if len(in.Branch) == 0 {
		return errors.New("empty branch name")
	}

	if in.User == nil {
		return errors.New("empty user")
	}

	if in.CommitId == "" {
		return errors.New("empty commit id")
	}

	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserFFBranch(ctx context.Context, in *gitalypb.UserFFBranchRequest) (*gitalypb.UserFFBranchResponse, error) {
	if err := validateFFRequest(s.locator, in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(in.Branch))

	// While we're creating a quarantine directory, we know that it won't ever have any new
	// objects given that we're doing a fast-forward merge. We still want to create one such
	// that Rails can efficiently compute new objects.
	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, in.GetRepository())
	if err != nil {
		return nil, err
	}

	var revision git.ObjectID
	if expectedOldOID := in.GetExpectedOldOid(); expectedOldOID != "" {
		objectHash, err := quarantineRepo.ObjectHash(ctx)
		if err != nil {
			return nil, structerr.NewInternal("detecting object hash: %w", err)
		}

		revision, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}

		revision, err = quarantineRepo.ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", revision)),
		)
		if err != nil {
			return nil, structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		revision, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
		if err != nil {
			return nil, structerr.NewInvalidArgument("%w", err)
		}
	}

	commitID, err := git.ObjectHashSHA1.FromHex(in.CommitId)
	if err != nil {
		return nil, structerr.NewInvalidArgument("cannot parse commit ID: %w", err)
	}

	ancestor, err := quarantineRepo.IsAncestor(ctx, revision.Revision(), commitID.Revision())
	if err != nil {
		return nil, structerr.NewInternal("checking for ancestry: %w", err)
	}
	if !ancestor {
		return nil, structerr.NewFailedPrecondition("not fast forward")
	}

	if err := s.updateReferenceWithHooks(ctx, in.GetRepository(), in.User, quarantineDir, referenceName, commitID, revision); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return &gitalypb.UserFFBranchResponse{
				PreReceiveError: customHookErr.Error(),
			}, nil
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			return &gitalypb.UserFFBranchResponse{}, nil
		}

		return nil, structerr.NewInternal("updating ref with hooks: %w", err)
	}

	return &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId: in.CommitId,
		},
	}, nil
}

func validateUserMergeToRefRequest(locator storage.Locator, in *gitalypb.UserMergeToRefRequest) error {
	if err := locator.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}

	if len(in.FirstParentRef) == 0 && len(in.Branch) == 0 {
		return errors.New("empty first parent ref and branch name")
	}

	if in.User == nil {
		return errors.New("empty user")
	}

	if in.SourceSha == "" {
		return errors.New("empty source SHA")
	}

	if len(in.TargetRef) == 0 {
		return errors.New("empty target ref")
	}

	if !strings.HasPrefix(string(in.TargetRef), "refs/merge-requests") {
		return errors.New("invalid target ref")
	}

	return nil
}

// UserMergeToRef overwrites the given TargetRef to point to either Branch or
// FirstParentRef. Afterwards, it performs a merge of SourceSHA with either
// Branch or FirstParentRef and updates TargetRef to the merge commit.
func (s *Server) UserMergeToRef(ctx context.Context, request *gitalypb.UserMergeToRefRequest) (*gitalypb.UserMergeToRefResponse, error) {
	if err := validateUserMergeToRefRequest(s.locator, request); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repoPath, err := s.locator.GetRepoPath(request.Repository, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return nil, err
	}

	repo := s.localrepo(request.GetRepository())

	revision := git.Revision(request.Branch)
	if request.FirstParentRef != nil {
		revision = git.Revision(request.FirstParentRef)
	}

	oid, err := repo.ResolveRevision(ctx, revision)
	if err != nil {
		return nil, structerr.NewInvalidArgument("Invalid merge source")
	}

	sourceOID, err := repo.ResolveRevision(ctx, git.Revision(request.SourceSha))
	if err != nil {
		return nil, structerr.NewInvalidArgument("Invalid merge source")
	}

	authorDate, err := dateFromProto(request)
	if err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	// Initialize oldTargetOID from expected_old_oid when provided, otherwise
	// resolve it from target_ref. This will be used as an optimistic lock when
	// we finally update target_ref, to ensure it hasn't changed in the
	// meantime.
	var oldTargetOID git.ObjectID
	if expectedOldOID := request.GetExpectedOldOid(); expectedOldOID != "" {
		objectHash, err := repo.ObjectHash(ctx)
		if err != nil {
			return nil, structerr.NewInternal("detecting object hash: %w", err)
		}

		oldTargetOID, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid expected old object ID: %w", err).WithMetadata("old_object_id", expectedOldOID)
		}

		oldTargetOID, err = repo.ResolveRevision(
			ctx, git.Revision(fmt.Sprintf("%s^{object}", oldTargetOID)),
		)
		if err != nil {
			return nil, structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else if targetRef, err := repo.GetReference(ctx, git.ReferenceName(request.TargetRef)); err == nil {
		if targetRef.IsSymbolic {
			return nil, structerr.NewFailedPrecondition("target reference is symbolic: %q", request.TargetRef)
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

	var mergeCommitID string
	if featureflag.MergeToRefWithGit.IsEnabled(ctx) {
		mergeCommitID, err = s.merge(
			ctx,
			repo,
			string(request.User.Name),
			string(request.User.Email),
			authorDate,
			string(request.User.Name),
			string(request.User.Email),
			authorDate,
			string(request.Message),
			oid.String(),
			sourceOID.String(),
			false,
			false,
		)
	} else {
		mergeCommitID, err = s.mergeWithGit2Go(
			ctx,
			repo,
			repoPath,
			string(request.User.Name),
			string(request.User.Email),
			authorDate,
			string(request.User.Name),
			string(request.User.Email),
			authorDate,

			string(request.Message),
			oid.String(),
			sourceOID.String(),
			false,
		)
	}
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).WithFields(
			logrus.Fields{
				"source_sha": sourceOID,
				"target_sha": oid,
				"target_ref": string(request.TargetRef),
			},
		).Error("unable to create merge commit")

		if errors.Is(err, git2go.ErrInvalidArgument) {
			return nil, structerr.NewInvalidArgument("%w", err)
		}
		return nil, structerr.NewFailedPrecondition("Failed to create merge commit for source_sha %s and target_sha %s at %s",
			sourceOID, oid, string(request.TargetRef))
	}

	mergeOID, err := git.ObjectHashSHA1.FromHex(mergeCommitID)
	if err != nil {
		return nil, structerr.NewInternal("parsing merge commit SHA: %w", err)
	}

	// ... and move branch from target ref to the merge commit. The Ruby
	// implementation doesn't invoke hooks, so we don't either.
	if err := repo.UpdateRef(ctx, git.ReferenceName(request.TargetRef), mergeOID, oldTargetOID); err != nil {
		return nil, structerr.NewFailedPrecondition("Could not update %s. Please refresh and try again", string(request.TargetRef))
	}

	return &gitalypb.UserMergeToRefResponse{
		CommitId: mergeOID.String(),
	}, nil
}

func (s *Server) mergeWithGit2Go(
	ctx context.Context,
	repo *localrepo.Repo,
	repoPath string,
	authorName string,
	authorEmail string,
	authorDate time.Time,
	committerName string,
	committerEmail string,
	committerDate time.Time,
	message string,
	ours string,
	theirs string,
	squash bool,
) (string, error) {
	merge, err := s.git2goExecutor.Merge(ctx, repo, git2go.MergeCommand{
		Repository:    repoPath,
		AuthorName:    authorName,
		AuthorMail:    authorEmail,
		AuthorDate:    authorDate,
		CommitterName: committerName,
		CommitterMail: committerEmail,
		CommitterDate: committerDate,
		Message:       message,
		Ours:          ours,
		Theirs:        theirs,
		Squash:        squash,
	})
	if err != nil {
		return "", err
	}

	return merge.CommitID, nil
}
