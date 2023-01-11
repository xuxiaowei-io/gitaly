package operations

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	if err := validateUserUpdateSubmoduleRequest(req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	return s.userUpdateSubmodule(ctx, req)
}

func validateUserUpdateSubmoduleRequest(req *gitalypb.UserUpdateSubmoduleRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if req.GetUser() == nil {
		return errors.New("empty User")
	}

	if req.GetCommitSha() == "" {
		return errors.New("empty CommitSha")
	}

	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, req.GetCommitSha()); !match || err != nil {
		return errors.New("invalid CommitSha")
	}

	if len(req.GetBranch()) == 0 {
		return errors.New("empty Branch")
	}

	if len(req.GetSubmodule()) == 0 {
		return errors.New("empty Submodule")
	}

	if len(req.GetCommitMessage()) == 0 {
		return errors.New("empty CommitMessage")
	}

	return nil
}

func (s *Server) userUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	branches, err := quarantineRepo.GetBranches(ctx)
	if err != nil {
		return nil, structerr.NewInternal("get branches: %w", err)
	}
	if len(branches) == 0 {
		return &gitalypb.UserUpdateSubmoduleResponse{
			CommitError: "Repository is empty",
		}, nil
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.GetBranch()))

	var oldOID git.ObjectID
	if expectedOldOID := req.GetExpectedOldOid(); expectedOldOID != "" {
		objectHash, err := quarantineRepo.ObjectHash(ctx)
		if err != nil {
			return nil, structerr.NewInternal("detecting object hash: %w", err)
		}

		oldOID, err = objectHash.FromHex(expectedOldOID)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid expected old object ID: %w", err).WithMetadata("old_object_id", expectedOldOID)
		}

		oldOID, err = quarantineRepo.ResolveRevision(ctx, git.Revision(fmt.Sprintf("%s^{object}", oldOID)))
		if err != nil {
			return nil, structerr.NewInvalidArgument("cannot resolve expected old object ID: %w", err).
				WithMetadata("old_object_id", expectedOldOID)
		}
	} else {
		oldOID, err = quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
		if err != nil {
			if errors.Is(err, git.ErrReferenceNotFound) {
				return nil, structerr.NewInvalidArgument("Cannot find branch")
			}
			return nil, structerr.NewInternal("resolving revision: %w", err)
		}
	}

	repoPath, err := quarantineRepo.Path()
	if err != nil {
		return nil, fmt.Errorf("locate repo: %w", err)
	}

	authorDate, err := dateFromProto(req)
	if err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	result, err := s.git2goExecutor.Submodule(ctx, quarantineRepo, git2go.SubmoduleCommand{
		Repository: repoPath,
		AuthorMail: string(req.GetUser().GetEmail()),
		AuthorName: string(req.GetUser().GetName()),
		AuthorDate: authorDate,
		Branch:     string(req.GetBranch()),
		CommitSHA:  req.GetCommitSha(),
		Submodule:  string(req.GetSubmodule()),
		Message:    string(req.GetCommitMessage()),
	})
	if err != nil {
		errStr := strings.TrimPrefix(err.Error(), "submodule: ")
		errStr = strings.TrimSpace(errStr)

		var resp *gitalypb.UserUpdateSubmoduleResponse
		for _, legacyErr := range []string{
			git2go.LegacyErrPrefixInvalidBranch,
			git2go.LegacyErrPrefixInvalidSubmodulePath,
			git2go.LegacyErrPrefixFailedCommit,
		} {
			if strings.HasPrefix(errStr, legacyErr) {
				resp = &gitalypb.UserUpdateSubmoduleResponse{
					CommitError: legacyErr,
				}
				ctxlogrus.
					Extract(ctx).
					WithError(err).
					Error("UserUpdateSubmodule: git2go subcommand failure")
				break
			}
		}
		if strings.Contains(errStr, "is already at") {
			resp = &gitalypb.UserUpdateSubmoduleResponse{
				CommitError: errStr,
			}
		}
		if resp != nil {
			return resp, nil
		}
		return nil, structerr.NewInternal("submodule subcommand: %w", err)
	}

	commitID, err := git.ObjectHashSHA1.FromHex(result.CommitID)
	if err != nil {
		return nil, structerr.NewInvalidArgument("cannot parse commit ID: %w", err)
	}

	if err := s.updateReferenceWithHooks(
		ctx,
		req.GetRepository(),
		req.GetUser(),
		quarantineDir,
		referenceName,
		commitID,
		oldOID,
	); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return &gitalypb.UserUpdateSubmoduleResponse{
				PreReceiveError: customHookErr.Error(),
			}, nil
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			return &gitalypb.UserUpdateSubmoduleResponse{
				CommitError: err.Error(),
			}, nil
		}

		return nil, structerr.NewInternal("updating ref with hooks: %w", err)
	}

	return &gitalypb.UserUpdateSubmoduleResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      result.CommitID,
			BranchCreated: false,
			RepoCreated:   false,
		},
	}, nil
}
