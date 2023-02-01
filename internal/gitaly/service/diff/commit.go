package diff

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

type requestWithLeftRightCommitIds interface {
	GetRepository() *gitalypb.Repository
	GetLeftCommitId() string
	GetRightCommitId() string
}

func (s *server) CommitDiff(in *gitalypb.CommitDiffRequest, stream gitalypb.DiffService_CommitDiffServer) error {
	ctxlogrus.Extract(stream.Context()).WithFields(log.Fields{
		"LeftCommitId":  in.LeftCommitId,
		"RightCommitId": in.RightCommitId,
		//nolint:staticcheck // This is a deprecated field and will be remove in a upcoming release
		"IgnoreWhitespaceChange": in.IgnoreWhitespaceChange,
		"Paths":                  logPaths(in.Paths),
	}).Debug("CommitDiff")

	if err := validateRequest(in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	leftSha := in.LeftCommitId
	rightSha := in.RightCommitId
	//nolint:staticcheck // This is a deprecated field and will be remove in a upcoming release
	ignoreWhitespaceChange := in.GetIgnoreWhitespaceChange()
	whitespaceChanges := in.GetWhitespaceChanges()
	paths := in.GetPaths()

	cmd := git.Command{
		Name: "diff",
		Flags: []git.Option{
			git.Flag{Name: "--patch"},
			git.Flag{Name: "--raw"},
			git.Flag{Name: "--abbrev=40"},
			git.Flag{Name: "--full-index"},
			git.Flag{Name: "--find-renames=30%"},
		},
		Args: []string{leftSha, rightSha},
	}

	if whitespaceChanges == gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE_ALL {
		cmd.Flags = append(cmd.Flags, git.Flag{Name: "--ignore-all-space"})
	} else if whitespaceChanges == gitalypb.CommitDiffRequest_WHITESPACE_CHANGES_IGNORE || ignoreWhitespaceChange {
		// ignoreWhitespaceChange is a deprecated field which we will eventually remove, for
		// now when `whitespaceChanges` is undefined we refer to ignoreWhitespaceChange.
		cmd.Flags = append(cmd.Flags, git.Flag{Name: "--ignore-space-change"})
	}

	if in.GetDiffMode() == gitalypb.CommitDiffRequest_WORDDIFF {
		cmd.Flags = append(cmd.Flags, git.Flag{Name: "--word-diff=porcelain"})
	}
	if len(paths) > 0 {
		for _, path := range paths {
			cmd.PostSepArgs = append(cmd.PostSepArgs, string(path))
		}
	}

	var limits diff.Limits
	if in.EnforceLimits {
		limits.EnforceLimits = true
		limits.MaxFiles = int(in.MaxFiles)
		limits.MaxLines = int(in.MaxLines)
		limits.MaxBytes = int(in.MaxBytes)
		limits.MaxPatchBytes = int(in.MaxPatchBytes)

		if len(in.MaxPatchBytesForFileExtension) > 0 {
			limits.MaxPatchBytesForFileExtension = map[string]int{}

			for extension, size := range in.MaxPatchBytesForFileExtension {
				limits.MaxPatchBytesForFileExtension[extension] = int(size)
			}
		}
	}
	limits.CollapseDiffs = in.CollapseDiffs
	limits.SafeMaxFiles = int(in.SafeMaxFiles)
	limits.SafeMaxLines = int(in.SafeMaxLines)
	limits.SafeMaxBytes = int(in.SafeMaxBytes)

	return s.eachDiff(stream.Context(), in.Repository, cmd, limits, func(diff *diff.Diff) error {
		response := &gitalypb.CommitDiffResponse{
			FromPath:       diff.FromPath,
			ToPath:         diff.ToPath,
			FromId:         diff.FromID,
			ToId:           diff.ToID,
			OldMode:        diff.OldMode,
			NewMode:        diff.NewMode,
			Binary:         diff.Binary,
			OverflowMarker: diff.OverflowMarker,
			Collapsed:      diff.Collapsed,
			TooLarge:       diff.TooLarge,
		}

		if len(diff.Patch) <= s.MsgSizeThreshold {
			response.RawPatchData = diff.Patch
			response.EndOfPatch = true

			if err := stream.Send(response); err != nil {
				return structerr.NewUnavailable("send: %w", err)
			}
		} else {
			patch := diff.Patch

			for len(patch) > 0 {
				if len(patch) > s.MsgSizeThreshold {
					response.RawPatchData = patch[:s.MsgSizeThreshold]
					patch = patch[s.MsgSizeThreshold:]
				} else {
					response.RawPatchData = patch
					response.EndOfPatch = true
					patch = nil
				}

				if err := stream.Send(response); err != nil {
					return structerr.NewUnavailable("send: %w", err)
				}

				// Use a new response so we don't send other fields (FromPath, ...) over and over
				response = &gitalypb.CommitDiffResponse{}
			}
		}

		return nil
	})
}

func (s *server) CommitDelta(in *gitalypb.CommitDeltaRequest, stream gitalypb.DiffService_CommitDeltaServer) error {
	ctxlogrus.Extract(stream.Context()).WithFields(log.Fields{
		"LeftCommitId":  in.LeftCommitId,
		"RightCommitId": in.RightCommitId,
		"Paths":         logPaths(in.Paths),
	}).Debug("CommitDelta")

	if err := validateRequest(in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	leftSha := in.LeftCommitId
	rightSha := in.RightCommitId
	paths := in.GetPaths()

	cmd := git.Command{
		Name: "diff",
		Flags: []git.Option{
			git.Flag{Name: "--raw"},
			git.Flag{Name: "--abbrev=40"},
			git.Flag{Name: "--full-index"},
			git.Flag{Name: "--find-renames"},
		},
		Args: []string{leftSha, rightSha},
	}
	if len(paths) > 0 {
		for _, path := range paths {
			cmd.PostSepArgs = append(cmd.PostSepArgs, string(path))
		}
	}

	var batch []*gitalypb.CommitDelta
	var batchSize int

	flushFunc := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := stream.Send(&gitalypb.CommitDeltaResponse{Deltas: batch}); err != nil {
			return structerr.NewUnavailable("send: %w", err)
		}

		return nil
	}

	err := s.eachDiff(stream.Context(), in.Repository, cmd, diff.Limits{}, func(diff *diff.Diff) error {
		delta := &gitalypb.CommitDelta{
			FromPath: diff.FromPath,
			ToPath:   diff.ToPath,
			FromId:   diff.FromID,
			ToId:     diff.ToID,
			OldMode:  diff.OldMode,
			NewMode:  diff.NewMode,
		}

		batch = append(batch, delta)
		batchSize += deltaSize(diff)

		if batchSize > s.MsgSizeThreshold {
			if err := flushFunc(); err != nil {
				return err
			}

			batch = nil
			batchSize = 0
		}

		return nil
	})
	if err != nil {
		return err
	}

	return flushFunc()
}

func validateRequest(in requestWithLeftRightCommitIds) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if in.GetLeftCommitId() == "" {
		return errors.New("empty LeftCommitId")
	}
	if in.GetRightCommitId() == "" {
		return errors.New("empty RightCommitId")
	}

	return nil
}

func (s *server) eachDiff(ctx context.Context, repo *gitalypb.Repository, subCmd git.Command, limits diff.Limits, callback func(*diff.Diff) error) error {
	diffConfig := git.ConfigPair{Key: "diff.noprefix", Value: "false"}

	cmd, err := s.gitCmdFactory.New(ctx, repo, subCmd, git.WithConfig(diffConfig))
	if err != nil {
		return structerr.NewInternal("cmd: %w", err)
	}

	diffParser := diff.NewDiffParser(cmd, limits)

	for diffParser.Parse() {
		if err := callback(diffParser.Diff()); err != nil {
			return err
		}
	}

	if err := diffParser.Err(); err != nil {
		return structerr.NewInternal("parse failure: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return structerr.NewUnavailable("%w", err)
	}

	return nil
}

func deltaSize(diff *diff.Diff) int {
	size := len(diff.FromID) + len(diff.ToID) +
		4 + 4 + // OldMode and NewMode are int32 = 32/8 = 4 bytes
		len(diff.FromPath) + len(diff.ToPath)

	return size
}

func logPaths(paths [][]byte) []string {
	result := make([]string, len(paths))
	for i, p := range paths {
		result[i] = string(p)
	}
	return result
}
