package operations

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
)

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
