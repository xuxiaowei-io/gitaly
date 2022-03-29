package repocleaner

import (
	"context"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// CleanAction is an implementation of the Action interface that moves
// repositories to a lost+found folder in the root storage directory.
type CleanAction struct {
	logger logrus.FieldLogger
	conns  praefect.Connections
}

// NewCleanAction returns a new instance of CleanAction
func NewCleanAction(logger logrus.FieldLogger, conns praefect.Connections) *CleanAction {
	return &CleanAction{
		logger: logger.WithField("component", "repocleaner.remove_repo_action"),
		conns:  conns,
	}
}

// Perform performs the action of moving repositories not known to Praefect to a
// lost+found folder in the storage root folder.
func (r *CleanAction) Perform(ctx context.Context, virtualStorage, storage string, replicaPaths []string) error {
	l := r.logger.WithFields(logrus.Fields{
		"virtual_storage": virtualStorage,
		"storage":         storage,
	})

	l.WithField("cleaned_repos", replicaPaths).
		Warn("cleaned repositories not managed by praefect")

	client, err := getInternalGitalyClient(r.conns, virtualStorage, storage)
	if err != nil {
		return err
	}

	if _, err := client.CleanRepos(ctx, &gitalypb.CleanReposRequest{
		StorageName:   storage,
		RelativePaths: replicaPaths,
	}); err != nil {
		return err
	}

	return nil
}
