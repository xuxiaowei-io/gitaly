package wiki

import (
	"errors"

	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

type requestWithCommitDetails interface {
	GetCommitDetails() *gitalypb.WikiCommitDetails
}

func validateRequestCommitDetails(request requestWithCommitDetails) error {
	commitDetails := request.GetCommitDetails()
	if commitDetails == nil {
		return errors.New("empty CommitDetails")
	}

	if len(commitDetails.GetName()) == 0 {
		return errors.New("empty CommitDetails.Name")
	}

	if len(commitDetails.GetEmail()) == 0 {
		return errors.New("empty CommitDetails.Email")
	}

	if len(commitDetails.GetMessage()) == 0 {
		return errors.New("empty CommitDetails.Message")
	}

	return nil
}
