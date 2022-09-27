package operations

import (
	"errors"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type cherryPickOrRevertRequest interface {
	GetRepository() *gitalypb.Repository
	GetUser() *gitalypb.User
	GetCommit() *gitalypb.GitCommit
	GetBranchName() []byte
	GetMessage() []byte
}

func validateCherryPickOrRevertRequest(req cherryPickOrRevertRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if req.GetUser() == nil {
		return errors.New("empty User")
	}

	if req.GetCommit() == nil {
		return errors.New("empty Commit")
	}

	if len(req.GetBranchName()) == 0 {
		return errors.New("empty BranchName")
	}

	if len(req.GetMessage()) == 0 {
		return errors.New("empty Message")
	}

	return nil
}

type userTimestampProto interface {
	GetUser() *gitalypb.User
	GetTimestamp() *timestamppb.Timestamp
}

func dateFromProto(p userTimestampProto) (time.Time, error) {
	date := time.Now()

	if timestamp := p.GetTimestamp(); timestamp != nil {
		date = timestamp.AsTime()
	}

	if user := p.GetUser(); user != nil {
		location, err := time.LoadLocation(user.GetTimezone())
		if err != nil {
			return time.Time{}, err
		}
		date = date.In(location)
	}

	return date, nil
}
