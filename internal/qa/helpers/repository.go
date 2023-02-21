package helpers

import (
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"time"
)

func NewRepository() *gitalypb.Repository {
	return &gitalypb.Repository{
		StorageName:  "default",
		RelativePath: fmt.Sprintf("/ginkgo/%d", time.Now().UnixNano()),
	}
}
