package git

import (
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// DefaultCommitterName is the default name of the committer and author used to create
	// commits.
	DefaultCommitterName = "Scrooge McDuck"
	// DefaultCommitterMail is the default mail of the committer and author used to create
	// commits.
	DefaultCommitterMail = "scrooge@mcduck.com"
	// DefaultCommitTime is the default time used as written by WriteCommit().
	DefaultCommitTime = time.Date(2019, 11, 3, 11, 27, 59, 0, time.FixedZone("", 60*60))
	// DefaultCommitterSignature is the default signature in the format like it would be present
	// in commits: "$name <$email> $unixtimestamp $timezone".
	DefaultCommitterSignature = fmt.Sprintf(
		"%s <%s> %d %s", DefaultCommitterName, DefaultCommitterMail, DefaultCommitTime.Unix(), DefaultCommitTime.Format("-0700"),
	)
	// DefaultCommitAuthor is the Protobuf message representation of the default committer and
	// author used to create commits.
	DefaultCommitAuthor = &gitalypb.CommitAuthor{
		Name:     []byte(DefaultCommitterName),
		Email:    []byte(DefaultCommitterMail),
		Date:     timestamppb.New(DefaultCommitTime),
		Timezone: []byte("+0100"),
	}
)
