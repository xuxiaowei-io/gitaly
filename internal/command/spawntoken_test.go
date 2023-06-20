package command

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestGetSpawnToken_CommandStats(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx = log.InitContextCustomFields(ctx)

	putToken, err := getSpawnToken(ctx)
	require.Nil(t, err)
	putToken()

	customFields := log.CustomFieldsFromContext(ctx)
	require.NotNil(t, customFields)
	require.Contains(t, customFields.Fields(), "command.spawn_token_wait_ms")
}

// This test modifies a global config, hence should never run in parallel
func TestGetSpawnToken_CommandStats_timeout(t *testing.T) {
	priorTimeout := spawnConfig.Timeout
	priorSpawnTokens := spawnTokens

	spawnConfig.Timeout = 1 * time.Millisecond
	spawnTokens = make(chan struct{}, 1)
	spawnTokens <- struct{}{}
	defer func() {
		spawnConfig.Timeout = priorTimeout
		spawnTokens = priorSpawnTokens
	}()

	ctx := testhelper.Context(t)
	ctx = log.InitContextCustomFields(ctx)

	_, err := getSpawnToken(ctx)

	var structErr structerr.Error
	require.ErrorAs(t, err, &structErr)
	details := structErr.Details()
	require.Len(t, details, 1)

	limitErr, ok := details[0].(*gitalypb.LimitError)
	require.True(t, ok)

	testhelper.RequireGrpcCode(t, err, codes.ResourceExhausted)
	require.Equal(t, "process spawn timed out after 1ms", limitErr.ErrorMessage)
	require.Equal(t, durationpb.New(0), limitErr.RetryAfter)

	customFields := log.CustomFieldsFromContext(ctx)
	require.NotNil(t, customFields)
	logrusFields := customFields.Fields()

	require.GreaterOrEqual(t, logrusFields["command.spawn_token_wait_ms"], 0)
	require.Equal(t, logrusFields["command.spawn_token_error"], "spawn token timeout")
}
