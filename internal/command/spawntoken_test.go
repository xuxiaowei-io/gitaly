package command

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestGetSpawnToken_CommandStats(t *testing.T) {
	ctx := testhelper.Context(t)
	ctx = InitContextStats(ctx)

	putToken, err := getSpawnToken(ctx)
	require.Nil(t, err)
	putToken()

	stats := StatsFromContext(ctx)
	require.NotNil(t, stats)
	require.Contains(t, stats.Fields(), "command.spawn_token_wait_ms")
}
