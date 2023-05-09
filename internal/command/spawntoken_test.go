package command

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestGetSpawnToken_CommandStats(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx = InitContextStats(ctx)

	putToken, err := getSpawnToken(ctx)
	require.Nil(t, err)
	putToken()

	stats := StatsFromContext(ctx)
	require.NotNil(t, stats)
	require.Contains(t, stats.Fields(), "command.spawn_token_wait_ms")
}

// This test modifies a global config, hence should never run in parallel
func TestGetSpawnToken_CommandStats_timeout(t *testing.T) {
	priorTimeout := spawnConfig.Timeout
	priorSpawnTokens := spawnTokens

	spawnConfig.Timeout = 0
	spawnTokens = make(chan struct{}, 1)
	spawnTokens <- struct{}{}
	defer func() {
		spawnConfig.Timeout = priorTimeout
		spawnTokens = priorSpawnTokens
	}()

	ctx := testhelper.Context(t)
	ctx = InitContextStats(ctx)

	_, err := getSpawnToken(ctx)
	require.ErrorContains(t, err, "process spawn timed out after")

	stats := StatsFromContext(ctx)
	require.NotNil(t, stats)
	fields := stats.Fields()

	require.GreaterOrEqual(t, fields["command.spawn_token_wait_ms"], 0)
	require.Equal(t, fields["command.spawn_token_error"], "spawn token timeout")
}
