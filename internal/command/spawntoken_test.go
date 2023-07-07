package command

import (
	"fmt"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestNewSpawnTokenManagerFromEnv mocks ENV variables, thus cannot run in parallel.
func TestNewSpawnTokenManagerFromEnv(t *testing.T) {
	for _, tc := range []struct {
		desc           string
		envs           map[string]string
		expectedErr    error
		expectedConfig SpawnConfig
	}{
		{
			desc: "spawn token ENVs are not set",
			expectedConfig: SpawnConfig{
				Timeout:     10 * time.Second,
				MaxParallel: 10,
			},
		},
		{
			desc: "spawn token ENVs are set correctly",
			envs: map[string]string{
				"GITALY_COMMAND_SPAWN_MAX_PARALLEL": "100",
				"GITALY_COMMAND_SPAWN_TIMEOUT":      "99s",
			},
			expectedConfig: SpawnConfig{
				Timeout:     99 * time.Second,
				MaxParallel: 100,
			},
		},
		{
			desc: "spawn token ENVs are set incorrectly",
			envs: map[string]string{
				"GITALY_COMMAND_SPAWN_MAX_PARALLEL": "100",
				"GITALY_COMMAND_SPAWN_TIMEOUT":      "hello",
			},
			expectedErr: &envconfig.ParseError{
				KeyName:   "GITALY_COMMAND_SPAWN_TIMEOUT",
				FieldName: "Timeout",
				TypeName:  "time.Duration",
				Value:     "hello",
				Err:       fmt.Errorf(`time: invalid duration "hello"`),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for key, value := range tc.envs {
				t.Setenv(key, value)
			}
			manager, err := NewSpawnTokenManagerFromEnv()
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				require.Nil(t, manager)
			} else {
				require.NoError(t, err)
				require.NotNil(t, manager)
				require.Equal(t, manager.spawnConfig, tc.expectedConfig)
			}
		})
	}
}

func TestGetSpawnToken_CommandStats(t *testing.T) {
	t.Parallel()

	ctx := log.InitContextCustomFields(testhelper.Context(t))
	manager := NewSpawnTokenManager(SpawnConfig{
		Timeout:     200 * time.Millisecond,
		MaxParallel: 10,
	})
	putToken, err := manager.GetSpawnToken(ctx)
	require.Nil(t, err)
	putToken()

	customFields := log.CustomFieldsFromContext(ctx)
	require.NotNil(t, customFields)
	require.Contains(t, customFields.Fields(), "command.spawn_token_wait_ms")
}

// This test modifies a global config, hence should never run in parallel
func TestGetSpawnToken_CommandStats_timeout(t *testing.T) {
	t.Parallel()

	ctx := log.InitContextCustomFields(testhelper.Context(t))
	manager := NewSpawnTokenManager(SpawnConfig{
		Timeout: 1 * time.Millisecond,
	})
	_, err := manager.GetSpawnToken(ctx)

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
