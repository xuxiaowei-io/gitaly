package repocleaner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestLogWarnAction_Perform(t *testing.T) {
	ctx := testhelper.Context(t)

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)
	action := NewLogWarnAction(logger)
	err := action.Perform(ctx, "vs1", "g1", []string{"p/1", "p/2"})
	require.NoError(t, err)
	require.Len(t, hook.AllEntries(), 2)

	exp := []map[string]interface{}{{
		"Data": log.Fields{
			"component":             "repocleaner.log_warn_action",
			"virtual_storage":       "vs1",
			"storage":               "g1",
			"relative_replica_path": "p/1",
		},
		"Message": "repository is not managed by praefect",
	}, {
		"Data": log.Fields{
			"component":             "repocleaner.log_warn_action",
			"virtual_storage":       "vs1",
			"storage":               "g1",
			"relative_replica_path": "p/2",
		},
		"Message": "repository is not managed by praefect",
	}}

	require.ElementsMatch(t, exp, []map[string]interface{}{{
		"Data":    hook.AllEntries()[0].Data,
		"Message": hook.AllEntries()[0].Message,
	}, {
		"Data":    hook.AllEntries()[1].Data,
		"Message": hook.AllEntries()[1].Message,
	}})
}
