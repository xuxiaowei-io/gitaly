//go:build !gitaly_test_sha256

package limithandler

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestLimitStats(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx = InitLimitStats(ctx)

	stats := limitStatsFromContext(ctx)
	stats.AddConcurrencyQueueMs(13)

	assert.Equal(t, FieldsProducer(ctx, nil), logrus.Fields{"limit.concurrency_queue_ms": int64(13)})
}
