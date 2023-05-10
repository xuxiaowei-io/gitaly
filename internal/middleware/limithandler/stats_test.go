package limithandler

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestLimitStats(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx = InitLimitStats(ctx)

	stats := limitStatsFromContext(ctx)
	stats.SetLimitingKey("test-limiter", "hello-world")
	stats.AddConcurrencyQueueMs(13)
	stats.SetConcurrencyQueueLength(99)
	stats.SetConcurrencyDroppedReason("max_time")

	assert.Equal(t, FieldsProducer(ctx, nil), logrus.Fields{
		"limit.limiting_type":            "test-limiter",
		"limit.limiting_key":             "hello-world",
		"limit.concurrency_queue_ms":     int64(13),
		"limit.concurrency_queue_length": 99,
		"limit.concurrency_dropped":      "max_time",
	})
}
