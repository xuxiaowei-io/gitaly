package command

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type requestStatsKey struct{}

// Stats records statistics about a command that was spawned.
type Stats struct {
	resource map[string]int
	metadata map[string]string
	sync.Mutex
}

// RecordSum sums up all the values for a given key.
func (stats *Stats) RecordSum(key string, value int) {
	stats.Lock()
	defer stats.Unlock()

	if prevValue, ok := stats.resource[key]; ok {
		value += prevValue
	}

	stats.resource[key] = value
}

// RecordMax will store the max value for a given key.
func (stats *Stats) RecordMax(key string, value int) {
	stats.Lock()
	defer stats.Unlock()

	if prevValue, ok := stats.resource[key]; ok {
		if prevValue > value {
			return
		}
	}

	stats.resource[key] = value
}

// RecordMetadata records metadata for the given key.
func (stats *Stats) RecordMetadata(key string, value string) {
	stats.Lock()
	defer stats.Unlock()

	stats.metadata[key] = value
}

// Fields returns all the stats as logrus.Fields
func (stats *Stats) Fields() logrus.Fields {
	stats.Lock()
	defer stats.Unlock()

	f := logrus.Fields{}
	for k, v := range stats.resource {
		f[k] = v
	}
	for k, v := range stats.metadata {
		f[k] = v
	}
	return f
}

// StatsFromContext gets the `Stats` from the given context.
func StatsFromContext(ctx context.Context) *Stats {
	stats, _ := ctx.Value(requestStatsKey{}).(*Stats)
	return stats
}

// InitContextStats returns a new context with `Stats` added to the given context.
func InitContextStats(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestStatsKey{}, &Stats{
		resource: make(map[string]int),
		metadata: make(map[string]string),
	})
}
