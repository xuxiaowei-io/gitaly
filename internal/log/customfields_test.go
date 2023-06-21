package log_test

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestStatsFromContext_BackgroundContext(t *testing.T) {
	ctx := testhelper.Context(t)

	customFields := log.CustomFieldsFromContext(ctx)
	require.Nil(t, customFields)
}

func TestStatsFromContext_InitContext(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), logrus.Fields{})
}

func TestStatsFromContext_RecordSum(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	customFields.RecordSum("foo", 1)
	customFields.RecordSum("foo", 1)

	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), logrus.Fields{"foo": 2})
}

func TestStatsFromContext_RecordSumByRef(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	customFields.RecordSum("foo", 1)
	customFields.RecordSum("foo", 1)

	stats2 := log.CustomFieldsFromContext(ctx)

	require.NotNil(t, stats2)
	require.Equal(t, stats2.Fields(), logrus.Fields{"foo": 2})
}

func TestStatsFromContext_RecordMax(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	customFields.RecordMax("foo", 1024)
	customFields.RecordMax("foo", 256)
	customFields.RecordMax("foo", 512)

	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), logrus.Fields{"foo": 1024})
}

func TestStatsFromContext_RecordMetadata(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = log.InitContextCustomFields(ctx)

	customFields := log.CustomFieldsFromContext(ctx)

	customFields.RecordMetadata("foo", "bar")
	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), logrus.Fields{"foo": "bar"})

	customFields.RecordMetadata("foo", "baz") // override the existing value
	require.NotNil(t, customFields)
	require.Equal(t, customFields.Fields(), logrus.Fields{"foo": "baz"})
}
