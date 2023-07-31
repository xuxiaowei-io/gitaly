package git

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewSignature(t *testing.T) {
	expectedSignature := Signature{
		Name:  "foo",
		Email: "foo@example.com",
		When:  time.Unix(1234567890, 0).In(time.UTC),
	}

	for _, tt := range []struct {
		name      string
		userName  string
		userEmail string
		when      time.Time
		expected  Signature
	}{
		{
			name:      "valid params",
			userName:  "foo",
			userEmail: "foo@example.com",
			when:      time.Unix(1234567890, 0).In(time.UTC),
			expected:  expectedSignature,
		},
		{
			name:      "special characters in username are replaced",
			userName:  "<foo>\n",
			userEmail: "foo@example.com",
			when:      time.Unix(1234567890, 0).In(time.UTC),
			expected:  expectedSignature,
		},
		{
			name:      "special characters in email are replaced",
			userName:  "foo",
			userEmail: "<foo@example.com>\n",
			when:      time.Unix(1234567890, 0).In(time.UTC),
			expected:  expectedSignature,
		},
		{
			name:      "time is truncated to seconds",
			userName:  "foo",
			userEmail: "foo@example.com",
			when:      time.Unix(1234567890, 123).In(time.UTC),
			expected:  expectedSignature,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, NewSignature(tt.userName, tt.userEmail, tt.when))
		})
	}
}
