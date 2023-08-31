package git

import (
	"strconv"
	"strings"
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

func TestFormatTime(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc               string
		t                  time.Time
		expectedString     string
		expectedSignature  string
		expectedParsedTime time.Time
	}{
		{
			desc:               "zero value",
			t:                  time.Time{},
			expectedString:     "Mon Jan 01 0001 00:00:00 +0000",
			expectedSignature:  "-62135596800 +0000",
			expectedParsedTime: time.Date(1, time.January, 1, 0, 0, 0, 0, time.FixedZone("", 0)),
		},
		{
			desc:               "Unix birth time",
			t:                  time.Unix(0, 0).In(time.UTC),
			expectedString:     "Thu Jan 01 1970 00:00:00 +0000",
			expectedSignature:  "0 +0000",
			expectedParsedTime: time.Date(1970, time.January, 1, 0, 0, 0, 0, time.FixedZone("", 0)),
		},
		{
			desc:               "recent UTC date",
			t:                  time.Date(2023, time.August, 29, 9, 15, 46, 0, time.FixedZone("", 0)),
			expectedString:     "Tue Aug 29 2023 09:15:46 +0000",
			expectedSignature:  "1693300546 +0000",
			expectedParsedTime: time.Date(2023, time.August, 29, 9, 15, 46, 0, time.FixedZone("", 0)),
		},
		{
			desc:               "recent date in non-standard timezone",
			t:                  time.Date(2023, time.August, 29, 9, 15, 46, 0, time.FixedZone("CEST", 2*60*60)),
			expectedString:     "Tue Aug 29 2023 09:15:46 +0200",
			expectedSignature:  "1693293346 +0200",
			expectedParsedTime: time.Date(2023, time.August, 29, 9, 15, 46, 0, time.FixedZone("", 2*60*60)),
		},
		{
			desc:               "sub-second accuracy is ignored",
			t:                  time.Date(2023, time.August, 29, 9, 15, 46, 9000, time.FixedZone("CEST", 2*60*60)),
			expectedString:     "Tue Aug 29 2023 09:15:46 +0200",
			expectedSignature:  "1693293346 +0200",
			expectedParsedTime: time.Date(2023, time.August, 29, 9, 15, 46, 0, time.FixedZone("", 2*60*60)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("FormatTime", func(t *testing.T) {
				actualString := FormatTime(tc.t)
				require.Equal(t, tc.expectedString, actualString)

				// We use `time.ParseInLocation()` here such that Go won't automatically translate e.g. `+0200`
				// into "CEST" or `time.Local`.
				actualParsedTime, err := time.ParseInLocation(rfc2822DateFormat, actualString, time.FixedZone("", 0))

				require.NoError(t, err)
				require.Equal(t, tc.expectedParsedTime, actualParsedTime)
			})

			t.Run("FormatSignatureTime", func(t *testing.T) {
				actualSignature := FormatSignatureTime(tc.t)
				require.Equal(t, tc.expectedSignature, actualSignature)

				unixTimeStr, timezoneStr, ok := strings.Cut(actualSignature, " ")
				require.True(t, ok)

				unixTime, err := strconv.ParseInt(unixTimeStr, 10, 64)
				require.NoError(t, err)
				timezone, err := time.ParseInLocation("-0700", timezoneStr, time.FixedZone("", 0))
				require.NoError(t, err)

				require.Equal(t, tc.expectedParsedTime, time.Unix(unixTime, 0).In(timezone.Location()))
			})
		})
	}
}
