package git

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRevision(t *testing.T) {
	for _, tc := range []struct {
		desc        string
		revision    string
		opts        []ValidateRevisionOption
		expectedErr error
	}{
		{
			desc:        "empty revision",
			revision:    "",
			expectedErr: fmt.Errorf("empty revision"),
		},
		{
			desc:     "empty revision with allowed empty revisions",
			revision: "",
			opts: []ValidateRevisionOption{
				AllowEmptyRevision(),
			},
		},
		{
			desc:     "valid revision",
			revision: "foo/bar",
		},
		{
			desc:        "leading dash",
			revision:    "-foo/bar",
			expectedErr: fmt.Errorf("revision can't start with '-'"),
		},
		{
			desc:     "intermediate dash",
			revision: "foo-bar",
		},
		{
			desc:        "space",
			revision:    "foo bar",
			expectedErr: fmt.Errorf("revision can't contain whitespace"),
		},
		{
			desc:        "newline",
			revision:    "foo\nbar",
			expectedErr: fmt.Errorf("revision can't contain whitespace"),
		},
		{
			desc:        "tab",
			revision:    "foo\tbar",
			expectedErr: fmt.Errorf("revision can't contain whitespace"),
		},
		{
			desc:        "carriage-return",
			revision:    "foo\rbar",
			expectedErr: fmt.Errorf("revision can't contain whitespace"),
		},
		{
			desc:        "NUL-byte",
			revision:    "foo\x00bar",
			expectedErr: fmt.Errorf("revision can't contain NUL"),
		},
		{
			desc:        "colon",
			revision:    "foo/bar:baz",
			expectedErr: fmt.Errorf("revision can't contain ':'"),
		},
		{
			desc:        "backslash",
			revision:    "foo\\bar\\baz",
			expectedErr: fmt.Errorf("revision can't contain '\\'"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expectedErr, ValidateRevision([]byte(tc.revision), tc.opts...))
		})
	}
}
