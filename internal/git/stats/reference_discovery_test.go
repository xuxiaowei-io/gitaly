package stats

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

func TestSingleRefParses(t *testing.T) {
	buf := &bytes.Buffer{}
	git.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	git.WritePktlineFlush(t, buf)
	git.WritePktlineString(t, buf, oid1+" HEAD\x00capability")
	git.WritePktlineFlush(t, buf)

	d, err := ParseReferenceDiscovery(buf)
	require.NoError(t, err)
	require.Equal(t, []string{"capability"}, d.Caps)
	require.Equal(t, []Reference{{Oid: oid1, Name: "HEAD"}}, d.Refs)
}

func TestMultipleRefsAndCapsParse(t *testing.T) {
	buf := &bytes.Buffer{}
	git.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	git.WritePktlineFlush(t, buf)
	git.WritePktlineString(t, buf, oid1+" HEAD\x00first second")
	git.WritePktlineString(t, buf, oid2+" refs/heads/master")
	git.WritePktlineFlush(t, buf)

	d, err := ParseReferenceDiscovery(buf)
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, d.Caps)
	require.Equal(t, []Reference{{Oid: oid1, Name: "HEAD"}, {Oid: oid2, Name: "refs/heads/master"}}, d.Refs)
}

func TestInvalidHeaderFails(t *testing.T) {
	buf := &bytes.Buffer{}
	git.WritePktlineString(t, buf, "# service=invalid\n")
	git.WritePktlineFlush(t, buf)
	git.WritePktlineString(t, buf, oid1+" HEAD\x00caps")
	git.WritePktlineFlush(t, buf)

	_, err := ParseReferenceDiscovery(buf)
	require.Error(t, err)
}

func TestMissingRefsFail(t *testing.T) {
	buf := &bytes.Buffer{}
	git.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	git.WritePktlineFlush(t, buf)
	git.WritePktlineFlush(t, buf)

	_, err := ParseReferenceDiscovery(buf)
	require.Error(t, err)
}

func TestInvalidRefFail(t *testing.T) {
	buf := &bytes.Buffer{}
	git.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	git.WritePktlineFlush(t, buf)
	git.WritePktlineString(t, buf, oid1+" HEAD\x00caps")
	git.WritePktlineString(t, buf, oid2)
	git.WritePktlineFlush(t, buf)

	_, err := ParseReferenceDiscovery(buf)
	require.Error(t, err)
}

func TestMissingTrailingFlushFails(t *testing.T) {
	buf := &bytes.Buffer{}
	git.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	git.WritePktlineFlush(t, buf)
	git.WritePktlineString(t, buf, oid1+" HEAD\x00caps")

	d := ReferenceDiscovery{}
	require.Error(t, d.Parse(buf))
}
