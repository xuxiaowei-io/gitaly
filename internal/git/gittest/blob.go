package gittest

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
)

// WriteBlobs writes n distinct blobs into the git repository's object
// database. Each object has the current time in nanoseconds as contents.
func WriteBlobs(tb testing.TB, cfg config.Cfg, testRepoPath string, n int) []string {
	var blobIDs []string
	for i := 0; i < n; i++ {
		contents := []byte(strconv.Itoa(time.Now().Nanosecond()))
		blobIDs = append(blobIDs, WriteBlob(tb, cfg, testRepoPath, contents).String())
	}

	return blobIDs
}

// WriteBlob writes the given contents as a blob into the repository and returns its OID.
func WriteBlob(tb testing.TB, cfg config.Cfg, testRepoPath string, contents []byte) git.ObjectID {
	hex := text.ChompBytes(ExecOpts(tb, cfg, ExecConfig{Stdin: bytes.NewReader(contents)},
		"-C", testRepoPath, "hash-object", "-w", "--stdin",
	))
	oid, err := DefaultObjectHash.FromHex(hex)
	require.NoError(tb, err)
	return oid
}
