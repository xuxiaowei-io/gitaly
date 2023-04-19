package testhelper

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
)

type tbRecorder struct {
	// Embed a nil TB as we'd rather panic if some calls that were
	// made were not captured by the recorder.
	testing.TB
	tb testing.TB

	errorMessage string
	helper       bool
	failNow      bool
}

func (r *tbRecorder) Name() string {
	return r.tb.Name()
}

func (r *tbRecorder) Errorf(format string, args ...any) {
	r.errorMessage = fmt.Sprintf(format, args...)
}

func (r *tbRecorder) Helper() {
	r.helper = true
}

func (r *tbRecorder) FailNow() {
	r.failNow = true
}

func TestRequireDirectoryState(t *testing.T) {
	umask := perm.GetUmask()

	t.Parallel()

	rootDir := t.TempDir()

	relativePath := "assertion-root"

	require.NoError(t,
		os.MkdirAll(
			filepath.Join(rootDir, relativePath, "dir-a"),
			fs.ModePerm,
		),
	)
	require.NoError(t,
		os.MkdirAll(
			filepath.Join(rootDir, relativePath, "dir-b"),
			perm.PrivateDir,
		),
	)
	require.NoError(t,
		os.WriteFile(
			filepath.Join(rootDir, relativePath, "dir-a", "unparsed-file"),
			[]byte("raw content"),
			fs.ModePerm,
		),
	)
	require.NoError(t,
		os.WriteFile(
			filepath.Join(rootDir, relativePath, "parsed-file"),
			[]byte("raw content"),
			perm.PrivateFile,
		),
	)

	for _, tc := range []struct {
		desc                 string
		modifyAssertion      func(DirectoryState)
		expectedErrorMessage string
	}{
		{
			desc:            "correct assertion",
			modifyAssertion: func(DirectoryState) {},
		},
		{
			desc: "unexpected directory",
			modifyAssertion: func(state DirectoryState) {
				delete(state, "/assertion-root")
			},
			expectedErrorMessage: `+ (string) (len=15) "/assertion-root": (testhelper.DirectoryEntry)`,
		},
		{
			desc: "unexpected file",
			modifyAssertion: func(state DirectoryState) {
				delete(state, "/assertion-root/dir-a/unparsed-file")
			},
			expectedErrorMessage: `+ (string) (len=35) "/assertion-root/dir-a/unparsed-file": (testhelper.DirectoryEntry)`,
		},
		{
			desc: "wrong mode",
			modifyAssertion: func(state DirectoryState) {
				modified := state["/assertion-root/dir-b"]
				modified.Mode = fs.ModePerm
				state["/assertion-root/dir-b"] = modified
			},
			expectedErrorMessage: `-  Mode: (fs.FileMode)`,
		},
		{
			desc: "wrong unparsed content",
			modifyAssertion: func(state DirectoryState) {
				modified := state["/assertion-root/dir-a/unparsed-file"]
				modified.Content = "incorrect content"
				state["/assertion-root/dir-a/unparsed-file"] = modified
			},
			expectedErrorMessage: `-  Content: (string) (len=17) "incorrect content",
	            	+  Content: ([]uint8) (len=11) {
	            	+   00000000  72 61 77 20 63 6f 6e 74  65 6e 74                 |raw content|
	            	+  }`,
		},
		{
			desc: "wrong parsed content",
			modifyAssertion: func(state DirectoryState) {
				modified := state["/assertion-root/parsed-file"]
				modified.Content = "incorrect content"
				state["/assertion-root/parsed-file"] = modified
			},
			expectedErrorMessage: `-  Content: (string) (len=17) "incorrect content",
	            	+  Content: (string) (len=14) "parsed content"`,
		},
		{
			desc: "missing entry",
			modifyAssertion: func(state DirectoryState) {
				state["/does/not/exist/on/disk"] = DirectoryEntry{}
			},
			expectedErrorMessage: `- (string) (len=23) "/does/not/exist/on/disk": (testhelper.DirectoryEntry)`,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			expectedState := DirectoryState{
				"/assertion-root": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/assertion-root/parsed-file": {
					Mode:    umask.Mask(perm.PrivateFile),
					Content: "parsed content",
					ParseContent: func(tb testing.TB, content []byte) any {
						return "parsed content"
					},
				},
				"/assertion-root/dir-a":               {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/assertion-root/dir-a/unparsed-file": {Mode: umask.Mask(fs.ModePerm), Content: []byte("raw content")},
				"/assertion-root/dir-b":               {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
			}

			tc.modifyAssertion(expectedState)

			recordedTB := &tbRecorder{tb: t}
			RequireDirectoryState(recordedTB, rootDir, relativePath, expectedState)
			if tc.expectedErrorMessage != "" {
				require.Contains(t, recordedTB.errorMessage, tc.expectedErrorMessage)
				require.True(t, recordedTB.failNow)
			} else {
				require.Empty(t, recordedTB.errorMessage)
				require.False(t, recordedTB.failNow)
			}
			require.True(t, recordedTB.helper)
			require.NotNil(t,
				expectedState["/assertion-root/parsed-file"].ParseContent,
				"ParseContent should still be set on the original expected state",
			)
		})
	}
}
