package hook

import (
	"archive/tar"
	"bytes"
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestExtractHooks(t *testing.T) {
	umask := perm.GetUmask()

	writeFile := func(writer *tar.Writer, path string, mode fs.FileMode, content string) {
		require.NoError(t, writer.WriteHeader(&tar.Header{
			Name: path,
			Mode: int64(mode),
			Size: int64(len(content)),
		}))
		_, err := writer.Write([]byte(content))
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		desc                 string
		archive              io.Reader
		expectedState        testhelper.DirectoryState
		expectedErrorMessage string
	}{
		{
			desc:    "empty reader",
			archive: strings.NewReader(""),
			expectedState: testhelper.DirectoryState{
				"/": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
		{
			desc: "empty archive",
			archive: func() io.Reader {
				var buffer bytes.Buffer
				writer := tar.NewWriter(&buffer)
				defer testhelper.MustClose(t, writer)
				return &buffer
			}(),
			expectedState: testhelper.DirectoryState{
				"/": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
		{
			desc: "just custom_hooks directory",
			archive: func() io.Reader {
				var buffer bytes.Buffer
				writer := tar.NewWriter(&buffer)
				require.NoError(t, writer.WriteHeader(&tar.Header{
					Name: "custom_hooks/",
					Mode: int64(fs.ModePerm),
				}))
				defer testhelper.MustClose(t, writer)
				return &buffer
			}(),
			expectedState: testhelper.DirectoryState{
				"/":             {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
		{
			desc: "custom_hooks dir extracted",
			archive: func() io.Reader {
				var buffer bytes.Buffer
				writer := tar.NewWriter(&buffer)
				writeFile(writer, "custom_hooks/pre-receive", fs.ModePerm, "pre-receive content")
				require.NoError(t, writer.WriteHeader(&tar.Header{
					Name: "custom_hooks/subdirectory/",
					Mode: int64(perm.PrivateDir),
				}))
				writeFile(writer, "custom_hooks/subdirectory/supporting-file", perm.PrivateFile, "supporting-file content")
				writeFile(writer, "ignored_file", fs.ModePerm, "ignored content")
				writeFile(writer, "ignored_directory/ignored_file", fs.ModePerm, "ignored content")
				defer testhelper.MustClose(t, writer)
				return &buffer
			}(),
			expectedState: testhelper.DirectoryState{
				"/":                          {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks":              {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks/pre-receive":  {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-receive content")},
				"/custom_hooks/subdirectory": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
				"/custom_hooks/subdirectory/supporting-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("supporting-file content")},
			},
		},
		{
			desc:                 "corrupted archive",
			archive:              strings.NewReader("invalid tar content"),
			expectedErrorMessage: "waiting for tar command completion: exit status",
			expectedState: testhelper.DirectoryState{
				"/": {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)

			tmpDir := t.TempDir()
			err := ExtractHooks(ctx, tc.archive, tmpDir)
			if tc.expectedErrorMessage != "" {
				require.ErrorContains(t, err, tc.expectedErrorMessage)
			} else {
				require.NoError(t, err)
			}
			testhelper.RequireDirectoryState(t, tmpDir, "", tc.expectedState)
		})
	}
}
