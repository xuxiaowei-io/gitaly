package repoutil

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestGetCustomHooks_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	expectedTarResponse := []string{
		"custom_hooks/",
		"custom_hooks/pre-commit.sample",
		"custom_hooks/prepare-commit-msg.sample",
		"custom_hooks/pre-push.sample",
	}
	require.NoError(t, os.Mkdir(filepath.Join(repoPath, "custom_hooks"), perm.PrivateDir), "Could not create custom_hooks dir")
	for _, fileName := range expectedTarResponse[1:] {
		require.NoError(t, os.WriteFile(filepath.Join(repoPath, fileName), []byte("Some hooks"), perm.PrivateExecutable), fmt.Sprintf("Could not create %s", fileName))
	}

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, locator, &hooks, repo))

	reader := tar.NewReader(&hooks)
	fileLength := 0
	for {
		file, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		fileLength++
		require.Contains(t, expectedTarResponse, file.Name)
	}
	require.Equal(t, fileLength, len(expectedTarResponse))
}

func TestGetCustomHooks_symlink(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	linkTarget := "/var/empty"
	require.NoError(t, os.Symlink(linkTarget, filepath.Join(repoPath, "custom_hooks")), "Could not create custom_hooks symlink")

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, locator, &hooks, repo))

	reader := tar.NewReader(&hooks)
	file, err := reader.Next()
	require.NoError(t, err)

	require.Equal(t, "custom_hooks", file.Name, "tar entry name")
	require.Equal(t, byte(tar.TypeSymlink), file.Typeflag, "tar entry type")
	require.Equal(t, linkTarget, file.Linkname, "link target")

	_, err = reader.Next()
	require.Equal(t, io.EOF, err, "custom_hooks should have been the only entry")
}

func TestGetCustomHooks_nonexistentHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, locator, &hooks, repo))

	reader := tar.NewReader(&hooks)
	buf := bytes.NewBuffer(nil)
	_, err := io.Copy(buf, reader)
	require.NoError(t, err)

	require.Empty(t, buf.String(), "Returned stream should be empty")
}

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

	validArchive := func() io.Reader {
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
	}

	for _, tc := range []struct {
		desc                 string
		archive              io.Reader
		stripPrefix          bool
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
			desc:    "custom_hooks dir extracted",
			archive: validArchive(),
			expectedState: testhelper.DirectoryState{
				"/":                          {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks":              {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/custom_hooks/pre-receive":  {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-receive content")},
				"/custom_hooks/subdirectory": {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
				"/custom_hooks/subdirectory/supporting-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("supporting-file content")},
			},
		},
		{
			desc:        "custom_hooks dir extracted with prefix stripped",
			archive:     validArchive(),
			stripPrefix: true,
			expectedState: testhelper.DirectoryState{
				"/":                             {Mode: umask.Mask(fs.ModeDir | fs.ModePerm)},
				"/pre-receive":                  {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-receive content")},
				"/subdirectory":                 {Mode: umask.Mask(fs.ModeDir | perm.PrivateDir)},
				"/subdirectory/supporting-file": {Mode: umask.Mask(perm.PrivateFile), Content: []byte("supporting-file content")},
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
			err := ExtractHooks(ctx, tc.archive, tmpDir, tc.stripPrefix)
			if tc.expectedErrorMessage != "" {
				require.ErrorContains(t, err, tc.expectedErrorMessage)
			} else {
				require.NoError(t, err)
			}
			testhelper.RequireDirectoryState(t, tmpDir, "", tc.expectedState)
		})
	}
}
