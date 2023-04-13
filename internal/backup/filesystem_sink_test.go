//go:build !gitaly_test_sha256

package backup

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestFilesystemSink_GetReader(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		dir := testhelper.TempDir(t)
		const relativePath = "test.dat"
		require.NoError(t, os.WriteFile(filepath.Join(dir, relativePath), []byte("test"), perm.SharedFile))

		fsSink := NewFilesystemSink(dir)
		reader, err := fsSink.GetReader(ctx, relativePath)
		require.NoError(t, err)

		defer func() { require.NoError(t, reader.Close()) }()

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, []byte("test"), data)
	})

	t.Run("no file", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		dir, err := os.Getwd()
		require.NoError(t, err)

		fsSink := NewFilesystemSink(dir)
		reader, err := fsSink.GetReader(ctx, "not-existing")
		require.Equal(t, ErrDoesntExist, err)
		require.Nil(t, reader)
	})
}

func TestFilesystemSink_GetWriter(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		dir := testhelper.TempDir(t)
		const relativePath = "nested/dir/test.dat"

		fsSink := NewFilesystemSink(dir)
		w, err := fsSink.GetWriter(ctx, relativePath)
		require.NoError(t, err)

		_, err = io.Copy(w, strings.NewReader("test"))
		require.NoError(t, err)

		require.NoError(t, w.Close())

		require.FileExists(t, filepath.Join(dir, relativePath))
		data, err := os.ReadFile(filepath.Join(dir, relativePath))
		require.NoError(t, err)
		require.Equal(t, []byte("test"), data)
	})

	t.Run("overrides existing data", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		dir := testhelper.TempDir(t)
		const relativePath = "nested/dir/test.dat"
		fullPath := filepath.Join(dir, relativePath)

		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), perm.SharedDir))
		require.NoError(t, os.WriteFile(fullPath, []byte("initial"), perm.SharedFile))

		fsSink := NewFilesystemSink(dir)
		w, err := fsSink.GetWriter(ctx, relativePath)
		require.NoError(t, err)

		_, err = io.Copy(w, strings.NewReader("test"))
		require.NoError(t, err)

		require.NoError(t, w.Close())

		require.FileExists(t, fullPath)
		data, err := os.ReadFile(fullPath)
		require.NoError(t, err)
		require.Equal(t, []byte("test"), data)
	})

	t.Run("dir creation error", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		dir := testhelper.TempDir(t)
		const relativePath = "nested/test.dat"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "nested"), []byte("lock"), perm.PublicFile))

		fsSink := NewFilesystemSink(dir)
		_, err := fsSink.GetWriter(ctx, relativePath)
		require.EqualError(t, err, fmt.Sprintf(`filesystem sink: mkdir %s: not a directory`, filepath.Join(dir, "nested")))
	})
}
