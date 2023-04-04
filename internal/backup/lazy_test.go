//go:build !gitaly_test_sha256

package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestLazyWrite_noData(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	var called bool
	sink := MockSink{
		WriteFn: func(ctx context.Context, relativePath string, r io.Reader) error {
			called = true
			return nil
		},
	}

	err := LazyWrite(ctx, sink, "a-file", strings.NewReader(""))
	require.NoError(t, err)
	require.False(t, called)
}

func TestLazyWrite_data(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	expectedData := make([]byte, 512)
	_, err := rand.Read(expectedData)
	require.NoError(t, err)

	var data bytes.Buffer

	sink := MockSink{
		WriteFn: func(ctx context.Context, relativePath string, r io.Reader) error {
			_, err := io.Copy(&data, r)
			return err
		},
	}

	require.NoError(t, LazyWrite(ctx, sink, "a-file", bytes.NewReader(expectedData)))
	require.Equal(t, expectedData, data.Bytes())
}

func TestLazyWriter(t *testing.T) {
	t.Parallel()

	expectedData := make([]byte, 512)
	_, err := rand.Read(expectedData)
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		tempDir := testhelper.TempDir(t)
		tempFilePath := filepath.Join(tempDir, "file")

		w := NewLazyWriter(func() (io.WriteCloser, error) {
			return os.Create(tempFilePath)
		})

		_, err = io.Copy(w, bytes.NewReader([]byte("")))
		require.NoError(t, err)
		require.NoFileExists(t, tempFilePath)

		_, err = io.Copy(w, iotest.OneByteReader(bytes.NewReader(expectedData)))
		require.NoError(t, err)
		require.FileExists(t, tempFilePath)

		data, err := os.ReadFile(tempFilePath)
		require.NoError(t, err)
		require.Equal(t, expectedData, data)
	})

	t.Run("create error", func(t *testing.T) {
		w := NewLazyWriter(func() (io.WriteCloser, error) {
			return nil, assert.AnError
		})

		n, err := w.Write(make([]byte, 100))
		require.Equal(t, 0, n)
		require.Equal(t, assert.AnError, err)
	})
}

type MockSink struct {
	GetReaderFn func(ctx context.Context, relativePath string) (io.ReadCloser, error)
	GetWriterFn func(ctx context.Context, relativePath string) (io.WriteCloser, error)
	WriteFn     func(ctx context.Context, relativePath string, r io.Reader) error
}

func (s MockSink) GetWriter(ctx context.Context, relativePath string) (io.WriteCloser, error) {
	if s.GetWriterFn != nil {
		return s.GetWriterFn(ctx, relativePath)
	}
	return nopWriteCloser{}, nil
}

func (s MockSink) Write(ctx context.Context, relativePath string, r io.Reader) error {
	if s.WriteFn != nil {
		return s.WriteFn(ctx, relativePath, r)
	}
	return nil
}

func (s MockSink) GetReader(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	if s.GetReaderFn != nil {
		return s.GetReaderFn(ctx, relativePath)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (nopWriteCloser) Close() error {
	return nil
}
