package catfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestRequestQueue_ReadObject(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	oid := git.ObjectID(strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen()))

	t.Run("ReadInfo on ReadObject queue", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, "#!/bin/sh\nread\n")

		require.PanicsWithValue(t, "object queue used to read object info", func() {
			_, _ = queue.ReadInfo(ctx)
		})
	})

	t.Run("read without request", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, "#!/bin/sh\nread\n")
		_, err := queue.ReadObject(ctx)
		require.Equal(t, fmt.Errorf("no outstanding request"), err)
	})

	t.Run("read on closed reader", func(t *testing.T) {
		reader, queue := newInterceptedObjectQueue(t, ctx, "#!/bin/sh\nread\n")

		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.True(t, queue.isDirty())

		reader.close()
		require.True(t, queue.isDirty())

		_, err := queue.ReadObject(ctx)
		require.Equal(t, fmt.Errorf("cannot read object info: %w", fmt.Errorf("file already closed")), err)
	})

	t.Run("read with unconsumed object", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			echo "%s commit 464"
		`, oid))

		// We queue two revisions...
		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.NoError(t, queue.RequestObject(ctx, "foo"))

		// .. and only unqueue one object. This object isn't read though, ...
		_, err := queue.ReadObject(ctx)
		require.NoError(t, err)

		// ... which means that trying to read the second object should fail now.
		_, err = queue.ReadObject(ctx)
		require.Equal(t, fmt.Errorf("current object has not been fully read"), err)

		require.True(t, queue.isDirty())
	})

	t.Run("read with invalid object header", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, `#!/bin/sh
			echo "something something"
		`)

		require.NoError(t, queue.RequestObject(ctx, "foo"))

		_, err := queue.ReadObject(ctx)
		require.Equal(t, fmt.Errorf("invalid info line: %q", "something something"), err)

		// The queue must be dirty when we failed due to an unexpected error.
		require.True(t, queue.isDirty())
	})

	t.Run("read with unexpected exit", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, `#!/bin/sh
			exit 1
		`)

		require.NoError(t, queue.RequestObject(ctx, "foo"))

		_, err := queue.ReadObject(ctx)
		require.Equal(t, fmt.Errorf("read info line: %w", io.EOF), err)

		// The queue must be dirty when we failed due to an unexpected error.
		require.True(t, queue.isDirty())
	})

	t.Run("read with missing object", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			echo "%s missing"
		`, oid))

		require.NoError(t, queue.RequestObject(ctx, "foo"))

		_, err := queue.ReadObject(ctx)
		require.Equal(t, NotFoundError{error: fmt.Errorf("object not found")}, err)

		// The queue must be empty even if the object wasn't found: this is a graceful
		// failure that we should handle alright.
		require.False(t, queue.isDirty())
	})

	t.Run("read single object", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			echo "%s blob 10"
			echo "1234567890"
		`, oid))

		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.True(t, queue.isDirty())

		object, err := queue.ReadObject(ctx)
		require.NoError(t, err)
		require.Equal(t, ObjectInfo{
			Oid:  oid,
			Type: "blob",
			Size: 10,
		}, object.ObjectInfo)

		data, err := io.ReadAll(object)
		require.NoError(t, err)
		require.Equal(t, "1234567890", string(data))

		require.False(t, queue.isDirty())
	})

	t.Run("read multiple objects", func(t *testing.T) {
		secondOID := git.ObjectID(strings.Repeat("2", gittest.DefaultObjectHash.EncodedLen()))

		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			echo "%s blob 10"
			echo "1234567890"
			echo "%s commit 10"
			echo "0987654321"
		`, oid, secondOID))

		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.True(t, queue.isDirty())

		for _, expectedObject := range []struct {
			info ObjectInfo
			data string
		}{
			{
				info: ObjectInfo{
					Oid:  oid,
					Type: "blob",
					Size: 10,
				},
				data: "1234567890",
			},
			{
				info: ObjectInfo{
					Oid:  secondOID,
					Type: "commit",
					Size: 10,
				},
				data: "0987654321",
			},
		} {
			require.True(t, queue.isDirty())

			object, err := queue.ReadObject(ctx)
			require.NoError(t, err)
			require.Equal(t, expectedObject.info, object.ObjectInfo)

			data, err := io.ReadAll(object)
			require.NoError(t, err)
			require.Equal(t, expectedObject.data, string(data))
		}

		require.False(t, queue.isDirty())
	})

	t.Run("truncated object", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			echo "%s blob 10"
			printf "123"
		`, oid))

		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.True(t, queue.isDirty())

		object, err := queue.ReadObject(ctx)
		require.NoError(t, err)
		require.Equal(t, ObjectInfo{
			Oid:  oid,
			Type: "blob",
			Size: 10,
		}, object.ObjectInfo)

		// The first read will succeed and return the prefix.
		var buf [10]byte
		n, err := object.Read(buf[:])
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "123", string(buf[:n]))

		// But the second read must fail and give us a hint that the read had been
		// truncated. Note that we explicitly expect to not see an io.EOF here,
		// which might indicate success to the caller.
		_, err = object.Read(buf[:])
		require.Equal(t, fmt.Errorf("discard newline: \"EOF\""), err)

		require.True(t, queue.isDirty())
	})
}

func TestRequestQueue_RequestObject(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	oid := git.ObjectID(strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen()))

	requireRevision := func(t *testing.T, queue *requestQueue, rev git.Revision) {
		object, err := queue.ReadObject(ctx)
		require.NoError(t, err)

		data, err := io.ReadAll(object)
		require.NoError(t, err)
		require.Equal(t, rev, git.Revision(data))
	}

	t.Run("requesting revision on closed queue", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, "#!/bin/sh")
		queue.close()

		require.Equal(t, fmt.Errorf("cannot request revision: %w", os.ErrClosed), queue.RequestObject(ctx, "foo"))
	})

	t.Run("requesting revision on closed process", func(t *testing.T) {
		process, queue := newInterceptedObjectQueue(t, ctx, "#!/bin/sh")

		process.close()

		require.Equal(t, fmt.Errorf("cannot request revision: %w", os.ErrClosed), queue.RequestObject(ctx, "foo"))
	})

	t.Run("single request", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			read revision
			echo "%s blob ${#revision}"
			echo "${revision}"
		`, oid))

		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.NoError(t, queue.Flush(ctx))

		requireRevision(t, queue, "foo")
	})

	t.Run("multiple request", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			while read revision
			do
				echo "%s blob ${#revision}"
				echo "${revision}"
			done
		`, oid))

		require.NoError(t, queue.RequestObject(ctx, "foo"))
		require.NoError(t, queue.RequestObject(ctx, "bar"))
		require.NoError(t, queue.RequestObject(ctx, "baz"))
		require.NoError(t, queue.RequestObject(ctx, "qux"))
		require.NoError(t, queue.Flush(ctx))

		requireRevision(t, queue, "foo")
		requireRevision(t, queue, "bar")
		requireRevision(t, queue, "baz")
		requireRevision(t, queue, "qux")
	})

	t.Run("multiple request with intermediate flushing", func(t *testing.T) {
		_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			while read revision
			do
				read flush
				if test "$flush" != "FLUSH"
				then
					echo "expected a flush"
					exit 1
				fi

				echo "%s blob ${#revision}"
				echo "${revision}"
			done
		`, oid))

		for _, revision := range []git.Revision{
			"foo",
			"bar",
			"foo",
			"qux",
		} {
			require.NoError(t, queue.RequestObject(ctx, revision))
			require.NoError(t, queue.Flush(ctx))
			requireRevision(t, queue, revision)
		}
	})
}

func TestRequestQueue_RequestInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	oid := git.ObjectID(strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen()))
	expectedInfo := &ObjectInfo{oid, "blob", 955}

	requireRevision := func(t *testing.T, queue *requestQueue) {
		info, err := queue.ReadInfo(ctx)
		require.NoError(t, err)

		require.NoError(t, err)
		require.Equal(t, info, expectedInfo)
	}

	t.Run("requesting revision on closed queue", func(t *testing.T) {
		_, queue := newInterceptedInfoQueue(t, ctx, "#!/bin/sh")
		queue.close()

		require.Equal(t, fmt.Errorf("cannot request revision: %w", os.ErrClosed), queue.RequestInfo(ctx, "foo"))
	})

	t.Run("requesting revision on closed process", func(t *testing.T) {
		process, queue := newInterceptedInfoQueue(t, ctx, "#!/bin/sh")

		process.close()

		require.Equal(t, fmt.Errorf("cannot request revision: %w", os.ErrClosed), queue.RequestInfo(ctx, "foo"))
	})

	t.Run("single request", func(t *testing.T) {
		_, queue := newInterceptedInfoQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			read revision
			echo "%s blob 955"
		`, oid))

		require.NoError(t, queue.RequestInfo(ctx, "foo"))
		require.NoError(t, queue.Flush(ctx))

		requireRevision(t, queue)
	})

	t.Run("multiple request", func(t *testing.T) {
		_, queue := newInterceptedInfoQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			while read revision
			do
				echo "%s blob 955"
			done
		`, oid))

		require.NoError(t, queue.RequestInfo(ctx, "foo"))
		require.NoError(t, queue.RequestInfo(ctx, "bar"))
		require.NoError(t, queue.RequestInfo(ctx, "baz"))
		require.NoError(t, queue.RequestInfo(ctx, "qux"))
		require.NoError(t, queue.Flush(ctx))

		requireRevision(t, queue)
		requireRevision(t, queue)
		requireRevision(t, queue)
		requireRevision(t, queue)
	})

	t.Run("multiple request with intermediate flushing", func(t *testing.T) {
		_, queue := newInterceptedInfoQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			while read revision
			do
				read flush
				if test "$flush" != "FLUSH"
				then
					echo "expected a flush"
					exit 1
				fi

				echo "%s blob 955"
			done
		`, oid))

		for _, revision := range []git.Revision{
			"foo",
			"bar",
			"foo",
			"qux",
		} {
			require.NoError(t, queue.RequestInfo(ctx, revision))
			require.NoError(t, queue.Flush(ctx))
			requireRevision(t, queue)
		}
	})
}

func TestRequestQueue_CommandStats(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx = command.InitContextStats(ctx)

	oid := git.ObjectID(strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen()))

	_, queue := newInterceptedObjectQueue(t, ctx, fmt.Sprintf(`#!/bin/sh
			read revision
			echo "%s blob ${#revision}"
			echo "${revision}"
		`, oid))

	require.NoError(t, queue.RequestObject(ctx, "foo"))
	require.NoError(t, queue.Flush(ctx))
	_, err := queue.ReadObject(ctx)
	require.NoError(t, err)

	stats := command.StatsFromContext(ctx)
	fields := stats.Fields()
	require.Contains(t, fields, "catfile.request_object_count")
	require.Contains(t, fields, "catfile.request_object_ms")
	require.Contains(t, fields, "catfile.flush_count")
	require.Contains(t, fields, "catfile.flush_ms")
	require.Contains(t, fields, "catfile.read_object_count")
	require.Contains(t, fields, "catfile.read_object_ms")
	require.Contains(t, fields, "catfile.duration_ms")
}

func TestRequestQueueCounters64BitAlignment(t *testing.T) {
	require.Equal(t, 0, int(unsafe.Sizeof(requestQueue{}.counters))%8)
}

func newInterceptedObjectQueue(t *testing.T, ctx context.Context, script string) (ObjectContentReader, *requestQueue) {
	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	commandFactory := gittest.NewInterceptingCommandFactory(t, ctx, cfg, func(execEnv git.ExecutionEnvironment) string {
		return script
	})
	repoExecutor := repoExecutor{
		GitRepo:       repo,
		gitCmdFactory: commandFactory,
	}

	reader, err := newObjectContentReader(ctx, &repoExecutor, nil)
	require.NoError(t, err)
	t.Cleanup(reader.close)

	queue, cleanup, err := reader.objectQueue(ctx, "trace")
	require.NoError(t, err)
	t.Cleanup(cleanup)

	return reader, queue
}

func newInterceptedInfoQueue(t *testing.T, ctx context.Context, script string) (ObjectInfoReader, *requestQueue) {
	cfg := testcfg.Build(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	commandFactory := gittest.NewInterceptingCommandFactory(t, ctx, cfg, func(execEnv git.ExecutionEnvironment) string {
		return script
	})
	repoExecutor := repoExecutor{
		GitRepo:       repo,
		gitCmdFactory: commandFactory,
	}

	reader, err := newObjectInfoReader(ctx, &repoExecutor, nil)
	require.NoError(t, err)
	t.Cleanup(reader.close)

	queue, cleanup, err := reader.infoQueue(ctx, "trace")
	require.NoError(t, err)
	t.Cleanup(cleanup)

	return reader, queue
}
