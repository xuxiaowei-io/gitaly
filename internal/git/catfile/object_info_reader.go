package catfile

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

// ObjectInfo represents a header returned by `git cat-file --batch`
type ObjectInfo struct {
	Oid  git.ObjectID
	Type string
	Size int64
}

// IsBlob returns true if object type is "blob"
func (o *ObjectInfo) IsBlob() bool {
	return o.Type == "blob"
}

// ObjectID is the ID of the object.
func (o *ObjectInfo) ObjectID() git.ObjectID {
	return o.Oid
}

// ObjectType is the type of the object.
func (o *ObjectInfo) ObjectType() string {
	return o.Type
}

// ObjectSize is the size of the object.
func (o *ObjectInfo) ObjectSize() int64 {
	return o.Size
}

// NotFoundError is returned when requesting an object that does not exist.
type NotFoundError struct{ error }

// IsNotFound tests whether err has type NotFoundError.
func IsNotFound(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
}

// ParseObjectInfo reads from a reader and parses the data into an ObjectInfo struct with the given
// object hash.
func ParseObjectInfo(objectHash git.ObjectHash, stdout *bufio.Reader) (*ObjectInfo, error) {
	// We keep track of errors so that in case we encounter multi-line data, which
	// isn't a pathname with multi-line, we throw an error instead of returning an object
	var prevError error

restart:
	infoLine, err := stdout.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read info line: %w", err)
	}

	infoLine = strings.TrimSuffix(infoLine, "\n")
	if strings.HasSuffix(infoLine, " missing") {
		// We use a hack to flush stdout of git-cat-file(1), which is that we request an
		// object that cannot exist. This causes Git to write an error and immediately flush
		// stdout. The only downside is that we need to filter this error here, but that's
		// acceptable while git-cat-file(1) doesn't yet have any way to natively flush.
		if strings.HasPrefix(infoLine, flushCommandHack) {
			goto restart
		}

		return nil, NotFoundError{fmt.Errorf("object not found")}
	}

	if strings.HasSuffix(infoLine, "ambiguous") {
		return nil, fmt.Errorf("ambiguous object ID")
	}

	// The below three errors should only happen when the pathname is taking over multiple lines.
	// In such a scenario we want to keep reading till we hit the `missing` param, so we goto
	// the start.
	info := strings.Split(infoLine, " ")
	if len(info) != 3 {
		prevError = fmt.Errorf("invalid info line: %q", infoLine)
		goto restart
	}

	oid, err := objectHash.FromHex(info[0])
	if err != nil {
		prevError = fmt.Errorf("parse object ID: %w", err)
		goto restart
	}

	objectSize, err := strconv.ParseInt(info[2], 10, 64)
	if err != nil {
		prevError = fmt.Errorf("parse object size: %w", err)
		goto restart
	}

	if prevError != nil {
		return nil, prevError
	}

	return &ObjectInfo{
		Oid:  oid,
		Type: info[1],
		Size: objectSize,
	}, nil
}

// ObjectInfoReader returns information about an object referenced by a given revision.
type ObjectInfoReader interface {
	cacheable

	// Info requests information about the revision pointed to by the given revision.
	Info(context.Context, git.Revision) (*ObjectInfo, error)

	// ObjectQueue returns an ObjectQueue that can be used to batch multiple object info
	// requests. Using the queue is more efficient than using `Info()` when requesting a bunch
	// of objects. The returned function must be executed after use of the ObjectQueue has
	// finished.
	ObjectQueue(context.Context) (ObjectQueue, func(), error)
}

// objectInfoReader is a reader for Git object information. This reader is implemented via a
// long-lived  `git cat-file --batch-check` process such that we do not have to spawn a separate
// process per object info we're about to read.
type objectInfoReader struct {
	cmd        *command.Command
	objectHash git.ObjectHash

	counter *prometheus.CounterVec

	queue      requestQueue
	queueInUse int32
}

func newObjectInfoReader(
	ctx context.Context,
	repo git.RepositoryExecutor,
	counter *prometheus.CounterVec,
) (*objectInfoReader, error) {
	batchCmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--batch-check"},
				git.Flag{Name: "--buffer"},
			},
		},
		git.WithSetupStdin(),
	)
	if err != nil {
		return nil, err
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	objectInfoReader := &objectInfoReader{
		cmd:        batchCmd,
		objectHash: objectHash,
		counter:    counter,
		queue: requestQueue{
			objectHash: objectHash,
			stdout:     bufio.NewReader(batchCmd),
			stdin:      bufio.NewWriter(batchCmd),
		},
	}

	return objectInfoReader, nil
}

func (o *objectInfoReader) close() {
	o.queue.close()
	_ = o.cmd.Wait()
}

func (o *objectInfoReader) isClosed() bool {
	return o.queue.isClosed()
}

func (o *objectInfoReader) isDirty() bool {
	return o.queue.isDirty()
}

func (o *objectInfoReader) infoQueue(ctx context.Context, tracedMethod string) (*requestQueue, func(), error) {
	if !atomic.CompareAndSwapInt32(&o.queueInUse, 0, 1) {
		return nil, nil, fmt.Errorf("object queue already in use")
	}

	trace := startTrace(ctx, o.counter, tracedMethod)
	o.queue.trace = trace

	return &o.queue, func() {
		atomic.StoreInt32(&o.queueInUse, 0)
		trace.finish()
	}, nil
}

func (o *objectInfoReader) Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	queue, cleanup, err := o.infoQueue(ctx, "catfile.Info")
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if err := queue.RequestInfo(revision); err != nil {
		return nil, err
	}

	if err := queue.Flush(); err != nil {
		return nil, err
	}

	objectInfo, err := queue.ReadInfo()
	if err != nil {
		return nil, err
	}

	return objectInfo, nil
}

func (o *objectInfoReader) ObjectQueue(ctx context.Context) (ObjectQueue, func(), error) {
	queue, cleanup, err := o.infoQueue(ctx, "catfile.InfoQueue")
	if err != nil {
		return nil, nil, err
	}

	return queue, cleanup, nil
}
