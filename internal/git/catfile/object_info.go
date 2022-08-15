package catfile

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

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
		if strings.HasPrefix(infoLine, flushCommand) {
			goto restart
		}

		return nil, NotFoundError{fmt.Errorf("object not found")}
	}

	info := strings.Split(infoLine, " ")
	if len(info) != 3 {
		return nil, fmt.Errorf("invalid info line: %q", infoLine)
	}

	oid, err := objectHash.FromHex(info[0])
	if err != nil {
		return nil, fmt.Errorf("parse object ID: %w", err)
	}

	objectSize, err := strconv.ParseInt(info[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse object size: %w", err)
	}

	return &ObjectInfo{
		Oid:  oid,
		Type: info[1],
		Size: objectSize,
	}, nil
}
