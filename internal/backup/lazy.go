package backup

import (
	"bytes"
	"context"
	"io"
)

// LazyWrite saves all the data from the r by relativePath and will only create
// a file if there is data to be written.
func LazyWrite(ctx context.Context, sink Sink, relativePath string, r io.Reader) error {
	var buf [256]byte
	n, err := r.Read(buf[:])
	if err == io.EOF {
		if n == 0 {
			return nil
		}
	} else if err != nil {
		return err
	}
	r = io.MultiReader(bytes.NewReader(buf[:n]), r)
	return sink.Write(ctx, relativePath, r)
}

// LazyWriter is a WriteCloser that will call Create when on the first call to
// Write. This means it will only create a file if there will be data written
// to it.
type LazyWriter struct {
	create func() (io.WriteCloser, error)
	w      io.WriteCloser
}

// NewLazyWriter initializes a new LazyWriter. create is called on the first
// call of Write, any errors will be returned by this call.
func NewLazyWriter(create func() (io.WriteCloser, error)) *LazyWriter {
	return &LazyWriter{
		create: create,
	}
}

func (w *LazyWriter) Write(p []byte) (int, error) {
	if w.w == nil {
		var err error
		w.w, err = w.create()
		if err != nil {
			return 0, err
		}
	}

	return w.w.Write(p)
}

// Close calls Close on the WriteCloser returned by Create, passing on any
// returned error. Close must be called to properly clean up resources.
func (w *LazyWriter) Close() error {
	if w.w == nil {
		return nil
	}
	return w.w.Close()
}
