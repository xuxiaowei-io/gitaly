package helper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGrpcCode(t *testing.T) {
	t.Parallel()
	for desc, tc := range map[string]struct {
		in  error
		exp codes.Code
	}{
		"unwrapped status": {
			in:  status.Error(codes.NotFound, ""),
			exp: codes.NotFound,
		},
		"wrapped status": {
			in:  fmt.Errorf("context: %w", status.Error(codes.NotFound, "")),
			exp: codes.NotFound,
		},
		"unwrapped status created by helpers": {
			in:  structerr.NewInternal(""),
			exp: codes.Internal,
		},
		"wrapped status created by helpers": {
			in:  fmt.Errorf("context: %w", structerr.NewInternal("")),
			exp: codes.Internal,
		},
		"double wrapped status created by helpers": {
			in:  fmt.Errorf("outer: %w", fmt.Errorf("context: %w", structerr.NewInternal(""))),
			exp: codes.Internal,
		},
		"nil input": {
			in:  nil,
			exp: codes.OK,
		},
		"no code defined": {
			in:  assert.AnError,
			exp: codes.Unknown,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			assert.Equal(t, tc.exp, GrpcCode(tc.in))
		})
	}
}
