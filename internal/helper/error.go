package helper

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GrpcCode translates errors into codes.Code values.
// It unwraps the nested errors until it finds the most nested one that returns the codes.Code.
// If err is nil it returns codes.OK.
// If no codes.Code found it returns codes.Unknown.
func GrpcCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	code := codes.Unknown
	for ; err != nil; err = errors.Unwrap(err) {
		st, ok := status.FromError(err)
		if ok {
			code = st.Code()
		}
	}

	return code
}
