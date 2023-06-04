package encoding

import (
	"google.golang.org/grpc/encoding"
)

const Name = "raw"

func init() {
	// TODO: This should be called explicitly
	encoding.RegisterCodec(codec{})
}

// codec is a Codec implementation with raw encoding. Clients and Servers
// exchange raw bytes without serializing/deserializing. It means they can
// send/receive any message without being restricted to Protobuf. Both sides
// are responsible for performing serialization and deserialization in the
// handler.
// The usage of this encoder is restricted:
// - The call must be bi-directional streaming
// - Both sides must use SendMsg and RecvMsg. Code-generated Send and Recv are
// not usable.
type codec struct{}

func (codec) Marshal(v interface{}) ([]byte, error) {
	return v.([]byte), nil
}

func (codec) Unmarshal(data []byte, v interface{}) error {
	*v.(*[]byte) = data
	return nil
}

func (codec) Name() string {
	return Name
}
