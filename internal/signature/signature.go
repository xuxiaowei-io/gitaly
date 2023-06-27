package signature

import (
	"bytes"
	"fmt"
	"os"
)

// SigningKey is the common interface interface of SSH and GPG signing keys
type SigningKey interface {
	CreateSignature([]byte) ([]byte, error)
}

// ParseSigningKey parses a signing key and returns either GPG or SSH key
func ParseSigningKey(path string) (SigningKey, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	if bytes.HasPrefix(key, []byte("-----BEGIN OPENSSH")) {
		return parseSSHSigningKey(key)
	}

	return parseGpgSigningKey(key)
}
