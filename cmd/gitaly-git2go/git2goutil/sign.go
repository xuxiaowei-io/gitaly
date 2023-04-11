package git2goutil

import (
	"bytes"
	"fmt"
	"os"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
)

// CreateCommitSignature reads the given signing key and produces PKCS#7 detached signature.
// When the path to the signing key is not present, an empty signature is returned.
func CreateCommitSignature(signingKeyPath string, contentToSign []byte) ([]byte, error) {
	if signingKeyPath == "" {
		return nil, nil
	}

	key, err := os.ReadFile(signingKeyPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	return createGPGSignature(key, contentToSign)
}

func createGPGSignature(key []byte, contentToSign []byte) ([]byte, error) {
	entity, err := openpgp.ReadEntity(packet.NewReader(bytes.NewReader(key)))
	if err != nil {
		return nil, fmt.Errorf("read entity: %w", err)
	}

	sigBuf := new(bytes.Buffer)
	if err := openpgp.ArmoredDetachSignText(
		sigBuf,
		entity,
		bytes.NewReader(contentToSign),
		&packet.Config{},
	); err != nil {
		return nil, fmt.Errorf("sign commit: %w", err)
	}

	return sigBuf.Bytes(), nil
}
