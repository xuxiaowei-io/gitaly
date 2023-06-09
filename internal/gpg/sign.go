package gpg

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
)

// CreateSignature creates a gpg signature
func CreateSignature(key []byte, contentToSign []byte) ([]byte, error) {
	entity, err := openpgp.ReadEntity(packet.NewReader(bytes.NewReader(key)))
	if err != nil {
		return nil, fmt.Errorf("read entity: %w", err)
	}

	var sigBuf strings.Builder
	if err := openpgp.ArmoredDetachSignText(
		&sigBuf,
		entity,
		bytes.NewReader(contentToSign),
		&packet.Config{},
	); err != nil {
		return nil, fmt.Errorf("sign commit: %w", err)
	}

	return []byte(sigBuf.String()), nil
}
