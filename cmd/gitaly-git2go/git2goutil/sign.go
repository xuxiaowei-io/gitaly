package git2goutil

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/signature"
)

// CreateCommitSignature reads the given signing key and produces PKCS#7 detached signature.
// When the path to the signing key is not present, an empty signature is returned.
func CreateCommitSignature(signingKeyPath string, contentToSign []byte) ([]byte, error) {
	if signingKeyPath == "" {
		return nil, nil
	}

	signingKey, err := signature.ParseSigningKey(signingKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signing key: %w", err)
	}

	return signingKey.CreateSignature(contentToSign)
}
