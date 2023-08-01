package signature

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var commit = []byte(`
tree 86ec18bfe87ad42a782fdabd8310f9b7ac750f51
parent b83d6e391c22777fca1ed3012fce84f633d7fed0
parent 4a24d82dbca5c11c61556f3b35ca472b7463187e
author User <user@email> 1491906794 +0000
committer User <user@email> 1491906794 +0000

Update README.md to include
`)

func TestParseSigningKeys(t *testing.T) {
	primaryPath := "testdata/signing_key.ssh"
	secondaryPaths := []string{"testdata/signing_key.gpg"}

	expectedSSHSignature, err := os.ReadFile("testdata/signing_key.ssh.sig")
	require.NoError(t, err)

	expectedGPGSignature, err := os.ReadFile("testdata/signing_key.gpg.sig")
	require.NoError(t, err)

	signingKeys, err := ParseSigningKeys(primaryPath, secondaryPaths...)
	require.NoError(t, err)
	require.NotNil(t, signingKeys.primaryKey)
	require.Len(t, signingKeys.secondaryKeys, 1)

	signature, err := signingKeys.CreateSignature(commit)
	require.NoError(t, err)
	require.Equal(t, signature, expectedSSHSignature)

	require.NoError(t, signingKeys.Verify(expectedSSHSignature, commit))
	require.NoError(t, signingKeys.Verify(expectedGPGSignature, commit))
}
