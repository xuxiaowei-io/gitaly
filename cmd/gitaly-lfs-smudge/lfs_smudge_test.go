package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitlab"
)

const (
	lfsOid     = "3ea5dd307f195f449f0e08234183b82e92c3d5f4cff11c2a6bb014f9e0de12aa"
	lfsPointer = `version https://git-lfs.github.com/spec/v1
oid sha256:3ea5dd307f195f449f0e08234183b82e92c3d5f4cff11c2a6bb014f9e0de12aa
size 177735
`
	lfsPointerWithCRLF = `version https://git-lfs.github.com/spec/v1
oid sha256:3ea5dd307f195f449f0e08234183b82e92c3d5f4cff11c2a6bb014f9e0de12aa` + "\r\nsize 177735"
	invalidLfsPointer = `version https://git-lfs.github.com/spec/v1
oid sha256:3ea5dd307f195f449f0e08234183b82e92c3d5f4cff11c2a6bb014f9e0de12aa&gl_repository=project-51
size 177735
`
	invalidLfsPointerWithNonHex = `version https://git-lfs.github.com/spec/v1
oid sha256:3ea5dd307f195f449f0e08234183b82e92c3d5f4cff11c2a6bb014f9e0de12z-
size 177735`
	glRepository = "project-1"
	secretToken  = "topsecret"
	testData     = "hello world"
	certPath     = "../../internal/gitlab/testdata/certs/server.crt"
	keyPath      = "../../internal/gitlab/testdata/certs/server.key"
)

var defaultOptions = gitlab.TestServerOptions{
	SecretToken:      secretToken,
	LfsBody:          testData,
	LfsOid:           lfsOid,
	GlRepository:     glRepository,
	ClientCACertPath: certPath,
	ServerCertPath:   certPath,
	ServerKeyPath:    keyPath,
}

type mapConfig struct {
	env map[string]string
}

func (m *mapConfig) Get(key string) string {
	return m.env[key]
}

func TestSuccessfulLfsSmudge(t *testing.T) {
	testCases := []struct {
		desc string
		data string
	}{
		{
			desc: "regular LFS pointer",
			data: lfsPointer,
		},
		{
			desc: "LFS pointer with CRLF",
			data: lfsPointerWithCRLF,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var b bytes.Buffer
			reader := strings.NewReader(tc.data)

			c, cleanup := runTestServer(t, defaultOptions)
			defer cleanup()

			cfg, err := json.Marshal(c)
			require.NoError(t, err)

			tlsCfg, err := json.Marshal(config.TLS{
				CertPath: certPath,
				KeyPath:  keyPath,
			})
			require.NoError(t, err)

			env := map[string]string{
				"GL_REPOSITORY":      "project-1",
				"GL_INTERNAL_CONFIG": string(cfg),
				"GITALY_TLS":         string(tlsCfg),
			}
			cfgProvider := &mapConfig{env: env}

			err = smudge(&b, reader, cfgProvider)
			require.NoError(t, err)
			require.Equal(t, testData, b.String())
		})
	}
}

func TestUnsuccessfulLfsSmudge(t *testing.T) {
	testCases := []struct {
		desc              string
		data              string
		missingEnv        string
		tlsCfg            config.TLS
		expectedError     bool
		options           gitlab.TestServerOptions
		expectedGitalyTLS string
	}{
		{
			desc:          "bad LFS pointer",
			data:          "test data",
			options:       defaultOptions,
			expectedError: false,
		},
		{
			desc:          "invalid LFS pointer",
			data:          invalidLfsPointer,
			options:       defaultOptions,
			expectedError: false,
		},
		{
			desc:          "invalid LFS pointer with non-hex characters",
			data:          invalidLfsPointerWithNonHex,
			options:       defaultOptions,
			expectedError: false,
		},
		{
			desc:          "missing GL_REPOSITORY",
			data:          lfsPointer,
			missingEnv:    "GL_REPOSITORY",
			options:       defaultOptions,
			expectedError: true,
		},
		{
			desc:          "missing GL_INTERNAL_CONFIG",
			data:          lfsPointer,
			missingEnv:    "GL_INTERNAL_CONFIG",
			options:       defaultOptions,
			expectedError: true,
		},
		{
			desc: "failed HTTP response",
			data: lfsPointer,
			options: gitlab.TestServerOptions{
				SecretToken:   secretToken,
				LfsBody:       testData,
				LfsOid:        lfsOid,
				GlRepository:  glRepository,
				LfsStatusCode: http.StatusInternalServerError,
			},
			expectedError: true,
		},
		{
			desc:          "invalid TLS paths",
			data:          lfsPointer,
			options:       defaultOptions,
			tlsCfg:        config.TLS{CertPath: "fake-path", KeyPath: "not-real"},
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c, cleanup := runTestServer(t, tc.options)
			defer cleanup()

			cfg, err := json.Marshal(c)
			require.NoError(t, err)

			tlsCfg, err := json.Marshal(tc.tlsCfg)
			require.NoError(t, err)

			env := map[string]string{
				"GL_REPOSITORY":      "project-1",
				"GL_INTERNAL_CONFIG": string(cfg),
				"GITALY_TLS":         string(tlsCfg),
			}

			if tc.missingEnv != "" {
				delete(env, tc.missingEnv)
			}

			cfgProvider := &mapConfig{env: env}

			var b bytes.Buffer
			reader := strings.NewReader(tc.data)

			err = smudge(&b, reader, cfgProvider)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.data, b.String())
			}
		})
	}
}
