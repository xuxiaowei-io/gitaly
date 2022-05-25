package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

func TestConfigFromEnvironment(t *testing.T) {
	gitlabCfg := config.Gitlab{
		URL:             "https://example.com",
		RelativeURLRoot: "gitlab",
		HTTPSettings: config.HTTPSettings{
			ReadTimeout: 1,
			User:        "user",
			Password:    "correcthorsebatterystaple",
			CAFile:      "/ca/file",
			CAPath:      "/ca/path",
			SelfSigned:  true,
		},
		SecretFile: "/secret/path",
	}

	tlsCfg := config.TLS{
		CertPath: "/cert/path",
		KeyPath:  "/key/path",
	}

	marshalledGitlabCfg, err := json.Marshal(gitlabCfg)
	require.NoError(t, err)

	marshalledTLSCfg, err := json.Marshal(tlsCfg)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc        string
		environment []string
		expectedErr string
		expectedCfg Config
	}{
		{
			desc:        "empty environment",
			expectedErr: "error loading project: GL_REPOSITORY is not defined",
		},
		{
			desc: "successful",
			environment: []string{
				"GL_REPOSITORY=repo",
				"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
				"GITALY_TLS=" + string(marshalledTLSCfg),
			},
			expectedCfg: Config{
				GlRepository: "repo",
				Gitlab:       gitlabCfg,
				TLS:          tlsCfg,
			},
		},
		{
			desc: "missing GlRepository",
			environment: []string{
				"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
				"GITALY_TLS=" + string(marshalledTLSCfg),
			},
			expectedErr: "error loading project: GL_REPOSITORY is not defined",
		},
		{
			desc: "missing Gitlab config",
			environment: []string{
				"GL_REPOSITORY=repo",
				"GITALY_TLS=" + string(marshalledTLSCfg),
			},
			expectedErr: "unable to retrieve GL_INTERNAL_CONFIG",
		},
		{
			desc: "invalid Gitlab config",
			environment: []string{
				"GL_REPOSITORY=repo",
				"GL_INTERNAL_CONFIG=invalid",
				"GITALY_TLS=" + string(marshalledTLSCfg),
			},
			expectedErr: "unable to unmarshal GL_INTERNAL_CONFIG: invalid character 'i' looking for beginning of value",
		},
		{
			desc: "missing TLS config",
			environment: []string{
				"GL_REPOSITORY=repo",
				"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
			},
			expectedErr: "unable to retrieve GITALY_TLS",
		},
		{
			desc: "invalid TLS config",
			environment: []string{
				"GL_REPOSITORY=repo",
				"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
				"GITALY_TLS=invalid",
			},
			expectedErr: "unable to unmarshal GITALY_TLS: invalid character 'i' looking for beginning of value",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, err := configFromEnvironment(tc.environment)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedCfg, cfg)
		})
	}
}
