package client

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

const secret = "it's a secret"

func TestJWTAuthenticationHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprint(w, r.Header.Get(apiSecretHeaderName))
		require.NoError(t, err)
	}))
	defer server.Close()

	tests := []struct {
		secret string
		method string
	}{
		{
			secret: secret,
			method: http.MethodGet,
		},
		{
			secret: secret,
			method: http.MethodPost,
		},
		{
			secret: "\n\t " + secret + "\t \n",
			method: http.MethodGet,
		},
		{
			secret: "\n \t" + secret + "\n\t ",
			method: http.MethodPost,
		},
	}

	for _, tc := range tests {
		t.Run(tc.method+" with "+tc.secret, func(t *testing.T) {
			gitlabnet := &GitlabNetClient{
				httpClient: &HTTPClient{Client: server.Client(), Host: server.URL},
				secret:     tc.secret,
			}

			response, err := gitlabnet.DoRequest(testhelper.Context(t), tc.method, "/jwt_auth", nil)
			require.NoError(t, err)
			require.NotNil(t, response)
			defer response.Body.Close()

			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)

			claims := &jwt.RegisteredClaims{}
			token, err := jwt.ParseWithClaims(string(responseBody), claims, func(token *jwt.Token) (interface{}, error) {
				return []byte(secret), nil
			})
			require.NoError(t, err)
			require.True(t, token.Valid)
			require.Equal(t, "gitlab-shell", claims.Issuer)
			require.WithinDuration(t, time.Now().Truncate(time.Second), claims.IssuedAt.Time, time.Second)
			require.WithinDuration(t, time.Now().Truncate(time.Second).Add(time.Minute), claims.ExpiresAt.Time, time.Second)
		})
	}
}
