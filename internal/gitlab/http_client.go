package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	promcfg "gitlab.com/gitlab-org/gitaly/v15/internal/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/prometheus/metrics"
	"gitlab.com/gitlab-org/gitaly/v15/internal/version"
	"gitlab.com/gitlab-org/gitlab-shell/v14/client"
)

var glIDRegex = regexp.MustCompile(`\A[0-9]+\z`)

// HTTPClient is an HTTP client used to talk to the internal GitLab Rails API.
type HTTPClient struct {
	*client.GitlabNetClient
	latencyMetric metrics.HistogramVec
	logger        logrus.FieldLogger
}

// NewHTTPClient creates an HTTP client to talk to the Rails internal API
func NewHTTPClient(
	logger logrus.FieldLogger,
	gitlabCfg config.Gitlab,
	tlsCfg config.TLS,
	promCfg promcfg.Config,
) (*HTTPClient, error) {
	url, err := url.PathUnescape(gitlabCfg.URL)
	if err != nil {
		return nil, err
	}

	var opts []client.HTTPClientOpt
	if tlsCfg.CertPath != "" && tlsCfg.KeyPath != "" {
		opts = append(opts, client.WithClientCert(tlsCfg.CertPath, tlsCfg.KeyPath))
	}

	httpClient, err := client.NewHTTPClientWithOpts(
		url,
		gitlabCfg.RelativeURLRoot,
		gitlabCfg.HTTPSettings.CAFile,
		gitlabCfg.HTTPSettings.CAPath,
		uint64(gitlabCfg.HTTPSettings.ReadTimeout),
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("building new HTTP client for GitLab API: %w", err)
	}

	if httpClient == nil {
		return nil, fmt.Errorf("%s is not a valid url", gitlabCfg.URL)
	}

	secret, err := os.ReadFile(gitlabCfg.SecretFile)
	if err != nil {
		return nil, fmt.Errorf("reading secret file: %w", err)
	}

	gitlabnetClient, err := client.NewGitlabNetClient(gitlabCfg.HTTPSettings.User, gitlabCfg.HTTPSettings.Password, string(secret), httpClient)
	if err != nil {
		return nil, fmt.Errorf("instantiating gitlab net client: %w", err)
	}

	gitlabnetClient.SetUserAgent("gitaly/" + version.GetVersion())

	return &HTTPClient{
		GitlabNetClient: gitlabnetClient,
		latencyMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_gitlab_api_latency_seconds",
				Help:    "Latency between posting to GitLab's `/internal/` APIs and receiving a response",
				Buckets: promCfg.GRPCLatencyBuckets,
			},
			[]string{"endpoint"},
		),
		logger: logger.WithField("component", "gitlab_http_client"),
	}, nil
}

// Describe describes Prometheus metrics exposed by the HTTPClient.
func (c *HTTPClient) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect collects Prometheus metrics exposed by the HTTPClient.
func (c *HTTPClient) Collect(metrics chan<- prometheus.Metric) {
	c.latencyMetric.Collect(metrics)
}

// allowedRequest is a request for the internal gitlab api /allowed endpoint
type allowedRequest struct {
	Action       string `json:"action,omitempty"`
	GLRepository string `json:"gl_repository,omitempty"`
	Project      string `json:"project,omitempty"`
	Changes      string `json:"changes,omitempty"`
	Protocol     string `json:"protocol,omitempty"`
	Env          string `json:"env,omitempty"`
	Username     string `json:"username,omitempty"`
	KeyID        string `json:"key_id,omitempty"`
	UserID       string `json:"user_id,omitempty"`
}

func (a *allowedRequest) parseAndSetGLID(glID string) error {
	var value string

	switch {
	case strings.HasPrefix(glID, "username-"):
		a.Username = strings.TrimPrefix(glID, "username-")
		return nil
	case strings.HasPrefix(glID, "key-"):
		a.KeyID = strings.TrimPrefix(glID, "key-")
		value = a.KeyID
	case strings.HasPrefix(glID, "user-"):
		a.UserID = strings.TrimPrefix(glID, "user-")
		value = a.UserID
	}

	if !glIDRegex.MatchString(value) {
		return fmt.Errorf("gl_id='%s' is invalid", glID)
	}

	return nil
}

// allowedResponse is a response for the internal gitlab api's /allowed endpoint with a subset
// of fields
type allowedResponse struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
}

// Allowed checks if a ref change for a given repository is allowed through the gitlab internal api /allowed endpoint
func (c *HTTPClient) Allowed(ctx context.Context, params AllowedParams) (bool, string, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("allowed")).ObserveDuration()

	gitObjDirVars, err := marshallGitObjectDirs(params.GitObjectDirectory, params.GitAlternateObjectDirectories)
	if err != nil {
		return false, "", fmt.Errorf("when getting git object directories json encoded string: %w", err)
	}

	req := allowedRequest{
		Action:       "git-receive-pack",
		GLRepository: params.GLRepository,
		Changes:      params.Changes,
		Protocol:     params.GLProtocol,
		Project:      strings.Replace(params.RepoPath, "'", "", -1),
		Env:          gitObjDirVars,
	}

	if err := req.parseAndSetGLID(params.GLID); err != nil {
		return false, "", fmt.Errorf("setting gl_id: %w", err)
	}

	resp, err := c.Post(ctx, "/allowed", &req)
	if err != nil {
		return false, "", err
	}
	defer c.finalizeResponse(resp)

	var response allowedResponse

	switch resp.StatusCode {
	case http.StatusOK,
		http.StatusMultipleChoices:

		mtype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err != nil {
			return false, "", fmt.Errorf("/allowed endpoint respond with unsupported content type: %w", err)
		}

		if mtype != "application/json" {
			return false, "", fmt.Errorf("/allowed endpoint respond with unsupported content type: %s", mtype)
		}

		if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return false, "", fmt.Errorf("decoding response from /allowed endpoint: %w", err)
		}
	default:
		return false, "", fmt.Errorf("gitlab api is not accessible: %d", resp.StatusCode)
	}

	return response.Status, response.Message, nil
}

type preReceiveResponse struct {
	ReferenceCounterIncreased bool `json:"reference_counter_increased"`
}

// PreReceive increases the reference counter for a push for a given gl_repository through the gitlab internal API /pre_receive endpoint
func (c *HTTPClient) PreReceive(ctx context.Context, glRepository string) (bool, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("pre-receive")).ObserveDuration()

	resp, err := c.Post(ctx, "/pre_receive", map[string]string{"gl_repository": glRepository})
	if err != nil {
		return false, fmt.Errorf("http post to gitlab api /pre_receive endpoint: %w", err)
	}

	defer c.finalizeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("pre-receive call failed with status: %d", resp.StatusCode)
	}

	mtype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return false, fmt.Errorf("/pre_receive endpoint respond with unsupported content type: %w", err)
	}

	if mtype != "application/json" {
		return false, fmt.Errorf("/pre_receive endpoint respond with unsupported content type: %s", mtype)
	}

	var result preReceiveResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decoding response from /pre_receive endpoint: %w", err)
	}

	return result.ReferenceCounterIncreased, nil
}

// postReceiveResponse is the response the GitLab internal api provides on a successful /post_receive call
type postReceiveResponse struct {
	ReferenceCounterDecreased bool                 `json:"reference_counter_decreased"`
	Messages                  []PostReceiveMessage `json:"messages"`
}

// PostReceive decreases the reference counter for a push for a given gl_repository through the gitlab internal API /post_receive endpoint
func (c *HTTPClient) PostReceive(ctx context.Context, glRepository, glID, changes string, pushOptions ...string) (bool, []PostReceiveMessage, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("post-receive")).ObserveDuration()

	resp, err := c.Post(ctx, "/post_receive", map[string]interface{}{"gl_repository": glRepository, "identifier": glID, "changes": changes, "push_options": pushOptions})
	if err != nil {
		return false, nil, fmt.Errorf("http post to gitlab api /post_receive endpoint: %w", err)
	}

	defer c.finalizeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return false, nil, fmt.Errorf("post-receive call failed with status: %d", resp.StatusCode)
	}

	mtype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return false, nil, fmt.Errorf("/post_receive endpoint respond with invalid content type: %w", err)
	}

	if mtype != "application/json" {
		return false, nil, fmt.Errorf("/post_receive endpoint respond with unsupported content type: %s", mtype)
	}

	var result postReceiveResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, nil, fmt.Errorf("decoding response from /post_receive endpoint: %w", err)
	}

	return result.ReferenceCounterDecreased, result.Messages, nil
}

// Check performs an HTTP request to the internal/check API endpoint to verify
// the connection and tokens. It returns basic information of the installed
// GitLab
func (c *HTTPClient) Check(ctx context.Context) (*CheckInfo, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("check")).ObserveDuration()

	resp, err := c.Get(ctx, "/check")
	if err != nil {
		return nil, fmt.Errorf("HTTP GET to GitLab endpoint /check failed: %w", err)
	}

	defer c.finalizeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Check HTTP request failed with status: %d", resp.StatusCode)
	}

	var info CheckInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode response from /check endpoint: %w", err)
	}

	return &info, nil
}

// Features performs an HTTP request to the /features API endpoint to check
// if any feature flags are enabled.
func (c *HTTPClient) features(ctx context.Context) ([]Feature, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("feature")).ObserveDuration()

	resp, err := c.DoRequest(
		ctx,
		http.MethodGet,
		"/api/v4/features",
		nil,
	)
	if err != nil {
		return nil, err
	}

	defer c.finalizeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Feature HTTP request failed with status: %d", resp.StatusCode)
	}

	var features []Feature

	if err := json.NewDecoder(resp.Body).Decode(&features); err != nil {
		return nil, fmt.Errorf("failed to decode response from /feature endpoint: %w", err)
	}

	return features, nil
}

func (c *HTTPClient) finalizeResponse(resp *http.Response) {
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		c.logger.WithError(err).Errorf("discard body error for the request %q", resp.Request.RequestURI)
	}
	if err := resp.Body.Close(); err != nil {
		c.logger.WithError(err).Errorf("close body error for the request %q", resp.Request.RequestURI)
	}
}

// marshallGitObjectDirs generates a json encoded string containing GIT_OBJECT_DIRECTORY_RELATIVE, and GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE
func marshallGitObjectDirs(gitObjectDirRel string, gitAltObjectDirsRel []string) (string, error) {
	envString, err := json.Marshal(map[string]interface{}{
		"GIT_OBJECT_DIRECTORY_RELATIVE":             gitObjectDirRel,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE": gitAltObjectDirsRel,
	})
	if err != nil {
		return "", err
	}

	return string(envString), nil
}

var gitalyFeaturePrefix = "gitaly_"

// Features calls Gitlab's /features API and
// returns all feature flags that are explicitly turned on.
// NOTE: We only inject a feature flag as enabled if it is explicitly turned on
// if a boolean feature gate is set to true. If a boolean feature gate is set to
// false, we don't have a way of knowing whether or not a feature flag should be
// turned on. In other words, whatever uses this function should expect that
// feature toggling by percentage or project will not be seen as an enabled
// feature flag. Only feature flags that are turned completely on will be
// injected into the outgoing context as enabled.
func (c *HTTPClient) Features(ctx context.Context) (map[featureflag.FeatureFlag]bool, error) {
	features, err := c.features(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting features: %w", err)
	}

	featureFlags := make(map[featureflag.FeatureFlag]bool)

	for _, feature := range features {
		var featureEnabled bool

		if !strings.HasPrefix(feature.Name, gitalyFeaturePrefix) {
			continue
		}

		featureName := strings.TrimPrefix(feature.Name, gitalyFeaturePrefix)

		if feature.State == "off" {
			featureFlags[featureflag.FeatureFlag{Name: featureName}] = false
		} else if feature.State == "on" {
			if len(feature.Gates) == 0 {
				// If there are no feature gates, then we consider the
				// feature as turned on if the state is "on". Otherwise,
				// we don't make a decision.
				featureEnabled = true
			} else if len(feature.Gates) > 1 {
				// If there are more than 1 feature gate that means
				// there is a feature gate with a Key other than
				// boolean. In this case, we don't have a way to
				// determine if the flag should be on or off.
				continue
			} else {
				// Once we get here, we know that there is only 1
				// feature gate. If it is a boolean, then that
				// determines whether the feature is turned on or off.
				if feature.Gates[0].Key != "boolean" {
					continue
				}

				v, ok := feature.Gates[0].Value.(bool)
				if !ok {
					ctxlogrus.Extract(ctx).
						WithField("feature_flag", feature.Name).
						Error("feature gate value not a bool")
					continue
				}

				featureEnabled = v
			}
			featureFlags[featureflag.FeatureFlag{Name: featureName}] = featureEnabled
		}

	}

	return featureFlags, nil
}
