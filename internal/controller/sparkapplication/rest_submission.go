/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

const (
	contentTypeJSON          = "application/json"
	maxResponseBodySizeBytes = 1048576 // 1MB
	maxErrorBodyLength       = 200
	startupPollInterval      = 1 * time.Second
	maxIdleConns             = 100
	maxIdleConnsPerHost      = 100
	idleConnTimeout          = 90 * time.Second
)

var restLogger = ctrl.Log.WithName("rest-submitter")

// TLSConfig holds TLS settings for the submitter REST client.
type TLSConfig struct {
	Enabled    bool
	CertFile   string
	KeyFile    string
	CACertFile string
}

// RestSparkSubmitterConfig holds submitter connection settings.
type RestSparkSubmitterConfig struct {
	URL             string
	RetryMaxRetries int
	RequestTimeout  time.Duration
	InitialBackoff  time.Duration
	TLS             *TLSConfig
}

// RestSparkSubmitter submits a SparkApplication via the REST submitter service.
type RestSparkSubmitter struct {
	httpClient     *http.Client
	submitURL      string
	hostAddr       string
	maxRetries     int
	initialBackoff time.Duration
}

var _ SparkApplicationSubmitter = &RestSparkSubmitter{}

// submitRequest is the submit API request payload.
type submitRequest struct {
	SparkSubmitArgs     []string                `json:"spark_submit_args"`
	DriverPodTemplate   *corev1.PodTemplateSpec `json:"driver_pod_template,omitempty"`
	ExecutorPodTemplate *corev1.PodTemplateSpec `json:"executor_pod_template,omitempty"`
}

// submitResponse is the submit API success response.
type submitResponse struct {
	AppName       string `json:"app_name"`
	Message       string `json:"message"`
	SubmittedAt   string `json:"submitted_at"`
	SparkAppID    string `json:"spark_app_id"`
	DriverPodName string `json:"driver_pod_name"`
	DriverPodUID  string `json:"driver_pod_uid"`
	Namespace     string `json:"namespace"`
}

// submitErrorResponse is the submitter error payload.
type submitErrorResponse struct {
	Timestamp string `json:"timestamp"`
	Status    int    `json:"status"`
	Error     string `json:"error"`
	Message   string `json:"message"`
	Details   string `json:"details"`
}

// NewRestSparkSubmitter creates a REST client for the submitter service.
func NewRestSparkSubmitter(cfg RestSparkSubmitterConfig) (*RestSparkSubmitter, error) {
	submitURL := strings.TrimSpace(cfg.URL)
	if submitURL == "" {
		return nil, fmt.Errorf("submitter URL is required")
	}

	u, err := url.Parse(submitURL)
	if err != nil {
		return nil, fmt.Errorf("invalid submitter URL %q: %w", cfg.URL, err)
	}
	if u.Port() == "" {
		return nil, fmt.Errorf("submitter URL %q must include an explicit port", cfg.URL)
	}

	transport, err := buildTransport(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	httpClient := &http.Client{
		Timeout:   cfg.RequestTimeout,
		Transport: transport,
	}

	return &RestSparkSubmitter{
		httpClient:     httpClient,
		submitURL:      submitURL,
		hostAddr:       u.Host,
		maxRetries:     cfg.RetryMaxRetries,
		initialBackoff: cfg.InitialBackoff,
	}, nil
}

// buildTransport creates an HTTP transport, optionally with TLS configured.
func buildTransport(tlsCfg *TLSConfig) (http.RoundTripper, error) {
	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     idleConnTimeout,
	}

	if tlsCfg == nil || !tlsCfg.Enabled {
		return transport, nil
	}

	tlsConfig := &tls.Config{}

	caCertFile := strings.TrimSpace(tlsCfg.CACertFile)
	certFile := strings.TrimSpace(tlsCfg.CertFile)
	keyFile := strings.TrimSpace(tlsCfg.KeyFile)

	if caCertFile != "" {
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert file %q: %w", caCertFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert from %q", caCertFile)
		}
		tlsConfig.RootCAs = pool
	}

	if (certFile != "") != (keyFile != "") {
		return nil, fmt.Errorf("both --submitter-tls-cert-file and --submitter-tls-key-file must be provided for mTLS, got cert=%q key=%q", certFile, keyFile)
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate %q / %q: %w", certFile, keyFile, err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		restLogger.Info("TLS enabled with client certificate (mTLS)", "certFile", certFile, "keyFile", keyFile)
	} else {
		restLogger.Info("TLS enabled (server verification only)", "caCertFile", caCertFile)
	}

	transport.TLSClientConfig = tlsConfig
	return transport, nil
}

// WaitForConnection blocks until the submitter service is reachable or the context expires.
func (c *RestSparkSubmitter) WaitForConnection(ctx context.Context) error {
	restLogger.Info("Waiting for submitter service to be ready", "addr", c.hostAddr)
	for {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.hostAddr)
		if err == nil {
			_ = conn.Close()
			restLogger.Info("Submitter service is ready", "addr", c.hostAddr)
			return nil
		}
		restLogger.V(1).Info("Submitter service not yet ready, retrying", "addr", c.hostAddr, "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for submitter service at %s: %w", c.hostAddr, ctx.Err())
		case <-time.After(startupPollInterval):
		}
	}
}

// Submit implements SparkApplicationSubmitter interface.
func (c *RestSparkSubmitter) Submit(ctx context.Context, app *v1beta2.SparkApplication) error {
	args, err := buildSparkSubmitArgs(app, true)
	if err != nil {
		return fmt.Errorf("failed to build spark-submit arguments: %v", err)
	}

	request := newSubmitRequest(args, app)
	restLogger.Info("Submitting spark application via REST", "name", app.Name, "namespace", app.Namespace)

	response, err := c.doSubmitWithRetry(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to submit Spark application %s/%s: %w", app.Namespace, app.Name, err)
	}

	restLogger.Info("Submitted successfully",
		"name", app.Name,
		"driverPod", response.DriverPodName,
		"sparkAppId", response.SparkAppID,
	)
	return nil
}

func newSubmitRequest(args []string, app *v1beta2.SparkApplication) *submitRequest {
	request := &submitRequest{SparkSubmitArgs: args}
	if app.Spec.Driver.Template != nil {
		request.DriverPodTemplate = app.Spec.Driver.Template
	}
	if app.Spec.Executor.Template != nil {
		request.ExecutorPodTemplate = app.Spec.Executor.Template
	}
	return request
}

// doSubmitWithRetry submits the request with exponential backoff.
func (c *RestSparkSubmitter) doSubmitWithRetry(ctx context.Context, request *submitRequest) (*submitResponse, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal submission request to JSON: %w", err)
	}

	var lastErr error
	backoff := c.initialBackoff

	for attempt := 0; attempt < c.maxRetries; attempt++ {
		result, statusCode, postErr := c.tryPost(ctx, jsonData)
		if postErr == nil {
			return result, nil
		}
		lastErr = postErr

		if !c.canRetry(attempt, statusCode) {
			return nil, lastErr
		}

		backoff, err = c.waitForRetry(ctx, attempt, backoff, lastErr, statusCode)
		if err != nil {
			return nil, err
		}
	}

	return nil, lastErr
}

func (c *RestSparkSubmitter) tryPost(ctx context.Context, jsonData []byte) (*submitResponse, int, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.submitURL, bytes.NewReader(jsonData))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create HTTP request to %s: %w", c.submitURL, err)
	}
	httpReq.Header.Set("Content-Type", contentTypeJSON)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to send HTTP request to %s: %w", c.submitURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	return parseSubmitResponse(resp)
}

func (c *RestSparkSubmitter) canRetry(attempt, statusCode int) bool {
	if attempt >= c.maxRetries-1 {
		return false
	}
	if statusCode == 0 {
		return true // network error
	}
	return statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func (c *RestSparkSubmitter) waitForRetry(ctx context.Context, attempt int, backoff time.Duration, lastErr error, statusCode int) (time.Duration, error) {
	if statusCode == 0 {
		restLogger.V(1).Info("Retrying after network error", "attempt", attempt+1, "maxRetries", c.maxRetries, "backoff", backoff, "error", lastErr)
	} else {
		restLogger.V(1).Info("Retrying after server error", "attempt", attempt+1, "maxRetries", c.maxRetries, "backoff", backoff, "statusCode", statusCode)
	}
	select {
	case <-ctx.Done():
		return backoff, fmt.Errorf("retry cancelled: %w", ctx.Err())
	case <-time.After(backoff):
		return backoff * 2, nil
	}
}

func parseSubmitResponse(resp *http.Response) (*submitResponse, int, error) {
	statusCode := resp.StatusCode
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySizeBytes))

	if readErr != nil {
		return nil, statusCode, fmt.Errorf("failed to read response body from submitter service (status %d): %w", statusCode, readErr)
	}

	switch statusCode {
	case http.StatusCreated:
		var r submitResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, statusCode, fmt.Errorf("failed to parse success response from submitter service (status %d): %w", statusCode, err)
		}
		return &r, statusCode, nil
	default:
		return nil, statusCode, parseErrorBody(body, statusCode)
	}
}

func parseErrorBody(body []byte, statusCode int) error {
	var errResp submitErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Message != "" {
		if errResp.Details != "" {
			return fmt.Errorf("submitter service returned error: %s (HTTP %d, error code: %s): %s",
				errResp.Message, statusCode, errResp.Error, errResp.Details)
		}
		return fmt.Errorf("submitter service returned error: %s (HTTP %d, error code: %s)",
			errResp.Message, statusCode, errResp.Error)
	}

	bodyStr := string(body)
	if len(bodyStr) > maxErrorBodyLength {
		bodyStr = bodyStr[:maxErrorBodyLength] + "..."
	}
	return fmt.Errorf("submitter service returned unexpected response (HTTP %d): %s", statusCode, bodyStr)
}
