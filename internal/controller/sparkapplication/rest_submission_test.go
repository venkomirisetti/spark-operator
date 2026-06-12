package sparkapplication

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestNewRestSparkSubmitter(t *testing.T) {
	t.Run("rejects empty URL", func(t *testing.T) {
		_, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{})
		assert.Error(t, err)
	})

	t.Run("rejects URL without port", func(t *testing.T) {
		_, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{URL: "http://host"})
		assert.Error(t, err)
	})

	t.Run("succeeds with valid URL", func(t *testing.T) {
		s, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{
			URL: "http://host:8080", RetryMaxRetries: 3, RequestTimeout: 10 * time.Second, InitialBackoff: 1 * time.Second,
		})
		assert.NoError(t, err)
		assert.NotNil(t, s)
	})
}

func TestWaitForConnection(t *testing.T) {
	t.Run("succeeds when port is open", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { _ = ln.Close() }()

		s := newTestSubmitter(t, "http://"+ln.Addr().String())
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		assert.NoError(t, s.WaitForConnection(ctx))
	})

	t.Run("fails when context expires", func(t *testing.T) {
		s := newTestSubmitter(t, "http://127.0.0.1:19999")
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		assert.Error(t, s.WaitForConnection(ctx))
	})
}

func TestSubmitRetry(t *testing.T) {
	setK8sEnv(t)
	t.Run("retries on 503 and succeeds", func(t *testing.T) {
		attempt := 0
		server := retryThenSuccessServer(3, http.StatusServiceUnavailable, &attempt)
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		assert.NoError(t, s.Submit(context.Background(), newTestApp()))
		assert.Equal(t, 3, attempt)
	})

	t.Run("does not retry on 400", func(t *testing.T) {
		attempt := 0
		server := retryAlwaysFailServer(http.StatusBadRequest, &attempt)
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		assert.Error(t, s.Submit(context.Background(), newTestApp()))
		assert.Equal(t, 1, attempt)
	})

	t.Run("gives up after max retries", func(t *testing.T) {
		attempt := 0
		server := retryAlwaysFailServer(http.StatusInternalServerError, &attempt)
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		assert.Error(t, s.Submit(context.Background(), newTestApp()))
		assert.Equal(t, 3, attempt)
	})
}

func TestSubmitPodTemplates(t *testing.T) {
	setK8sEnv(t)
	t.Run("sends templates inline when present", func(t *testing.T) {
		var capturedReq submitRequest
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&capturedReq)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(submitResponse{DriverPodName: "drv", SparkAppID: "id"})
		}))
		defer server.Close()

		app := newTestApp()
		app.Spec.Driver.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "driver"}}}}
		app.Spec.Executor.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "executor"}}}}

		s := newTestSubmitter(t, server.URL)
		require.NoError(t, s.Submit(context.Background(), app))
		assert.NotNil(t, capturedReq.DriverPodTemplate)
		assert.NotNil(t, capturedReq.ExecutorPodTemplate)
	})

	t.Run("omits templates when not specified", func(t *testing.T) {
		var capturedReq submitRequest
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&capturedReq)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(submitResponse{DriverPodName: "drv", SparkAppID: "id"})
		}))
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		require.NoError(t, s.Submit(context.Background(), newTestApp()))
		assert.Nil(t, capturedReq.DriverPodTemplate)
		assert.Nil(t, capturedReq.ExecutorPodTemplate)
	})
}

func TestSubmitContextCancellation(t *testing.T) {
	setK8sEnv(t)

	t.Run("returns error when context cancelled during retry backoff", func(t *testing.T) {
		attempt := 0
		server := retryAlwaysFailServer(http.StatusInternalServerError, &attempt)
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		s, _ := NewRestSparkSubmitter(RestSparkSubmitterConfig{
			URL:             server.URL,
			RetryMaxRetries: 3,
			RequestTimeout:  5 * time.Second,
			InitialBackoff:  2 * time.Second,
		})
		err := s.Submit(ctx, newTestApp())
		assert.Error(t, err)
		assert.Equal(t, 1, attempt)
	})

	t.Run("returns error on network failure", func(t *testing.T) {
		s := newTestSubmitter(t, "http://192.0.2.1:9999")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		assert.Error(t, s.Submit(ctx, newTestApp()))
	})
}

func TestParseErrorBody(t *testing.T) {
	t.Run("structured error with details", func(t *testing.T) {
		body, _ := json.Marshal(submitErrorResponse{
			Status:  422,
			Error:   "SUBMISSION_FAILED",
			Message: "Failed to submit",
			Details: "pods is forbidden",
		})
		err := parseErrorBody(body, 422)
		assert.Contains(t, err.Error(), "Failed to submit")
		assert.Contains(t, err.Error(), "pods is forbidden")
		assert.Contains(t, err.Error(), "SUBMISSION_FAILED")
	})

	t.Run("structured error without details", func(t *testing.T) {
		body, _ := json.Marshal(submitErrorResponse{
			Status:  400,
			Error:   "BAD_REQUEST",
			Message: "Invalid job configuration",
		})
		err := parseErrorBody(body, 400)
		assert.Contains(t, err.Error(), "Invalid job configuration")
		assert.Contains(t, err.Error(), "BAD_REQUEST")
		assert.NotContains(t, err.Error(), "details")
	})

	t.Run("non-JSON response body", func(t *testing.T) {
		err := parseErrorBody([]byte("Bad Gateway"), 502)
		assert.Contains(t, err.Error(), "unexpected response")
		assert.Contains(t, err.Error(), "Bad Gateway")
	})

	t.Run("truncates long response body", func(t *testing.T) {
		longBody := make([]byte, maxErrorBodyLength+100)
		for i := range longBody {
			longBody[i] = 'x'
		}
		err := parseErrorBody(longBody, 500)
		assert.Contains(t, err.Error(), "...")
		assert.LessOrEqual(t, len(err.Error()), maxErrorBodyLength+100)
	})
}

func TestCanRetry(t *testing.T) {
	s := &RestSparkSubmitter{maxRetries: 3}

	assert.True(t, s.canRetry(0, 0), "network error should retry")
	assert.True(t, s.canRetry(0, 503), "503 should retry")
	assert.True(t, s.canRetry(0, 429), "429 should retry")
	assert.True(t, s.canRetry(0, 500), "500 should retry")
	assert.False(t, s.canRetry(0, 400), "400 should not retry")
	assert.False(t, s.canRetry(0, 422), "422 should not retry")
	assert.False(t, s.canRetry(2, 503), "last attempt should not retry")
}

func TestNewSubmitter(t *testing.T) {
	t.Run("returns SparkSubmitter for spark-submit strategy", func(t *testing.T) {
		s, err := NewSubmitter(SubmissionStrategySparkSubmit)
		assert.NoError(t, err)
		assert.IsType(t, &SparkSubmitter{}, s)
	})

	t.Run("returns error for unknown strategy", func(t *testing.T) {
		_, err := NewSubmitter("unknown")
		assert.Error(t, err)
	})
}

// --- Test helpers ---

func newTestSubmitter(t *testing.T, url string) *RestSparkSubmitter {
	t.Helper()
	s, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{
		URL:             url,
		RetryMaxRetries: 3,
		RequestTimeout:  5 * time.Second,
		InitialBackoff:  10 * time.Millisecond,
	})
	require.NoError(t, err)
	return s
}

func setK8sEnv(t *testing.T) {
	t.Helper()
	t.Setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "6443")
}

func newTestApp() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "test-app", Namespace: "default"},
		Spec:       v1beta2.SparkApplicationSpec{},
	}
}

func retryThenSuccessServer(succeedOnAttempt int, failStatus int, attempt *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*attempt++
		if *attempt < succeedOnAttempt {
			w.WriteHeader(failStatus)
			_ = json.NewEncoder(w).Encode(submitErrorResponse{Message: "transient"})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(submitResponse{DriverPodName: "drv", SparkAppID: "id"})
	}))
}

func retryAlwaysFailServer(failStatus int, attempt *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*attempt++
		w.WriteHeader(failStatus)
		_ = json.NewEncoder(w).Encode(submitErrorResponse{Message: "error"})
	}))
}

// --- TLS tests ---

func TestBuildTransport_NilConfig(t *testing.T) {
	transport, err := buildTransport(nil)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestBuildTransport_Disabled(t *testing.T) {
	transport, err := buildTransport(&TLSConfig{Enabled: false})
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestBuildTransport_InvalidCACert(t *testing.T) {
	badCA := writeTempFile(t, "bad-ca.pem", "not a cert")
	_, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CACertFile: badCA,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse CA cert")
}

func TestBuildTransport_MissingCACertFile(t *testing.T) {
	_, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CACertFile: "/nonexistent/ca.crt",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read CA cert file")
}

func TestBuildTransport_PartialCertKeyErrors(t *testing.T) {
	certFile := writeTempFile(t, "tls.crt", "cert")

	t.Run("cert without key", func(t *testing.T) {
		_, err := buildTransport(&TLSConfig{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  "",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "both --submitter-tls-cert-file and --submitter-tls-key-file must be provided")
	})

	t.Run("key without cert", func(t *testing.T) {
		_, err := buildTransport(&TLSConfig{
			Enabled:  true,
			CertFile: "",
			KeyFile:  "/some/key.pem",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "both --submitter-tls-cert-file and --submitter-tls-key-file must be provided")
	})
}

func TestBuildTransport_InvalidCertKeyPair(t *testing.T) {
	certFile := writeTempFile(t, "tls.crt", "not a cert")
	keyFile := writeTempFile(t, "tls.key", "not a key")

	_, err := buildTransport(&TLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load client certificate")
}

func TestBuildTransport_ValidTLS(t *testing.T) {
	certFile, keyFile, caFile := generateTestCertFiles(t)

	transport, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CACertFile: caFile,
	})
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestBuildTransport_WhitespacePathsIgnored(t *testing.T) {
	transport, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CertFile:   "  ",
		KeyFile:    "  ",
		CACertFile: "  ",
	})
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

// --- TLS test helpers ---

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/" + name
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	return path
}

func generateTestCertFiles(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile = dir + "/tls.crt"
	keyFile = dir + "/tls.key"
	caFile = dir + "/ca.crt"

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	require.NoError(t, os.WriteFile(caFile, caPEM, 0600))

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-leaf"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	require.NoError(t, os.WriteFile(certFile, leafPEM, 0600))

	keyBytes, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	return certFile, keyFile, caFile
}
