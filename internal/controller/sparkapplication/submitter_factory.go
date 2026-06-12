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
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

const (
	SubmissionStrategySparkSubmit   = "spark-submit"
	SubmissionStrategyRestSubmitter = "rest-submitter"
)

// REST submitter flags — registered via RegisterSubmitterFlags.
var (
	// Service connection
	submitterServiceURL     string
	submitterStartupTimeout time.Duration

	// Submission
	submitterRequestTimeout time.Duration

	// Retry with exponential backoff
	submitterMaxRetries     int
	submitterInitialBackoff time.Duration

	// TLS
	submitterTLSEnabled    bool
	submitterTLSCertFile   string
	submitterTLSKeyFile    string
	submitterTLSCACertFile string
)

// RegisterSubmitterFlags registers CLI flags for all submission strategies.
func RegisterSubmitterFlags(cmd *cobra.Command) {
	// Service connection
	cmd.Flags().StringVar(&submitterServiceURL, "submitter-service-url", "", "Full submit endpoint URL of the submitter service, e.g. http://host:8080/api/v1/spark-submit (required when submission-strategy=rest-submitter).")
	cmd.Flags().DurationVar(&submitterStartupTimeout, "submitter-startup-timeout", 5*time.Minute, "How long the controller waits for the submitter service to become reachable at startup.")

	// Submission
	cmd.Flags().DurationVar(&submitterRequestTimeout, "submitter-request-timeout", 2*time.Minute, "HTTP request timeout per spark submission attempt.")

	// Retry with exponential backoff
	cmd.Flags().IntVar(&submitterMaxRetries, "submitter-max-retries", 3, "Max retry attempts for transient spark submission failures.")
	cmd.Flags().DurationVar(&submitterInitialBackoff, "submitter-initial-backoff", 2*time.Second, "Initial backoff duration before the first retry. Doubles on each subsequent attempt.")

	// TLS
	cmd.Flags().BoolVar(&submitterTLSEnabled, "submitter-tls-enabled", false, "Enable mTLS for submitter connections.")
	cmd.Flags().StringVar(&submitterTLSCertFile, "submitter-tls-cert-file", "", "Client certificate PEM file path for mTLS to the submitter service.")
	cmd.Flags().StringVar(&submitterTLSKeyFile, "submitter-tls-key-file", "", "Client private key PEM file path for mTLS to the submitter service.")
	cmd.Flags().StringVar(&submitterTLSCACertFile, "submitter-tls-ca-cert-file", "", "CA certificate PEM file path to verify the submitter server.")
}

// NewSubmitter creates the appropriate SparkApplicationSubmitter based on the strategy.
func NewSubmitter(strategy string) (SparkApplicationSubmitter, error) {
	switch strategy {
	case SubmissionStrategyRestSubmitter:
		if submitterMaxRetries < 1 {
			restLogger.Info("WARNING: --submitter-max-retries is less than 1, defaulting to 1", "configured", submitterMaxRetries)
			submitterMaxRetries = 1
		}
		var tlsCfg *TLSConfig
		if submitterTLSEnabled {
			tlsCfg = &TLSConfig{
				Enabled:    true,
				CertFile:   submitterTLSCertFile,
				KeyFile:    submitterTLSKeyFile,
				CACertFile: submitterTLSCACertFile,
			}
		}
		submitter, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{
			URL:             submitterServiceURL,
			RetryMaxRetries: submitterMaxRetries,
			RequestTimeout:  submitterRequestTimeout,
			InitialBackoff:  submitterInitialBackoff,
			TLS:             tlsCfg,
		})
		if err != nil {
			return nil, err
		}

		startupCtx, cancel := context.WithTimeout(context.Background(), submitterStartupTimeout)
		defer cancel()
		if err := submitter.WaitForConnection(startupCtx); err != nil {
			return nil, fmt.Errorf("submitter service not reachable at startup: %w", err)
		}

		restLogger.Info("Using REST submitter service", "url", submitterServiceURL)
		return submitter, nil

	case SubmissionStrategySparkSubmit:
		return &SparkSubmitter{}, nil

	default:
		return nil, fmt.Errorf("unknown submission strategy: %q", strategy)
	}
}
