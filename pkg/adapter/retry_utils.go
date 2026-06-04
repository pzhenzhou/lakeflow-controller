package adapter

import (
	"fmt"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// buildRetryStrategy creates an Argo RetryStrategy from a Task's RetryPolicy
// This consolidates the common retry logic used across all task executor builders
func buildRetryStrategy(task *v1alpha1.Task) *argowfv1.RetryStrategy {
	if task.RetryPolicy == nil {
		return nil
	}

	// IMPORTANT: When maxRetries is 0, we must explicitly set limit to 0
	// Returning nil would cause Argo Workflows to use its default retry limit of 3
	limit := intstr.FromInt32(task.RetryPolicy.MaxRetries)
	retryStrategy := &argowfv1.RetryStrategy{
		Limit:       &limit,
		RetryPolicy: argowfv1.RetryPolicyOnFailure,
	}

	// Add backoff configuration if specified (only meaningful when MaxRetries > 0)
	if task.RetryPolicy.MaxRetries > 0 && task.RetryPolicy.BackOff.Duration > 0 {
		// Convert factor to IntOrString - use integer when possible to avoid Argo parsing issues
		// Argo Workflows has trouble parsing string values like "1.0" as integers
		var factor intstr.IntOrString
		if task.RetryPolicy.BackoffFactor == float64(int32(task.RetryPolicy.BackoffFactor)) {
			// Factor is a whole number (e.g., 1.0, 2.0, 3.0) - use integer format
			factor = intstr.FromInt32(int32(task.RetryPolicy.BackoffFactor))
		} else {
			// Factor has decimal places (e.g., 1.5, 2.3) - use string format
			factor = intstr.FromString(fmt.Sprintf("%.1f", task.RetryPolicy.BackoffFactor))
		}

		retryStrategy.Backoff = &argowfv1.Backoff{
			Duration: task.RetryPolicy.BackOff.Duration.String(),
			Factor:   &factor,
		}

		// Add MaxDuration if specified (only applicable when Backoff is set)
		if task.RetryPolicy.MaxDuration.Duration > 0 {
			retryStrategy.Backoff.MaxDuration = task.RetryPolicy.MaxDuration.Duration.String()
		}
	}

	return retryStrategy
}

// applySparkRetryDefaults enforces NO RETRIES (fail fast) for all Spark tasks.
//
// IMPORTANT: Spark jobs are long-running and expensive. Retrying them automatically
// without investigation is rarely desirable and can waste significant cluster resources.
// Therefore, this function ALWAYS sets MaxRetries=0, ignoring any user configuration.
//
// If retry behavior is needed for Spark jobs in the future, it should be handled
// at a higher level (e.g., workflow-level retry or manual rerun) rather than
// automatic Argo node-level retries.
func applySparkRetryDefaults(task *v1alpha1.Task) {
	// Log if user attempted to configure retries (will be ignored)
	if task.RetryPolicy != nil && task.RetryPolicy.MaxRetries > 0 {
		logger.Info("Spark task retry policy ignored - Spark jobs do not support automatic retries",
			"task", task.Name,
			"requestedMaxRetries", task.RetryPolicy.MaxRetries,
			"enforcedMaxRetries", 0,
			"reason", "Spark jobs are long-running and expensive; use workflow-level retry or manual rerun instead")
	}

	// Always enforce fail-fast behavior for Spark tasks
	task.RetryPolicy = &v1alpha1.TaskRetryPolicy{
		MaxRetries: 0,
	}
}
