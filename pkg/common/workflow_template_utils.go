package common

import (
	"context"
	"fmt"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/cenkalti/backoff/v4"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Backoff configuration constants for waiting on WorkflowTemplate readiness
const (
	// WorkflowTemplateBackoffInitialInterval is the initial wait time before the first retry
	WorkflowTemplateBackoffInitialInterval = 100 * time.Millisecond
	// WorkflowTemplateBackoffMaxInterval is the maximum wait time between retries
	WorkflowTemplateBackoffMaxInterval = 2 * time.Second
	// WorkflowTemplateBackoffMaxElapsedTime is the total deadline for all retries
	WorkflowTemplateBackoffMaxElapsedTime = 30 * time.Second
)

// ResourceGetter is a minimal interface for getting resources from the API server.
// This allows the utility to work with different client implementations (e.g., ArgoResourceClient).
type ResourceGetter interface {
	Get(ctx context.Context, name, namespace string, obj client.Object) error
}

// WaitForWorkflowTemplateReady waits for the WorkflowTemplate to be queryable with exponential backoff.
// This prevents race conditions where resources referencing the template (Workflow, CronWorkflow)
// are created before the template is visible in the API server's informer cache.
//
// The getter parameter should be an ArgoResourceClient or any type that implements the Get method.
// Returns nil if the template is ready, or an error if it fails after retries.
func WaitForWorkflowTemplateReady(ctx context.Context, getter ResourceGetter, templateName, namespace string) error {
	if templateName == "" {
		return fmt.Errorf("template name cannot be empty")
	}

	// Configure exponential backoff with a deadline
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = WorkflowTemplateBackoffInitialInterval
	backoffConfig.MaxInterval = WorkflowTemplateBackoffMaxInterval
	backoffConfig.MaxElapsedTime = WorkflowTemplateBackoffMaxElapsedTime

	// Create operation to retry
	operation := func() error {
		template := &argowfv1.WorkflowTemplate{}
		err := getter.Get(ctx, templateName, namespace, template)
		if err != nil {
			if errors.IsNotFound(err) {
				logger.V(1).Info("WorkflowTemplate not found yet, retrying",
					"name", templateName, "namespace", namespace)
				return err // Retryable error
			}
			// Other errors are not retryable
			logger.Error(err, "Error checking WorkflowTemplate", "name", templateName, "namespace", namespace)
			return backoff.Permanent(err)
		}

		// Verify the template has been fully persisted (has UID)
		if template.UID == "" {
			logger.V(1).Info("WorkflowTemplate exists but has no UID yet, retrying",
				"name", templateName, "namespace", namespace)
			return fmt.Errorf("WorkflowTemplate %s has no UID yet", templateName)
		}

		logger.Info("WorkflowTemplate is ready and verified",
			"name", templateName, "namespace", namespace, "uid", template.UID)
		return nil
	}

	// Retry the operation with backoff
	err := backoff.Retry(operation, backoffConfig)
	if err != nil {
		logger.Info("WorkflowTemplate not ready after retries",
			"name", templateName, "namespace", namespace,
			"maxElapsedTime", WorkflowTemplateBackoffMaxElapsedTime,
			"error", err)
		return err
	}

	return nil
}

// IsWorkflowTemplateReady is a convenience wrapper that returns a boolean instead of error.
// Use WaitForWorkflowTemplateReady if you need the actual error for logging or handling.
func IsWorkflowTemplateReady(ctx context.Context, getter ResourceGetter, templateName, namespace string) bool {
	return WaitForWorkflowTemplateReady(ctx, getter, templateName, namespace) == nil
}
