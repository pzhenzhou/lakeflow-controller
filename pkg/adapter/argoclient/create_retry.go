package argoclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/cenkalti/backoff/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Backoff configuration constants for workflow creation retry
const (
	// WorkflowCreationBackoffInitialInterval is the initial wait time before the first retry
	WorkflowCreationBackoffInitialInterval = 500 * time.Millisecond
	// WorkflowCreationBackoffMaxInterval is the maximum wait time between retries
	WorkflowCreationBackoffMaxInterval = 5 * time.Second
	// WorkflowCreationBackoffMaxElapsedTime is the total deadline for all retries
	WorkflowCreationBackoffMaxElapsedTime = 30 * time.Second
)

// CreateWorkflowWithRetry creates an Argo Workflow with retry logic to handle
// Argo informer cache lag (template not found errors).
//
// When a WorkflowTemplate and Workflow are created in quick succession,
// Argo's informer cache may not have synced the template yet, causing
// "workflowtemplates.argoproj.io not found" errors. This function handles
// such failures by deleting the failed workflow and retrying with exponential backoff.
//
// Parameters:
//   - ctx: context for cancellation
//   - client: ArgoResourceClient for K8s CRUD operations
//   - workflow: the Argo Workflow to create (typically uses GenerateName)
//   - namespace: Kubernetes namespace
//
// Returns:
//   - createdWorkflowName: the actual name of the created workflow
//   - error: nil on success
func CreateWorkflowWithRetry(
	ctx context.Context,
	client ArgoResourceClient,
	workflow *argowfv1.Workflow,
	namespace string,
) (string, error) {
	if workflow == nil {
		return "", fmt.Errorf("workflow is required")
	}
	if namespace == "" {
		namespace = workflow.Namespace
	}

	// Configure exponential backoff
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = WorkflowCreationBackoffInitialInterval
	backoffConfig.MaxInterval = WorkflowCreationBackoffMaxInterval
	backoffConfig.MaxElapsedTime = WorkflowCreationBackoffMaxElapsedTime

	// State tracking across retry attempts
	var (
		createdWorkflowName string
		workflowOriginal    = workflow.DeepCopy() // Preserve original for retries
	)

	operation := func() error {
		// Step 1: Check existing workflow from previous attempt
		if createdWorkflowName != "" {
			existing := &argowfv1.Workflow{}
			err := client.Get(ctx, createdWorkflowName, namespace, existing)
			if err == nil {
				// Workflow exists, check its status
				if IsWorkflowTemplateNotFoundError(existing.Status.Message) {
					// Previous attempt failed, delete and retry
					logger.Info("Deleting failed workflow for retry",
						"workflow", createdWorkflowName,
						"phase", existing.Status.Phase,
						"message", existing.Status.Message)

					if delErr := client.Delete(ctx, createdWorkflowName, namespace, &argowfv1.Workflow{}); delErr != nil {
						logger.Error(delErr, "Failed to delete failed workflow, will retry anyway",
							"workflow", createdWorkflowName)
					}
					createdWorkflowName = ""
					// Fall through to create new workflow
					return fmt.Errorf("workflow failed due to template not found: %s", existing.Status.Message)
				}
				if existing.Status.Phase == argowfv1.WorkflowError || existing.Status.Phase == argowfv1.WorkflowFailed {
					return backoff.Permanent(fmt.Errorf("workflow failed: %s", existing.Status.Message))
				}
				if existing.Status.Phase == "" && existing.Status.Message == "" {
					// Status not populated yet, retry to avoid race with controller
					return fmt.Errorf("workflow status not ready yet")
				}
				// Workflow exists in acceptable state
				logger.Info("Workflow exists in acceptable state",
					"workflow", createdWorkflowName,
					"phase", existing.Status.Phase)
				return nil // Success
			}
			if apierrors.IsNotFound(err) {
				// Workflow doesn't exist, create new one
				createdWorkflowName = ""
			} else {
				// Transient error, retry without creating duplicates
				return fmt.Errorf("failed to get existing workflow: %w", err)
			}
		}

		// Step 2: Create new workflow
		newWorkflow := workflowOriginal.DeepCopy()
		// Reset fields for clean creation
		newWorkflow.Name = ""
		newWorkflow.Namespace = namespace
		newWorkflow.ResourceVersion = ""
		newWorkflow.UID = ""
		newWorkflow.Status = argowfv1.WorkflowStatus{}

		if err := client.Create(ctx, newWorkflow); err != nil {
			// Handle "already exists" error - this is rare with GenerateName (hash collision)
			// Since we can't get the actual workflow name with GenerateName, treat as retryable
			// The workflow exists somewhere, but we can't track it without the name
			if apierrors.IsAlreadyExists(err) {
				logger.Info("Workflow already exists (rare GenerateName collision), retrying with new suffix",
					"generateName", newWorkflow.GenerateName)
				// Return retryable error - next attempt will generate a different suffix
				return fmt.Errorf("workflow name collision (already exists): %w", err)
			}
			// Other creation errors are not retryable
			logger.Error(err, "Failed to create workflow")
			return backoff.Permanent(fmt.Errorf("failed to create workflow: %w", err))
		}

		// After Create(), Name is populated by API server (from GenerateName)
		createdWorkflowName = newWorkflow.Name
		logger.Info("Workflow created", "name", createdWorkflowName)

		// Step 3: Check if workflow failed due to template not found
		createdWf := &argowfv1.Workflow{}
		if err := client.Get(ctx, createdWorkflowName, namespace, createdWf); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("workflow not found after create")
			}
			return fmt.Errorf("failed to get workflow status: %w", err)
		}

		if createdWf.Status.Phase == argowfv1.WorkflowError &&
			IsWorkflowTemplateNotFoundError(createdWf.Status.Message) {
			// Argo's informer cache hasn't synced yet, retry
			logger.Info("Workflow failed due to Argo informer cache lag, will retry",
				"workflow", createdWorkflowName,
				"phase", createdWf.Status.Phase,
				"message", createdWf.Status.Message)
			// Return retryable error - next iteration will check and delete this workflow
			return fmt.Errorf("workflow failed: %s", createdWf.Status.Message)
		}
		if createdWf.Status.Phase == argowfv1.WorkflowFailed {
			return backoff.Permanent(fmt.Errorf("workflow failed: %s", createdWf.Status.Message))
		}
		if createdWf.Status.Phase == "" && createdWf.Status.Message == "" {
			// Status not populated yet, retry to avoid racing with controller
			return fmt.Errorf("workflow status not ready yet")
		}

		// Workflow is in acceptable state
		logger.Info("Workflow created and verified",
			"workflow", createdWorkflowName,
			"phase", createdWf.Status.Phase)
		return nil
	}

	// Execute with backoff
	if err := backoff.Retry(operation, backoff.WithContext(backoffConfig, ctx)); err != nil {
		logger.Error(err, "Workflow creation failed after retries",
			"maxElapsedTime", WorkflowCreationBackoffMaxElapsedTime)
		return "", fmt.Errorf("workflow creation failed after retries: %w", err)
	}

	return createdWorkflowName, nil
}

// IsWorkflowTemplateNotFoundError checks if the error message indicates a WorkflowTemplate not found error.
// Matches Argo's error format: workflowtemplates.argoproj.io "name" not found
func IsWorkflowTemplateNotFoundError(message string) bool {
	return strings.Contains(message, "workflowtemplates.argoproj.io") &&
		strings.Contains(message, "not found")
}
