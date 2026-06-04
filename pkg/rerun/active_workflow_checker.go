package rerun

import (
	"context"
	"fmt"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HasActiveWorkflowInstance checks if there's any active workflow instance for the given LakeFlow
// This is the public interface method that delegates to the private implementation
func (m *workflowRerunManager) HasActiveWorkflowInstance(ctx context.Context, lw *v1alpha1.LakeFlow) (bool, string, error) {
	return m.hasActiveWorkflowInstance(ctx, lw)
}

// hasActiveWorkflowInstance checks if there's any active workflow instance
// Active means: Running or Pending phases (non-terminal states)
// Returns: (hasActive bool, activeWorkflowName string, error)
func (m *workflowRerunManager) hasActiveWorkflowInstance(ctx context.Context, lw *v1alpha1.LakeFlow) (bool, string, error) {
	// Check for active Workflow instances
	// workflowList includes ALL workflows with the label:
	// - Regular immediate workflows
	// - CronWorkflow child instances (they inherit the label)
	// - Retry workflows (labeled with is-retry=true)
	// - Resubmit workflows (labeled with is-resubmit=true)
	workflowList := &argowfv1.WorkflowList{}
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{
			v1alpha1.WorkflowNameLabel: lw.Name,
		},
	}

	if err := m.resourceManager.List(ctx, lw.Namespace, workflowList, listOpts...); err != nil {
		logger.Error(err, "Failed to list workflows for active instance check", "lakeflow", lw.Name)
		return false, "", fmt.Errorf("failed to list workflows: %w", err)
	}

	// Check each workflow for active states
	for i := range workflowList.Items {
		wf := &workflowList.Items[i]

		// Check if this workflow is in an active phase
		if isActivePhase(wf.Status.Phase) {
			// Identify the type of workflow for logging
			workflowType := "regular"
			if wf.Labels[v1alpha1.IsRetryWorkflowLabel] == "true" {
				workflowType = "retry"
			} else if wf.Labels[v1alpha1.IsResubmitWorkflowLabel] == "true" {
				workflowType = "resubmit"
			} else if wf.Labels[v1alpha1.ArgoCronWorkflowLabel] != "" {
				workflowType = "cron-child"
			}

			logger.Info("Found active workflow instance",
				"lakeflow", lw.Name,
				"workflow", wf.Name,
				"workflowType", workflowType,
				"phase", wf.Status.Phase)
			return true, wf.Name, nil
		}
	}

	// No active instances found
	logger.V(1).Info("No active workflow instances found",
		"lakeflow", lw.Name,
		"checkedWorkflows", len(workflowList.Items))
	return false, "", nil
}

// isActivePhase returns true if the workflow phase is considered "active"
// Active phases are those that represent a workflow currently executing or about to execute
func isActivePhase(phase argowfv1.WorkflowPhase) bool {
	return phase == argowfv1.WorkflowRunning || phase == argowfv1.WorkflowPending
}
