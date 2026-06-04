package status

import (
	"context"
	"fmt"
	"strings"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// setLakeFlowCondition is a custom helper function to set a condition on a LakeFlow.
// It mimics meta.SetStatusCondition but works with the custom LakeFlowCondition type.
func setLakeFlowCondition(conditions *[]v1alpha1.LakeFlowCondition, newCondition v1alpha1.LakeFlowCondition) {
	if conditions == nil {
		conditions = &[]v1alpha1.LakeFlowCondition{}
	}
	existingCondition := findLakeFlowCondition(*conditions, newCondition.Type)
	if existingCondition == nil {
		*conditions = append(*conditions, newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status || existingCondition.Reason != newCondition.Reason || existingCondition.Message != newCondition.Message {
		existingCondition.Status = newCondition.Status
		existingCondition.Reason = newCondition.Reason
		existingCondition.Message = newCondition.Message
		existingCondition.LastTransitionTime = newCondition.LastTransitionTime
	}
}

// findLakeFlowCondition finds a condition in a slice of conditions by type.
func findLakeFlowCondition(conditions []v1alpha1.LakeFlowCondition, conditionType string) *v1alpha1.LakeFlowCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

// updateCondition is a helper function to update a condition in the LakeFlow
func updateCondition(lw *v1alpha1.LakeFlow, conditionType string, status corev1.ConditionStatus, reason, message string) {
	condition := v1alpha1.LakeFlowCondition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
	setLakeFlowCondition(&lw.Status.Conditions, condition)
}

// mapArgoPhaseToLakeFlowPhase maps Argo Workflow phases to LakeFlow phases
func (h *reconcileStateHandler) mapArgoPhaseToLakeFlowPhase(argoPhase argowfv1.WorkflowPhase) v1alpha1.WorkflowPhase {
	switch argoPhase {
	case "":
		// Workflow exists but hasn't started yet
		return v1alpha1.WorkflowPhasePending
	case argowfv1.WorkflowPending:
		// Workflow submitted but waiting to start
		return v1alpha1.WorkflowPhasePending
	case argowfv1.WorkflowRunning:
		// Workflow actively executing
		return v1alpha1.WorkflowPhaseRunning
	case argowfv1.WorkflowSucceeded:
		// Workflow execution completed successfully
		return v1alpha1.WorkflowPhaseSucceeded
	case argowfv1.WorkflowFailed, argowfv1.WorkflowError:
		// Workflow execution failed
		return v1alpha1.WorkflowPhaseFailed
	default:
		// Unknown state - assume running
		return v1alpha1.WorkflowPhaseRunning
	}
}

// mapCronWorkflowPhaseToLakeFlowPhase maps Argo CronWorkflow status to LakeFlow phases
// Argo CronWorkflow only has two phases: "Active" and "Stopped"
func (h *reconcileStateHandler) mapCronWorkflowPhaseToLakeFlowPhase(cronWorkflow *argowfv1.CronWorkflow) v1alpha1.WorkflowPhase {
	switch cronWorkflow.Status.Phase {
	case "":
		// CronWorkflow exists but no status yet
		return v1alpha1.WorkflowPhasePending
	case "Active":
		// CronWorkflow is active - check execution state
		if len(cronWorkflow.Status.Active) > 0 {
			// Has actively running child workflows
			return v1alpha1.WorkflowPhaseRunning
		}
		// No active child workflows - workflow is idle
		// This covers: waiting for first schedule, between executions, or suspended
		return v1alpha1.WorkflowPhaseIdle
	case "Stopped":
		// CronWorkflow has been stopped - scheduling lifecycle finished
		// This happens when stop strategy is reached or manual stop
		return v1alpha1.WorkflowPhaseCompleted
	default:
		// Unknown/unexpected phase - fallback based on execution history
		h.logger.Info("Unknown CronWorkflow phase, using fallback logic",
			"cronworkflow", cronWorkflow.Name, "phase", cronWorkflow.Status.Phase)
		if len(cronWorkflow.Status.Active) > 0 {
			return v1alpha1.WorkflowPhaseRunning
		}
		return v1alpha1.WorkflowPhaseIdle
	}
}

// updateStatusReferences updates the status with references to created/updated resources
// It skips updating for resources with invalid names (empty or "<unnamed>") which indicate
// incomplete resource creation or resources that already exist.
//
// Note: Retry/resubmit workflow detection is handled by:
// - findMostRecentWorkflow detects retry/resubmit workflows via labels (IsRetryWorkflowLabel, IsResubmitWorkflowLabel)
// - syncManualInterventionStatus preserves workflowName/workflowTemplate for manual interventions
// Retry/resubmit workflows are created through createRerunWorkflow (not processResource),
// so their names should NOT appear in resourceNames during normal reconciliation.
func (h *reconcileStateHandler) updateStatusReferences(lw *v1alpha1.LakeFlow, resourceNames map[string]string) {
	// For now, we'll store the main workflow in the existing WorkflowName field
	lw.Status.LakeFlowName = lw.Name

	// Keep status references aligned with the desired trigger intent before applying resource updates.
	// This prevents stale references (e.g., old CronWorkflow after schedule -> dependency migration).
	if lw.Spec.WorkflowTrigger.Schedule.Cron != "" {
		lw.Status.WorkflowName = ""
	} else {
		lw.Status.CronWorkflow = ""
		// Dependency-triggered workflows do not have an operator-managed stable Workflow reference.
		// Clear stale WorkflowName when switching from immediate/schedule -> dependency.
		if len(lw.Spec.WorkflowTrigger.Depend.Workflows) > 0 {
			lw.Status.WorkflowName = ""
		}
	}

	for typeName, resourceName := range resourceNames {
		// Skip if resource name is invalid (empty or <unnamed>)
		// This can happen when a resource uses GenerateName and already exists,
		// so the new object's Name field is never populated by the API server
		if resourceName == "" || resourceName == "<unnamed>" {
			h.logger.Info("Skipping status update for resource with invalid name",
				"resourceType", typeName,
				"resourceName", resourceName,
				"lakeflow", lw.Name,
				"reason", "Resource name is empty or not assigned - resource may already exist")
			continue
		}

		if strings.EqualFold(typeName, "Workflow") {
			lw.Status.WorkflowName = resourceName
			lw.Status.CronWorkflow = ""
			// Initialize phase for immediate workflow if not set
			if lw.Status.Phase == "" {
				lw.Status.Phase = v1alpha1.WorkflowPhasePending
				h.logger.Info("Initializing immediate workflow phase to Pending", "lakeflow", lw.Name)
			}
		} else if strings.EqualFold(typeName, "WorkflowTemplate") {
			lw.Status.WorkflowTemplate = resourceName
		} else if strings.EqualFold(typeName, "CronWorkflow") {
			lw.Status.CronWorkflow = resourceName
			lw.Status.WorkflowName = ""
			// Initialize phase for CronWorkflow if not set
			if lw.Status.Phase == "" {
				lw.Status.Phase = v1alpha1.WorkflowPhaseIdle
				h.logger.Info("Initializing CronWorkflow phase to Idle", "lakeflow", lw.Name)
			}
		}
	}
}

func isScheduledTrigger(lw *v1alpha1.LakeFlow) bool {
	return lw.Spec.WorkflowTrigger.Schedule.Cron != ""
}

// updateFinishedTime updates LakeFlow finished time based on the LakeFlow trigger type.
// - For schedule-triggered LakeFlows: updates status.finishedTime.lastFinishedTime.
// - For non-schedule LakeFlows (immediate/dependency/manual): updates status.finishedTime.finishedAt.
func (h *reconcileStateHandler) updateFinishedTime(ctx context.Context, lw *v1alpha1.LakeFlow,
	argoWorkflow *argowfv1.Workflow,
	cronWorkflow *argowfv1.CronWorkflow) error {
	if lw == nil {
		return nil
	}

	if lw.Status.FinishedTime == nil {
		lw.Status.FinishedTime = &v1alpha1.LakeFlowFinishedTime{}
	}

	if isScheduledTrigger(lw) {
		var latestFinishedAt *metav1.Time

		// Prefer CronWorkflow as source if provided; fall back to the workflow FinishedAt when
		// we are syncing from a scheduled child workflow instance.
		if cronWorkflow != nil {
			t, err := h.getLatestChildWorkflowFinishedTime(ctx, cronWorkflow, lw.Namespace)
			if err != nil {
				return fmt.Errorf("failed to get latest child workflow finished time: %w", err)
			}
			latestFinishedAt = t
		} else if argoWorkflow != nil && !argoWorkflow.Status.FinishedAt.IsZero() {
			latestFinishedAt = &argoWorkflow.Status.FinishedAt
		}

		if latestFinishedAt == nil || latestFinishedAt.IsZero() {
			// No completed child workflows yet
			return nil
		}

		// Only update if this is newer than what we have
		if lw.Status.FinishedTime.LastFinishedTime.IsZero() || latestFinishedAt.After(lw.Status.FinishedTime.LastFinishedTime.Time) {
			lw.Status.FinishedTime.LastFinishedTime = *latestFinishedAt

			h.logger.Info("Updated cronworkflow last finished time",
				"lakeflow", lw.Name,
				"lastFinishedTime", lw.Status.FinishedTime.LastFinishedTime)
		}

		return nil
	}

	if argoWorkflow == nil || argoWorkflow.Status.FinishedAt.IsZero() {
		return nil
	}

	// Copy from metav1.Time to metav1.Time (they are compatible)
	lw.Status.FinishedTime.FinishedAt = argoWorkflow.Status.FinishedAt

	h.logger.Info("Updated workflow finished time",
		"lakeflow", lw.Name,
		"finishedAt", lw.Status.FinishedTime.FinishedAt)

	return nil
}

// getLatestChildWorkflowFinishedTime retrieves the most recent completion time from child workflows
func (h *reconcileStateHandler) getLatestChildWorkflowFinishedTime(ctx context.Context, cronWorkflow *argowfv1.CronWorkflow, namespace string) (*metav1.Time, error) {
	// List workflows that are owned by this cronworkflow
	workflowList := &argowfv1.WorkflowList{}

	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{v1alpha1.ArgoCronWorkflowLabel: cronWorkflow.Name},
	}

	if err := h.resourceManager.List(ctx, namespace, workflowList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list child workflows for cronworkflow %s: %w", cronWorkflow.Name, err)
	}

	var latestFinishedAt *metav1.Time

	for _, workflow := range workflowList.Items {
		// Only consider completed workflows (succeeded, failed, or error)
		if workflow.Status.Phase == argowfv1.WorkflowSucceeded ||
			workflow.Status.Phase == argowfv1.WorkflowFailed ||
			workflow.Status.Phase == argowfv1.WorkflowError {

			if !workflow.Status.FinishedAt.IsZero() {
				if latestFinishedAt == nil || workflow.Status.FinishedAt.After(latestFinishedAt.Time) {
					latestFinishedAt = &workflow.Status.FinishedAt
				}
			}
		}
	}

	return latestFinishedAt, nil
}
