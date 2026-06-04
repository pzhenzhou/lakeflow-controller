package status

import (
	"context"
	"fmt"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"github.com/pzhenzhou/lakeflow-controller/pkg/metrics"
	"github.com/pzhenzhou/lakeflow-controller/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileStateHandler implements workflow status sync from Argo Workflows/CronWorkflows
// to LakeFlow status fields (phase, finishedTime, etc.)
type reconcileStateHandler struct {
	resourceManager argoclient.ArgoResourceClient
	logger          logr.Logger
}

// SyncStatus intelligently chooses the most relevant workflow instance to sync from.
// Priority: most recent workflow by creation time (manual or normal).
// For manual interventions (resubmit/retry): only sync phase.
// For normal workflows (scheduled/dependency-triggered): sync phase + references.
func (h *reconcileStateHandler) SyncStatus(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	// Step 1: Find the most recent workflow instance (any type)
	mostRecentWf, workflowType, found := h.findMostRecentWorkflow(ctx, lw)
	if found {
		// CRITICAL: Temporal validation to prevent status regression
		// Don't sync if workflow is older than current LakeFlow status
		// This prevents syncing stale failed workflows after TTL cleanup of successful retries
		if h.isWorkflowStale(lw, mostRecentWf) {
			h.logger.Info("Skipping sync from stale workflow to preserve current status",
				"lakeflow", lw.Name,
				"staleWorkflow", mostRecentWf.Name,
				"staleWorkflowPhase", mostRecentWf.Status.Phase,
				"currentPhase", lw.Status.Phase)
			return nil
		}

		// Determine if this is a manual intervention or normal workflow
		isManual := workflowType == "resubmit" || workflowType == "retry"

		if isManual {
			// For manual interventions: sync phase only
			h.logger.Info("Syncing from manual intervention (most recent)",
				"lakeflow", lw.Name,
				"workflowName", mostRecentWf.Name,
				"workflowType", workflowType)
			return h.syncManualInterventionStatus(ctx, lw, mostRecentWf)
		}

		// For normal workflows: sync phase + references
		h.logger.Info("Syncing from normal workflow (most recent)",
			"lakeflow", lw.Name,
			"workflowName", mostRecentWf.Name,
			"workflowType", workflowType)
		return h.syncNormalWorkflowStatus(ctx, lw, mostRecentWf, workflowType)
	}

	// Step 2: No workflow instances found - sync from status references or initialize
	h.logger.Info("No workflow instances found, falling back to status references",
		"lakeflow", lw.Name)
	return h.syncFromStatusReferences(ctx, lw)
}

// isWorkflowStale checks if a workflow's finish time is older than the LakeFlow's current status
// Returns true if the workflow should NOT be synced because it represents stale state
// This prevents status regression when TTL cleanup removes newer successful workflows
func (h *reconcileStateHandler) isWorkflowStale(lw *v1alpha1.LakeFlow, wf *argowfv1.Workflow) bool {
	// If LakeFlow has no finish time recorded, workflow is not stale (first sync)
	if lw.Status.FinishedTime == nil {
		return false
	}

	// Get the workflow's finish time
	wfFinishedAt := wf.Status.FinishedAt

	// If workflow is still running, it's not stale (represents current execution)
	if wfFinishedAt.IsZero() {
		h.logger.V(1).Info("Workflow is still running, not stale",
			"lakeflow", lw.Name,
			"workflow", wf.Name,
			"phase", wf.Status.Phase)
		return false
	}

	// Get LakeFlow's last known finish time
	// Take the MAXIMUM of both times since a scheduled workflow can also be manually triggered
	var lwFinishedAt time.Time
	if !lw.Status.FinishedTime.LastFinishedTime.IsZero() {
		lwFinishedAt = lw.Status.FinishedTime.LastFinishedTime.Time
	}
	if !lw.Status.FinishedTime.FinishedAt.IsZero() {
		if lwFinishedAt.IsZero() || lw.Status.FinishedTime.FinishedAt.After(lwFinishedAt) {
			lwFinishedAt = lw.Status.FinishedTime.FinishedAt.Time
		}
	}

	// If LakeFlow has no finish time, workflow is not stale
	if lwFinishedAt.IsZero() {
		return false
	}

	// CRITICAL CHECK: Is workflow finish time BEFORE LakeFlow's finish time?
	// If yes, this workflow is stale (older execution that should not overwrite current status)
	if wfFinishedAt.Time.Before(lwFinishedAt) {
		h.logger.Info("Workflow is stale - will not sync to avoid status regression",
			"lakeflow", lw.Name,
			"workflow", wf.Name,
			"workflowFinishedAt", wfFinishedAt.Time,
			"lakeflowFinishedAt", lwFinishedAt,
			"staleDuration", lwFinishedAt.Sub(wfFinishedAt.Time).String(),
			"workflowPhase", wf.Status.Phase,
			"currentLakeworkflowPhase", lw.Status.Phase)
		return true
	}

	// Workflow is newer or same time - safe to sync
	return false
}

// findMostRecentWorkflow finds the most recently created workflow across all types
// Returns: (workflow, type, found) where type is "scheduled", "dependency", "resubmit", or "retry"
func (h *reconcileStateHandler) findMostRecentWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) (*argowfv1.Workflow, string, bool) {
	// List all workflows with the LakeFlow label
	workflowList := &argowfv1.WorkflowList{}
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{v1alpha1.WorkflowNameLabel: lw.Name},
	}

	if err := h.resourceManager.List(ctx, lw.Namespace, workflowList, listOpts...); err != nil {
		h.logger.Error(err, "Failed to list workflows", "lakeflow", lw.Name)
		return nil, "", false
	}

	if len(workflowList.Items) == 0 {
		return nil, "", false
	}

	// Use common utility to find most relevant workflow (running > recently finished > recently created)
	mostRecent := common.FindMostRelevantWorkflow(workflowList.Items)
	if mostRecent == nil {
		return nil, "", false
	}

	// Determine the type
	var workflowType string
	if mostRecent.Labels[v1alpha1.IsResubmitWorkflowLabel] == "true" {
		workflowType = "resubmit"
	} else if mostRecent.Labels[v1alpha1.IsRetryWorkflowLabel] == "true" {
		workflowType = "retry"
	} else if mostRecent.Labels[v1alpha1.ArgoCronWorkflowLabel] != "" {
		workflowType = "scheduled"
	} else {
		workflowType = "dependency"
	}

	h.logger.Info("Found most recent workflow",
		"lakeflow", lw.Name,
		"workflowName", mostRecent.Name,
		"workflowType", workflowType,
		"phase", mostRecent.Status.Phase,
		"creationTime", mostRecent.CreationTimestamp,
		"finishedAt", mostRecent.Status.FinishedAt,
		"selectionReason", common.GetWorkflowSelectionReason(mostRecent))

	return mostRecent, workflowType, true
}

// observeWorkflowAndTasks records the workflow-level and task-level metrics for a single
// concrete Workflow instance. eventTime is the workflow's FinishedAt (zero for running
// workflows, in which case the metrics manager falls back to time.Now()). isResubmit/isRetry
// override the trigger type; pass false/false for normal workflows (ObserveWorkflowState's
// behavior). The CronWorkflow metrics path is intentionally NOT routed through here: it uses
// LastFinishedTime and aggregates task metrics across all child workflows.
func (h *reconcileStateHandler) observeWorkflowAndTasks(lw *v1alpha1.LakeFlow, wf *argowfv1.Workflow, isResubmit, isRetry bool) {
	metrics.GetMetricsManager().ObserveWorkflowStateWithTrigger(lw, wf.Status.FinishedAt.Time, isResubmit, isRetry)
	metrics.GetMetricsManager().ObserveTaskExecution(lw, wf)
}

// syncManualInterventionStatus syncs ONLY the phase from a manual intervention workflow.
// It does NOT update workflowName or cronWorkflow references to preserve normal workflow info.
func (h *reconcileStateHandler) syncManualInterventionStatus(ctx context.Context, lw *v1alpha1.LakeFlow, wf *argowfv1.Workflow) error {
	// Map Argo phase to LakeFlow phase
	newPhase := h.mapArgoPhaseToLakeFlowPhase(wf.Status.Phase)

	// Update phase if changed
	if lw.Status.Phase != newPhase {
		lw.Status.Phase = newPhase
		h.logger.Info("Updated phase from manual intervention",
			"lakeflow", lw.Name,
			"workflow", wf.Name,
			"newPhase", newPhase)
	}
	// Update finished time (trigger-aware: scheduled -> LastFinishedTime, otherwise -> FinishedAt)
	_ = h.updateFinishedTime(ctx, lw, wf, nil)
	// Determine if this is a resubmit or retry workflow
	isResubmit := wf.Labels[v1alpha1.IsResubmitWorkflowLabel] == "true"
	isRetry := wf.Labels[v1alpha1.IsRetryWorkflowLabel] == "true"

	// Collect workflow-level and task-level metrics for this manual-intervention workflow.
	h.observeWorkflowAndTasks(lw, wf, isResubmit, isRetry)

	// Note: Intentionally NOT updating status.workflowName or status.cronWorkflow
	// These should remain as the last scheduled/normal execution
	h.logger.Info("Manual intervention phase synced (references preserved)",
		"lakeflow", lw.Name,
		"workflow", wf.Name,
		"phase", newPhase)

	return nil
}

// syncNormalWorkflowStatus syncs both phase AND references from a normal workflow.
// This is used for scheduled and dependency-triggered workflows.
func (h *reconcileStateHandler) syncNormalWorkflowStatus(ctx context.Context, lw *v1alpha1.LakeFlow, wf *argowfv1.Workflow, workflowType string) error {
	// Map Argo phase to LakeFlow phase
	newPhase := h.mapArgoPhaseToLakeFlowPhase(wf.Status.Phase)

	// Update phase if changed
	if lw.Status.Phase != newPhase {
		lw.Status.Phase = newPhase
		h.logger.Info("Updated phase from normal workflow",
			"lakeflow", lw.Name,
			"workflow", wf.Name,
			"workflowType", workflowType,
			"newPhase", newPhase)
	}

	// Update references based on workflow type
	if workflowType == "scheduled" {
		// For scheduled workflows, update cronWorkflow reference
		cronWorkflowName := wf.Labels[v1alpha1.ArgoCronWorkflowLabel]
		if cronWorkflowName != "" && lw.Status.CronWorkflow != cronWorkflowName {
			lw.Status.CronWorkflow = cronWorkflowName
			lw.Status.WorkflowName = "" // Clear direct workflow reference for CronWorkflow
			h.logger.Info("Updated cronWorkflow reference",
				"lakeflow", lw.Name,
				"cronWorkflow", cronWorkflowName)
		}
	} else if workflowType == "dependency" {
		// For dependency-triggered workflows, update workflowName reference
		if lw.Status.WorkflowName != wf.Name {
			lw.Status.WorkflowName = wf.Name
			lw.Status.CronWorkflow = "" // Clear CronWorkflow reference for direct workflow
			h.logger.Info("Updated workflowName reference",
				"lakeflow", lw.Name,
				"workflowName", wf.Name)
		}
		// Note: Dependency triggering and state cleanup is handled by Lake Watcher (external service)
	}

	// Update finished time (trigger-aware: scheduled -> LastFinishedTime, otherwise -> FinishedAt)
	_ = h.updateFinishedTime(ctx, lw, wf, nil)

	// Sync executionState from CronWorkflow if applicable
	if lw.Status.CronWorkflow != "" {
		if err := h.syncExecutionStateFromCronWorkflow(ctx, lw); err != nil {
			h.logger.Error(err, "Failed to sync executionState from CronWorkflow")
			// Don't fail the whole sync, just log
		}
	}

	// Collect workflow-level and task-level metrics. Normal workflows auto-detect trigger
	// type from spec, so no manual-intervention override (false/false).
	h.observeWorkflowAndTasks(lw, wf, false, false)

	h.logger.Info("Normal workflow phase and references synced",
		"lakeflow", lw.Name,
		"workflow", wf.Name,
		"phase", newPhase,
		"workflowType", workflowType)

	return nil
}

// syncFromStatusReferences falls back to syncing from existing status references
// when no workflow instances are found (e.g., initial state or all cleaned up by TTL)
func (h *reconcileStateHandler) syncFromStatusReferences(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	// Try CronWorkflow first
	if lw.Status.CronWorkflow != "" {
		h.logger.Info("Syncing from CronWorkflow reference",
			"lakeflow", lw.Name,
			"cronWorkflow", lw.Status.CronWorkflow)
		return h.syncCronWorkflowStatus(ctx, lw)
	}

	// Try direct Workflow
	if lw.Status.WorkflowName != "" {
		h.logger.Info("Syncing from Workflow reference",
			"lakeflow", lw.Name,
			"workflow", lw.Status.WorkflowName)
		return h.syncWorkflowStatus(ctx, lw)
	}

	// No references - this is likely a new LakeFlow
	h.logger.Info("No status references found - workflow may be newly created",
		"lakeflow", lw.Name)
	return nil
}

// syncExecutionStateFromCronWorkflow syncs the executionState based on CronWorkflow suspend status
func (h *reconcileStateHandler) syncExecutionStateFromCronWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow == "" {
		return nil // Not applicable
	}

	cronWorkflow := &argowfv1.CronWorkflow{}
	err := h.resourceManager.Get(ctx, lw.Status.CronWorkflow, lw.Namespace, cronWorkflow)
	if err != nil {
		if errors.IsNotFound(err) {
			h.logger.Info("CronWorkflow not found, cannot sync executionState",
				"cronWorkflow", lw.Status.CronWorkflow)
			return nil
		}
		return fmt.Errorf("failed to get CronWorkflow %s: %w", lw.Status.CronWorkflow, err)
	}

	// Determine executionState from CronWorkflow suspend status
	h.syncExecutionStateFromCronWorkflowObject(lw, cronWorkflow)
	return nil
}

// syncExecutionStateFromCronWorkflowObject derives status.executionState (Suspend/Active)
// from an already-fetched CronWorkflow's suspend flag. It is pure (no API access) so callers
// that already hold the CronWorkflow object (e.g. syncCronWorkflowStatus) reuse it without a
// second Get.
func (h *reconcileStateHandler) syncExecutionStateFromCronWorkflowObject(lw *v1alpha1.LakeFlow, cronWorkflow *argowfv1.CronWorkflow) {
	if cronWorkflow.Spec.Suspend {
		if lw.Status.ExecutionState != v1alpha1.ControlStateSuspend {
			lw.Status.ExecutionState = v1alpha1.ControlStateSuspend
			h.logger.Info("Updated executionState to Suspend from CronWorkflow",
				"lakeflow", lw.Name,
				"cronWorkflow", cronWorkflow.Name)
		}
		return
	}
	if lw.Status.ExecutionState != v1alpha1.ControlStateActive {
		lw.Status.ExecutionState = v1alpha1.ControlStateActive
		h.logger.Info("Updated executionState to Active from CronWorkflow",
			"lakeflow", lw.Name,
			"cronWorkflow", cronWorkflow.Name)
	}
}

// discoverAndSyncWorkflowByLabel discovers workflows by LakeFlow label for dependency-triggered workflows
func (h *reconcileStateHandler) discoverAndSyncWorkflowByLabel(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	workflowList := &argowfv1.WorkflowList{}

	// List workflows with the LakeFlow label
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{v1alpha1.WorkflowNameLabel: lw.Name},
	}

	if err := h.resourceManager.List(ctx, lw.Namespace, workflowList, listOpts...); err != nil {
		return fmt.Errorf("failed to list workflows with label %s=%s: %w",
			v1alpha1.WorkflowNameLabel, lw.Name, err)
	}

	if len(workflowList.Items) == 0 {
		h.logger.V(1).Info("No workflows found for dependency-triggered LakeFlow yet",
			"lakeflow", lw.Name)
		return nil
	}

	// Use common utility to find most relevant workflow (running > recently finished > recently created)
	latestWorkflow := common.FindMostRelevantWorkflow(workflowList.Items)
	if latestWorkflow == nil {
		return nil
	}

	// CRITICAL: Temporal validation to prevent status regression
	if h.isWorkflowStale(lw, latestWorkflow) {
		h.logger.Info("Skipping discovery sync from stale workflow",
			"lakeflow", lw.Name,
			"staleWorkflow", latestWorkflow.Name,
			"staleWorkflowPhase", latestWorkflow.Status.Phase,
			"currentPhase", lw.Status.Phase)
		return nil
	}

	h.logger.Info("Discovered dependency-triggered workflow",
		"lakeflow", lw.Name,
		"workflow", latestWorkflow.Name,
		"phase", latestWorkflow.Status.Phase,
		"creationTimestamp", latestWorkflow.CreationTimestamp,
		"finishedAt", latestWorkflow.Status.FinishedAt,
		"selectionReason", common.GetWorkflowSelectionReason(latestWorkflow))

	// Sync from this workflow
	newPhase := h.mapArgoPhaseToLakeFlowPhase(latestWorkflow.Status.Phase)
	if lw.Status.Phase != newPhase {
		lw.Status.Phase = newPhase
		h.logger.Info("Updated phase from discovered workflow",
			"lakeflow", lw.Name,
			"workflow", latestWorkflow.Name,
			"newPhase", newPhase)
	}

	// Update finished time if available (trigger-aware: scheduled -> LastFinishedTime, otherwise -> FinishedAt)
	if updateErr := h.updateFinishedTime(ctx, lw, latestWorkflow, nil); updateErr != nil {
		h.logger.Error(updateErr, "failed to update finished time", "lakeflow", lw.Name)
	}

	// Check if this is a resubmit/retry workflow (manual intervention)
	isResubmit := latestWorkflow.Labels[v1alpha1.IsResubmitWorkflowLabel] == "true"
	isRetry := latestWorkflow.Labels[v1alpha1.IsRetryWorkflowLabel] == "true"

	// Only update workflowName if NOT a manual intervention
	if !isResubmit && !isRetry {
		if lw.Status.WorkflowName != latestWorkflow.Name {
			lw.Status.WorkflowName = latestWorkflow.Name
			h.logger.Info("Updated workflowName from discovered workflow",
				"lakeflow", lw.Name,
				"workflow", latestWorkflow.Name)
		}
	} else {
		h.logger.Info("Skipping workflowName update for manual intervention workflow",
			"lakeflow", lw.Name,
			"workflow", latestWorkflow.Name,
			"isResubmit", isResubmit,
			"isRetry", isRetry)
	}

	// Collect workflow-level and task-level metrics for the discovered workflow.
	h.observeWorkflowAndTasks(lw, latestWorkflow, isResubmit, isRetry)

	return nil
}

// syncWorkflowStatus syncs status from a direct Workflow (immediate/dependency-triggered)
func (h *reconcileStateHandler) syncWorkflowStatus(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.WorkflowName == "" {
		return fmt.Errorf("workflowName not set in status")
	}

	workflow := &argowfv1.Workflow{}
	err := h.resourceManager.Get(ctx, lw.Status.WorkflowName, lw.Namespace, workflow)
	if err != nil {
		if errors.IsNotFound(err) {
			// Workflow might have been cleaned up by TTL - try discovering by label
			h.logger.Info("Workflow not found, attempting discovery by label",
				"workflow", lw.Status.WorkflowName)
			return h.discoverAndSyncWorkflowByLabel(ctx, lw)
		}
		return fmt.Errorf("failed to get workflow %s: %w", lw.Status.WorkflowName, err)
	}

	// Map Argo phase to LakeFlow phase
	newPhase := h.mapArgoPhaseToLakeFlowPhase(workflow.Status.Phase)

	if lw.Status.Phase != newPhase {
		lw.Status.Phase = newPhase
		updateCondition(lw, string(v1alpha1.ConditionTypeArgoResourceCreated), corev1.ConditionTrue,
			ReasonPhaseUpdated,
			fmt.Sprintf("Phase updated to %s from workflow %s", newPhase, workflow.Name))
	}

	// Update finished time if available (trigger-aware: scheduled -> LastFinishedTime, otherwise -> FinishedAt)
	_ = h.updateFinishedTime(ctx, lw, workflow, nil)

	// Collect workflow-level and task-level metrics. Direct workflows here are not manual
	// interventions, so no trigger override (false/false).
	h.observeWorkflowAndTasks(lw, workflow, false, false)

	return nil
}

// syncCronWorkflowStatus syncs status from a CronWorkflow (scheduled)
func (h *reconcileStateHandler) syncCronWorkflowStatus(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow == "" {
		return fmt.Errorf("cronWorkflow not set in status")
	}

	cronWorkflow := &argowfv1.CronWorkflow{}
	err := h.resourceManager.Get(ctx, lw.Status.CronWorkflow, lw.Namespace, cronWorkflow)
	if err != nil {
		if errors.IsNotFound(err) {
			updateCondition(lw, string(v1alpha1.ConditionTypeArgoResourceCreated), corev1.ConditionFalse,
				ReasonWorkflowNotFound,
				fmt.Sprintf("CronWorkflow %s not found", lw.Status.CronWorkflow))
			return fmt.Errorf("cronWorkflow %s not found", lw.Status.CronWorkflow)
		}
		return fmt.Errorf("failed to get cronWorkflow %s: %w", lw.Status.CronWorkflow, err)
	}

	// Sync execution state (active/suspended) from the already-fetched CronWorkflow object,
	// avoiding a second API Get.
	h.syncExecutionStateFromCronWorkflowObject(lw, cronWorkflow)

	// Map CronWorkflow phase to LakeFlow phase
	newPhase := h.mapCronWorkflowPhaseToLakeFlowPhase(cronWorkflow)

	if lw.Status.Phase != newPhase {
		lw.Status.Phase = newPhase
		updateCondition(lw, string(v1alpha1.ConditionTypeArgoResourceCreated), corev1.ConditionTrue,
			ReasonPhaseUpdated,
			fmt.Sprintf("Phase updated to %s from CronWorkflow %s", newPhase, cronWorkflow.Name))
	}

	// Update finished time from child workflows (trigger-aware: scheduled -> LastFinishedTime)
	if err := h.updateFinishedTime(ctx, lw, nil, cronWorkflow); err != nil {
		h.logger.Error(err, "Failed to update finished time for CronWorkflow", "cronworkflow", cronWorkflow.Name)
	}

	// Collect workflow-level metrics (transition timestamp and duration)
	// Note: For CronWorkflow, we use LastFinishedTime if available, otherwise the metrics
	// manager will use time.Now() for running/idle states. This preserves historical accuracy
	// even if observation is delayed (e.g., controller restart, reconciliation lag).
	var cronEventTime time.Time
	if lw.Status.FinishedTime != nil && !lw.Status.FinishedTime.LastFinishedTime.IsZero() {
		cronEventTime = lw.Status.FinishedTime.LastFinishedTime.Time
	}
	metrics.GetMetricsManager().ObserveWorkflowState(lw, cronEventTime)

	// Collect task-level metrics from ALL child workflows (active and recently completed).
	// This ensures we don't miss final task metrics for workflows that completed between reconciliations.
	// The ObserveTaskExecution function internally filters out tasks older than TaskMetricsRetentionPeriod.
	childWorkflowList := &argowfv1.WorkflowList{}
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{v1alpha1.ArgoCronWorkflowLabel: cronWorkflow.Name},
	}
	if listErr := h.resourceManager.List(ctx, lw.Namespace, childWorkflowList, listOpts...); listErr != nil {
		h.logger.Error(listErr, "Failed to list child workflows for metrics collection", "cronworkflow", cronWorkflow.Name)
	} else {
		for i := range childWorkflowList.Items {
			metrics.GetMetricsManager().ObserveTaskExecution(lw, &childWorkflowList.Items[i])
		}
	}

	return nil
}

// UpdateReconciliationStatus updates the status based on reconciliation results
func (h *reconcileStateHandler) UpdateReconciliationStatus(ctx context.Context, lw *v1alpha1.LakeFlow, result *reconciler.ReconcileResult) error {
	switch result.Action {
	case reconciler.ReconcileActionCreated:
		updateCondition(lw, string(v1alpha1.ConditionTypeArgoResourceCreated), corev1.ConditionTrue, ReasonResourcesCreated, result.Message)
		h.logger.Info("Resources created", "lakeflow", lw.Name, "resources", result.ResourceNames)

		// Update status references (workflowName, cronWorkflow, workflowTemplate)
		h.updateStatusReferences(lw, result.ResourceNames)

		// Update ObservedGeneration to indicate this generation has been successfully reconciled
		lw.Status.ObservedGeneration = lw.Generation

	case reconciler.ReconcileActionUpdated:
		updateCondition(lw, string(v1alpha1.ConditionTypeArgoResourceCreated), corev1.ConditionTrue, ReasonResourcesUpdated, result.Message)
		h.logger.Info("Resources updated", "lakeflow", lw.Name, "resources", result.ResourceNames)

		// Update status references
		h.updateStatusReferences(lw, result.ResourceNames)

		// Update ObservedGeneration
		lw.Status.ObservedGeneration = lw.Generation

	case reconciler.ReconcileActionNoChange:
		// Resources are up to date - no changes needed
		h.logger.Info("Resources are up to date", "lakeflow", lw.Name, "generation", lw.Generation, "observedGeneration", lw.Status.ObservedGeneration)

		// Even if no changes, ensure ObservedGeneration matches current Generation
		// This handles the case where the status was updated externally
		if lw.Status.ObservedGeneration != lw.Generation {
			h.logger.Info("Updating ObservedGeneration to match current Generation",
				"lakeflow", lw.Name,
				"currentGeneration", lw.Generation,
				"observedGeneration", lw.Status.ObservedGeneration)
			lw.Status.ObservedGeneration = lw.Generation
		}

		updateCondition(lw, string(v1alpha1.ConditionTypeArgoResourceCreated), corev1.ConditionTrue, ReasonResourcesUpToDate, "No changes detected, resources are up to date")

	case reconciler.ReconcileActionError:
		updateCondition(lw, string(v1alpha1.ConditionTypeArgoResourceCreated), corev1.ConditionFalse, ReasonReconciliationError, result.Message)
		h.logger.Error(result.Error, "Reconciliation failed", "lakeflow", lw.Name)
		// Don't update ObservedGeneration on error - this generation has not been successfully reconciled
		return fmt.Errorf("reconciliation failed: %w", result.Error)
	}
	return nil
}
