package status

import (
	"context"
	"fmt"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/sparkmanager"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// controlStateHandler implements lifecycle control state machine
// It manages transitions between states: Active, Suspend, Stopped, Terminated
type controlStateHandler struct {
	resourceManager         argoclient.ArgoResourceClient
	sparkApplicationManager *sparkmanager.SparkApplicationManager
	logger                  logr.Logger
}

func (h *controlStateHandler) Handle(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	// Logic from former workflow_control_state_handler.go's Handle method
	_, err := h.HandleStateTransition(ctx, lw)
	if err != nil {
		return err
	}

	// Stage 3 Task 8: Update WorkflowTemplate state label
	// When state changes, update the WorkflowTemplate's control state label
	// This ensures the label stays in sync with the actual workflow state
	if err := h.updateWorkflowTemplateStateLabel(ctx, lw); err != nil {
		h.logger.Error(err, "Failed to update WorkflowTemplate state label", "lakeflow", lw.Name)
		// Don't fail the entire state transition, just log
		// The label will be synced on the next reconciliation
	}

	return nil
}

// hasExecutableArgoResources checks if executable Argo resources exist that can be controlled.
// Returns true if Workflow or CronWorkflow exists in status.
// Note: WorkflowTemplate alone is not sufficient - it's just a template, not executable.
func (h *controlStateHandler) hasExecutableArgoResources(lw *v1alpha1.LakeFlow) bool {
	return lw.Status.WorkflowName != "" || lw.Status.CronWorkflow != ""
}

// finishedAtMessage renders the workflow finish time for "already finished" control-state
// conditions, falling back to "unknown time" when no finish time has been recorded.
func finishedAtMessage(lw *v1alpha1.LakeFlow) string {
	if lw.Status.FinishedTime != nil && !lw.Status.FinishedTime.FinishedAt.IsZero() {
		return lw.Status.FinishedTime.FinishedAt.Format(time.RFC3339)
	}
	return "unknown time"
}

func (h *controlStateHandler) HandleStateTransition(ctx context.Context, lw *v1alpha1.LakeFlow) (bool, error) {
	// Get current desired state from spec (default to Active if not set)
	desiredState := lw.Spec.State
	if desiredState == "" {
		desiredState = v1alpha1.ControlStateActive
	}

	// Get current execution phase from status
	currentPhase := lw.Status.Phase

	h.logger.Info("Handling state transition", "desiredState", desiredState, "currentPhase", currentPhase, "lakeflow", lw.Name)

	// Handle state transitions based on desired state
	switch desiredState {
	case v1alpha1.ControlStateActive:
		return true, h.handleActiveState(ctx, lw, currentPhase)
	case v1alpha1.ControlStateSuspend:
		return true, h.handleSuspendedState(ctx, lw, currentPhase)
	case v1alpha1.ControlStateStopped:
		return true, h.handleStoppedState(ctx, lw, currentPhase)
	case v1alpha1.ControlStateTerminated:
		return true, h.handleTerminatedState(ctx, lw, currentPhase)
	default:
		return false, fmt.Errorf("unsupported workflow control state: %s", desiredState)
	}
}

// handleActiveState ensures the workflow is actively running/scheduling
func (h *controlStateHandler) handleActiveState(ctx context.Context, lw *v1alpha1.LakeFlow, currentPhase v1alpha1.WorkflowPhase) error {
	// Check the execution state first - if not active, we need to resume/activate
	if lw.Status.ExecutionState == v1alpha1.ControlStateSuspend {
		h.logger.Info("Resuming suspended workflow to active state", "lakeflow", lw.Name)
		if err := h.resumeArgoWorkflow(ctx, lw); err != nil {
			if errors.IsNotFound(err) {
				h.logger.V(1).Info("Argo cronWorkflow/workflow not found",
					"lakeWorkflow", lw.Name,
					"namespace", lw.Namespace)
			} else {
				return err
			}
		}
		// Update execution state
		lw.Status.ExecutionState = v1alpha1.ControlStateActive
		updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
			"WorkflowResumed",
			fmt.Sprintf("Workflow resumed to active state. Current phase: %s", currentPhase))
		return nil
	}

	// Handle reactivation from Stopped/Terminated state for CronWorkflow
	// When a CronWorkflow is stopped/terminated, it gets suspended. Reactivating should unsuspend it.
	if lw.Status.ExecutionState == v1alpha1.ControlStateStopped || lw.Status.ExecutionState == v1alpha1.ControlStateTerminated {
		if lw.Status.CronWorkflow != "" {
			h.logger.Info("Reactivating CronWorkflow from stopped/terminated state",
				"lakeflow", lw.Name,
				"previousState", lw.Status.ExecutionState,
				"cronWorkflow", lw.Status.CronWorkflow)
			// Unsuspend the CronWorkflow so it can resume scheduling
			if err := h.resumeArgoCronWorkflow(ctx, lw); err != nil {
				return err
			}
			lw.Status.ExecutionState = v1alpha1.ControlStateActive
			updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
				"CronWorkflowReactivated",
				fmt.Sprintf("CronWorkflow reactivated from %s state. Scheduling resumed. Use %s or %s annotation for immediate execution.",
					lw.Status.ExecutionState, v1alpha1.RetryRequestAnnotation, v1alpha1.ResubmitRequestAnnotation))
			return nil
		}
		// For non-CronWorkflow (immediate/dependency-triggered), just acknowledge reactivation
		// The workflow instance is already finished, user needs to use retry/resubmit
		h.logger.Info("Reactivating workflow from stopped/terminated state",
			"lakeflow", lw.Name,
			"previousState", lw.Status.ExecutionState)
		// Fall through to the phase-based handling below
	}

	switch currentPhase {
	case v1alpha1.WorkflowPhaseIdle:
		// Idle and active - normal state for CronWorkflow between executions
		h.logger.Info("Workflow is idle and active", "lakeflow", lw.Name)
		// Ensure executionState is Active
		if lw.Status.ExecutionState != v1alpha1.ControlStateActive {
			lw.Status.ExecutionState = v1alpha1.ControlStateActive
			updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
				"WorkflowActive",
				"Workflow is active and idle, waiting for next scheduled execution")
		}
		return nil
	case v1alpha1.WorkflowPhaseRunning:
		// Already running and active, no action needed
		h.logger.Info("Workflow is already running", "lakeflow", lw.Name)
		// Ensure executionState is Active
		if lw.Status.ExecutionState != v1alpha1.ControlStateActive {
			lw.Status.ExecutionState = v1alpha1.ControlStateActive
			updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
				"WorkflowActive",
				"Workflow is actively running")
		}
		return nil
	case v1alpha1.WorkflowPhasePending:
		// Check if executable Argo resources have been created yet
		if !h.hasExecutableArgoResources(lw) {
			// No executable resources yet - this is expected for dependency-triggered workflows
			// waiting for dependencies. Lake Watcher will create the Workflow when ready.
			// Set ExecutionState to Active (coordination intent) without backend operations.
			h.logger.Info("Workflow pending without executable resources - acknowledging active intent",
				"lakeflow", lw.Name)
			lw.Status.ExecutionState = v1alpha1.ControlStateActive
			updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
				"WorkflowPendingDependencies",
				"Workflow is active but pending - waiting for dependencies to be satisfied")
			return nil
		}

		// Executable resources exist - apply resume operation if needed
		if err := h.resumeArgoWorkflow(ctx, lw); err != nil {
			return err
		}
		lw.Status.ExecutionState = v1alpha1.ControlStateActive
		updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
			"WorkflowActive",
			"Workflow activated from pending state")
		return nil
	case v1alpha1.WorkflowPhaseSucceeded, v1alpha1.WorkflowPhaseFailed, v1alpha1.WorkflowPhaseCompleted:
		// Workflow has finished - acknowledge active state
		h.logger.Info("Workflow has already finished, updating executionState to active",
			"lakeflow", lw.Name, "phase", currentPhase)

		// Update executionState to match spec.state for consistency
		lw.Status.ExecutionState = v1alpha1.ControlStateActive

		// Add a condition to clarify the workflow is finished but in active control state
		finishedAtMsg := finishedAtMessage(lw)
		updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
			"WorkflowFinishedButActive",
			fmt.Sprintf("Workflow finished with phase '%s' at %s. Control state is active. Use %s or %s annotation to re-run.",
				currentPhase, finishedAtMsg, v1alpha1.RetryRequestAnnotation, v1alpha1.ResubmitRequestAnnotation))

		return nil
	case "":
		// No phase set yet, assume pending (resources being coordinated)
		lw.Status.Phase = v1alpha1.WorkflowPhasePending
		lw.Status.ExecutionState = v1alpha1.ControlStateActive
		updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
			"WorkflowActive",
			"Workflow initialized in active state")
		return nil
	default:
		h.logger.Info("No action needed for active state", "lakeflow", lw.Name, "currentPhase", currentPhase)
		return nil
	}
}

// controlStatePlan describes the per-state inputs for the shutdown-style control transitions
// (Suspend/Stopped/Terminated), which share an identical phase-driven structure. The Active
// state is handled separately (handleActiveState) because it has no backend op on Idle/Running
// and carries resume/reactivate preludes.
type controlStatePlan struct {
	// target is the ExecutionState set on success across every branch.
	target v1alpha1.WorkflowControlState
	// backendOp applies the actual Argo mutation when executable resources exist.
	backendOp func(ctx context.Context, lw *v1alpha1.LakeFlow) error
	// applied{Reason,MsgFmt} cover Idle/Running and Pending-with-resources; MsgFmt takes the phase.
	appliedReason string
	appliedMsgFmt string
	// pendingAck{Reason,Msg} cover Pending without executable resources.
	pendingAckReason string
	pendingAckMsg    string
	// finished{Reason,Verb} build the "already finished" condition; Verb is "Suspend"/"Stop"/"Terminate".
	finishedReason string
	finishedVerb   string
	// initAck{Reason,Msg} cover the empty-phase initialization branch without executable resources.
	initAckReason string
	initAckMsg    string
	// initApplied{Reason,Msg} cover the empty-phase initialization branch with executable resources.
	initAppliedReason string
	initAppliedMsg    string
}

// applyControlState runs the shared phase switch for the shutdown-style control transitions.
// All Suspend/Stopped/Terminated behavior (including the empty-phase Phase=Pending initialization
// and the distinct "acknowledged" vs "applied" messaging) is encoded once here and parameterized
// by plan, so the three handlers stay byte-for-byte equivalent to their former hand-written forms.
func (h *controlStateHandler) applyControlState(ctx context.Context, lw *v1alpha1.LakeFlow, currentPhase v1alpha1.WorkflowPhase, plan controlStatePlan) error {
	applyBackend := func() error {
		if err := plan.backendOp(ctx, lw); err != nil {
			return err
		}
		lw.Status.ExecutionState = plan.target
		updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
			plan.appliedReason, fmt.Sprintf(plan.appliedMsgFmt, currentPhase))
		return nil
	}

	switch currentPhase {
	case v1alpha1.WorkflowPhaseIdle, v1alpha1.WorkflowPhaseRunning:
		// Workflow is ready or running - apply the backend operation directly.
		return applyBackend()
	case v1alpha1.WorkflowPhasePending:
		if !h.hasExecutableArgoResources(lw) {
			// No executable resources yet - acknowledge intent without backend operations.
			h.logger.Info("Workflow pending without executable resources - acknowledging control intent",
				"lakeflow", lw.Name, "targetState", plan.target)
			lw.Status.ExecutionState = plan.target
			updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
				plan.pendingAckReason, plan.pendingAckMsg)
			return nil
		}
		return applyBackend()
	case v1alpha1.WorkflowPhaseSucceeded, v1alpha1.WorkflowPhaseFailed, v1alpha1.WorkflowPhaseCompleted:
		// Workflow has finished - cannot physically act, but acknowledge user's intent.
		h.logger.Info("Workflow has already finished, updating executionState without backend action",
			"lakeflow", lw.Name, "phase", currentPhase)
		lw.Status.ExecutionState = plan.target
		updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
			plan.finishedReason,
			fmt.Sprintf("Workflow finished with phase '%s' at %s. %s state acknowledged but has no effect on completed workflow. Use %s or %s annotation to re-run.",
				currentPhase, finishedAtMessage(lw), plan.finishedVerb, v1alpha1.RetryRequestAnnotation, v1alpha1.ResubmitRequestAnnotation))
		return nil
	case "":
		// No phase set yet, assume pending.
		lw.Status.Phase = v1alpha1.WorkflowPhasePending
		if !h.hasExecutableArgoResources(lw) {
			h.logger.Info("Workflow initializing without executable resources - acknowledging control intent",
				"lakeflow", lw.Name, "targetState", plan.target)
			lw.Status.ExecutionState = plan.target
			updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
				plan.initAckReason, plan.initAckMsg)
			return nil
		}
		if err := plan.backendOp(ctx, lw); err != nil {
			return err
		}
		lw.Status.ExecutionState = plan.target
		updateCondition(lw, "ControlStateTransition", corev1.ConditionTrue,
			plan.initAppliedReason, plan.initAppliedMsg)
		return nil
	default:
		h.logger.Info("No action needed for control state", "lakeflow", lw.Name,
			"targetState", plan.target, "currentPhase", currentPhase)
		return nil
	}
}

// handleSuspendedState suspends the workflow if not already suspended
func (h *controlStateHandler) handleSuspendedState(ctx context.Context, lw *v1alpha1.LakeFlow, currentPhase v1alpha1.WorkflowPhase) error {
	// Check if already suspended
	if lw.Status.ExecutionState == v1alpha1.ControlStateSuspend {
		h.logger.Info("Workflow is already suspended", "lakeflow", lw.Name)
		return nil
	}

	return h.applyControlState(ctx, lw, currentPhase, controlStatePlan{
		target:            v1alpha1.ControlStateSuspend,
		backendOp:         h.suspendArgoWorkflow,
		appliedReason:     "WorkflowSuspended",
		appliedMsgFmt:     "Workflow suspended successfully. Current phase: %s",
		pendingAckReason:  "WorkflowSuspendAcknowledged",
		pendingAckMsg:     "Workflow suspend state acknowledged but pending - no backend operations until resources exist",
		finishedReason:    "WorkflowAlreadyFinished",
		finishedVerb:      "Suspend",
		initAckReason:     "WorkflowSuspendAcknowledged",
		initAckMsg:        "Workflow suspend state acknowledged during initialization - no backend operations until resources exist",
		initAppliedReason: "WorkflowSuspended",
		initAppliedMsg:    "Workflow suspended during initialization",
	})
}

// handleStoppedState gracefully stops the workflow if not already stopped
func (h *controlStateHandler) handleStoppedState(ctx context.Context, lw *v1alpha1.LakeFlow, currentPhase v1alpha1.WorkflowPhase) error {
	// Check if already stopped
	if lw.Status.ExecutionState == v1alpha1.ControlStateStopped {
		h.logger.Info("Workflow is already stopped", "lakeflow", lw.Name)
		return nil
	}

	return h.applyControlState(ctx, lw, currentPhase, controlStatePlan{
		target:            v1alpha1.ControlStateStopped,
		backendOp:         h.stopWorkflowOrCronWorkflow,
		appliedReason:     "WorkflowStopped",
		appliedMsgFmt:     "Workflow stopped gracefully. Current phase: %s",
		pendingAckReason:  "WorkflowStopAcknowledged",
		pendingAckMsg:     "Workflow stop state acknowledged but pending - no backend operations until resources exist",
		finishedReason:    "WorkflowAlreadyFinished",
		finishedVerb:      "Stop",
		initAckReason:     "WorkflowStopAcknowledged",
		initAckMsg:        "Workflow stop state acknowledged during initialization - no backend operations until resources exist",
		initAppliedReason: "WorkflowStopped",
		initAppliedMsg:    "Workflow stopped during initialization",
	})
}

// handleTerminatedState forcefully terminates the workflow if not already terminated
func (h *controlStateHandler) handleTerminatedState(ctx context.Context, lw *v1alpha1.LakeFlow, currentPhase v1alpha1.WorkflowPhase) error {
	// Check if already terminated
	if lw.Status.ExecutionState == v1alpha1.ControlStateTerminated {
		h.logger.Info("Workflow is already terminated", "lakeflow", lw.Name)
		return nil
	}

	return h.applyControlState(ctx, lw, currentPhase, controlStatePlan{
		target:            v1alpha1.ControlStateTerminated,
		backendOp:         h.terminateWorkflowOrCronWorkflow,
		appliedReason:     "WorkflowTerminated",
		appliedMsgFmt:     "Workflow terminated forcefully. Current phase: %s",
		pendingAckReason:  "WorkflowTerminateAcknowledged",
		pendingAckMsg:     "Workflow terminate state acknowledged but pending - no backend operations until resources exist",
		finishedReason:    "WorkflowAlreadyFinished",
		finishedVerb:      "Terminate",
		initAckReason:     "WorkflowTerminateAcknowledged",
		initAckMsg:        "Workflow terminate state acknowledged during initialization - no backend operations until resources exist",
		initAppliedReason: "WorkflowTerminated",
		initAppliedMsg:    "Workflow terminated during initialization",
	})
}

// suspendArgoWorkflow suspends the underlying Argo Workflow using patch operation
func (h *controlStateHandler) suspendArgoWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow != "" {
		// For CronWorkflows, suspend scheduling
		return h.suspendArgoCronWorkflow(ctx, lw)
	}
	h.logger.Info("No cronworkflow found in status, skip suspend", "lakeflow", lw.Name)
	return nil
}

// resumeArgoWorkflow resumes the underlying Argo Workflow using patch operation
func (h *controlStateHandler) resumeArgoWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow != "" {
		// For CronWorkflows, resume scheduling
		return h.resumeArgoCronWorkflow(ctx, lw)
	} else if lw.Status.WorkflowName != "" {
		// For regular Workflows, resume execution
		patchData := []byte(`{"spec":{"suspend":false}}`)
		workflow := &argowfv1.Workflow{}
		h.logger.Info("Resuming Workflow", "workflow", lw.Status.WorkflowName, "lakeflow", lw.Name)
		return h.resourceManager.Patch(ctx, lw.Status.WorkflowName, lw.Namespace, workflow, patchData, types.MergePatchType)
	}
	return errors.NewNotFound(
		schema.GroupResource{Group: "argoproj.io", Resource: "workflows"},
		lw.Name,
	)
}

// stopArgoWorkflow gracefully stops the underlying Argo Workflow using patch operation
func (h *controlStateHandler) stopArgoWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.WorkflowName == "" {
		return fmt.Errorf("no argo workflow name found in status")
	}

	patchData := []byte(`{"spec":{"shutdown":"Stop"}}`)
	workflow := &argowfv1.Workflow{}

	return h.resourceManager.Patch(ctx, lw.Status.WorkflowName, lw.Namespace, workflow, patchData, types.MergePatchType)
}

// terminateArgoWorkflow forcefully terminates the underlying Argo Workflow using patch operation
func (h *controlStateHandler) terminateArgoWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	workflowList := &argowfv1.WorkflowList{}
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{v1alpha1.WorkflowNameLabel: lw.Name},
	}

	if err := h.resourceManager.List(ctx, lw.Namespace, workflowList, listOpts...); err != nil {
		return fmt.Errorf("failed to list workflows for terminate: %w", err)
	}

	h.logger.Info("List argoWorkflow for lakeWorkflow",
		"lakeWorkflow", lw.Name, "namespace", lw.Namespace, "argoWorkflowCount", len(workflowList.Items))
	patchData := []byte(`{"spec":{"shutdown":"Terminate"}}`)
	for _, item := range workflowList.Items {
		if item.Status.Phase == argowfv1.WorkflowRunning {
			workflow := &argowfv1.Workflow{}
			if err := h.resourceManager.Patch(ctx, item.Name, item.Namespace, workflow, patchData, types.MergePatchType); err != nil {
				h.logger.Error(err, "Failed to terminate argoWorkflow", "argoWorkflow", item.Name)
				return err
			}
		}
	}

	// Also terminate any running SparkApplications (driver exec, or delete CR if exec fails)
	return h.terminateSparkApplications(ctx, lw)
}

// terminateSparkApplications delegates to SparkApplicationManager (exec pkill, or delete SparkApplication if exec fails).
func (h *controlStateHandler) terminateSparkApplications(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if h.sparkApplicationManager == nil {
		h.logger.Info("SparkApplicationManager not configured, skipping SparkApplication termination", "lakeflow", lw.Name)
		return nil
	}
	return h.sparkApplicationManager.TerminateRunningForLakeFlow(ctx, lw)
}

// stopOrTerminateChildWorkflows patches all running child workflows of a CronWorkflow
// with the specified shutdown strategy ("Stop" or "Terminate")
func (h *controlStateHandler) stopOrTerminateChildWorkflows(ctx context.Context, cronWorkflowName, namespace, shutdownStrategy string) error {
	workflowList := &argowfv1.WorkflowList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{v1alpha1.ArgoCronWorkflowLabel: cronWorkflowName},
	}

	if err := h.resourceManager.List(ctx, namespace, workflowList, listOpts...); err != nil {
		return fmt.Errorf("failed to list child workflows for cronworkflow %s: %w", cronWorkflowName, err)
	}

	patchData := []byte(fmt.Sprintf(`{"spec":{"shutdown":"%s"}}`, shutdownStrategy))

	for _, wf := range workflowList.Items {
		// Only patch running/pending workflows
		if wf.Status.Phase == argowfv1.WorkflowRunning || wf.Status.Phase == argowfv1.WorkflowPending {
			h.logger.Info("Patching child workflow",
				"workflow", wf.Name,
				"cronWorkflow", cronWorkflowName,
				"shutdownStrategy", shutdownStrategy)

			workflow := &argowfv1.Workflow{}
			if err := h.resourceManager.Patch(ctx, wf.Name, namespace, workflow, patchData, types.MergePatchType); err != nil {
				h.logger.Error(err, "Failed to patch child workflow", "workflow", wf.Name)
				// Continue with other workflows, don't fail on single error
			}
		}
	}
	return nil
}

// suspendArgoCronWorkflow suspends the underlying Argo CronWorkflow using patch operation
func (h *controlStateHandler) suspendArgoCronWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow == "" {
		return fmt.Errorf("no argo cronworkflow name found in status")
	}

	patchData := []byte(`{"spec":{"suspend":true}}`)
	cronWorkflow := &argowfv1.CronWorkflow{}

	h.logger.Info("Suspending CronWorkflow", "cronworkflow", lw.Status.CronWorkflow, "lakeflow", lw.Name)
	return h.resourceManager.Patch(ctx, lw.Status.CronWorkflow, lw.Namespace, cronWorkflow, patchData, types.MergePatchType)
}

// resumeArgoCronWorkflow resumes the underlying Argo CronWorkflow using patch operation
func (h *controlStateHandler) resumeArgoCronWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow == "" {
		return fmt.Errorf("no argo cronworkflow name found in status")
	}

	patchData := []byte(`{"spec":{"suspend":false}}`)
	cronWorkflow := &argowfv1.CronWorkflow{}

	h.logger.Info("Resuming CronWorkflow", "cronworkflow", lw.Status.CronWorkflow, "lakeflow", lw.Name)
	return h.resourceManager.Patch(ctx, lw.Status.CronWorkflow, lw.Namespace, cronWorkflow, patchData, types.MergePatchType)
}

// stopWorkflowOrCronWorkflow gracefully stops either a Workflow or CronWorkflow based on what exists in status
func (h *controlStateHandler) stopWorkflowOrCronWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow != "" {
		// For CronWorkflows, stop scheduling by suspending
		h.logger.Info("Stopping CronWorkflow by suspending scheduling", "cronworkflow", lw.Status.CronWorkflow, "lakeflow", lw.Name)
		if err := h.suspendArgoCronWorkflow(ctx, lw); err != nil {
			return err
		}
		// Also gracefully stop any running child workflows
		return h.stopOrTerminateChildWorkflows(ctx, lw.Status.CronWorkflow, lw.Namespace, "Stop")
	} else if lw.Status.WorkflowName != "" {
		// For regular Workflows, use shutdown
		h.logger.Info("Stopping Workflow", "workflow", lw.Status.WorkflowName, "lakeflow", lw.Name)
		return h.stopArgoWorkflow(ctx, lw)
	}
	return fmt.Errorf("no argo workflow or cronworkflow name found in status")
}

// terminateWorkflowOrCronWorkflow forcefully terminates either a Workflow or CronWorkflow based on what exists in status
func (h *controlStateHandler) terminateWorkflowOrCronWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.Status.CronWorkflow == "" && lw.Status.WorkflowName == "" {
		h.logger.Info("First create, CronWorkflow and WorkflowName is nil, terminating Workflow",
			"workflow", lw.Status.WorkflowName, "lakeflow", lw.Name)
		return h.terminateArgoWorkflow(ctx, lw)
	} else if lw.Status.CronWorkflow != "" {
		// For CronWorkflows, terminate by suspending scheduling
		h.logger.Info("Terminating CronWorkflow by suspending scheduling", "cronworkflow", lw.Status.CronWorkflow, "lakeflow", lw.Name)
		if err := h.suspendArgoCronWorkflow(ctx, lw); err != nil {
			return err
		}
		// Also forcefully terminate any running child workflows
		if err := h.stopOrTerminateChildWorkflows(ctx, lw.Status.CronWorkflow, lw.Namespace, "Terminate"); err != nil {
			return err
		}
		// Also terminate any running SparkApplications associated with this LakeFlow.
		return h.terminateSparkApplications(ctx, lw)
	} else if lw.Status.WorkflowName != "" {
		// For regular Workflows, use terminate
		h.logger.Info("Terminating Workflow", "workflow", lw.Status.WorkflowName, "lakeflow", lw.Name)
		return h.terminateArgoWorkflow(ctx, lw)
	}
	return fmt.Errorf("no argo workflow or cronworkflow name found in status")
}

// updateWorkflowTemplateStateLabel updates the WorkflowTemplate's control state label
// when the LakeFlow's spec.state changes. This keeps the label in sync with the actual state.
// Stage 3 Task 8: State Synchronization
func (h *controlStateHandler) updateWorkflowTemplateStateLabel(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	// Get the WorkflowTemplate name from status
	templateName := lw.Status.WorkflowTemplate
	if templateName == "" {
		// WorkflowTemplate hasn't been created yet, skip update
		h.logger.V(1).Info("WorkflowTemplate not created yet, skipping label update", "lakeflow", lw.Name)
		return nil
	}

	// Get current WorkflowTemplate
	workflowTemplate := &argowfv1.WorkflowTemplate{}
	if err := h.resourceManager.Get(ctx, templateName, lw.Namespace, workflowTemplate); err != nil {
		if errors.IsNotFound(err) {
			// Template doesn't exist yet, this is expected during initial creation
			h.logger.V(1).Info("WorkflowTemplate not found, skipping label update", "template", templateName)
			return nil
		}
		return fmt.Errorf("failed to get WorkflowTemplate: %w", err)
	}

	// Determine desired state
	desiredState := lw.Spec.State
	if desiredState == "" {
		desiredState = v1alpha1.ControlStateActive
	}

	// Check if label needs update
	currentLabel := workflowTemplate.Labels[v1alpha1.WorkflowControlStateLabel]
	if currentLabel == string(desiredState) {
		// Label is already correct, no update needed
		h.logger.V(1).Info("WorkflowTemplate state label already correct",
			"template", templateName,
			"state", desiredState)
		return nil
	}

	// Update the label using patch
	h.logger.Info("Updating WorkflowTemplate state label",
		"template", templateName,
		"oldState", currentLabel,
		"newState", desiredState)

	patchData := fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`,
		v1alpha1.WorkflowControlStateLabel,
		desiredState)

	return h.resourceManager.Patch(ctx, templateName, lw.Namespace, workflowTemplate, []byte(patchData), types.MergePatchType)
}
