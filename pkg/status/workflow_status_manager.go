package status

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/sparkmanager"
	"github.com/pzhenzhou/lakeflow-controller/pkg/metrics"
	"github.com/pzhenzhou/lakeflow-controller/pkg/reconciler"
)

const (
	ConditionTypeValidation   = "Validation"
	ConditionTypeRerunIgnored = "RerunIgnored"

	ReasonResourcesCreated    = "ResourcesCreated"
	ReasonResourcesUpdated    = "ResourcesUpdated"
	ReasonResourcesUpToDate   = "ResourcesUpToDate"
	ReasonReconciliationError = "ReconciliationError"
	ReasonWorkflowNotFound    = "WorkflowNotFound"
	ReasonPhaseUpdated        = "PhaseUpdated"
)

var (
	waitBackOff = wait.Backoff{
		Steps:    15,
		Duration: 500 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0.2,
		Cap:      30 * time.Second,
	}
)

// WorkflowStatusManager manages the status of a LakeFlow object throughout a single reconciliation loop.
// It is stateful and holds a copy of the original status to detect changes.
type WorkflowStatusManager struct {
	client.Client
	resourceManager       argoclient.ArgoResourceClient
	originalStatus        *v1alpha1.LakeFlowStatus
	logger                logr.Logger
	reconcileStateHandler *reconcileStateHandler
	controlStateHandler   *controlStateHandler
}

// NewStatusManager creates a new WorkflowStatusManager for a single reconciliation loop.
// It takes a deep copy of the initial status to track changes.
func NewStatusManager(
	c client.Client,
	rm argoclient.ArgoResourceClient,
	lw *v1alpha1.LakeFlow,
	logger logr.Logger,
	sparkMgr *sparkmanager.SparkApplicationManager,
) *WorkflowStatusManager {
	return &WorkflowStatusManager{
		Client:                c,
		resourceManager:       rm,
		originalStatus:        lw.Status.DeepCopy(),
		logger:                logger,
		reconcileStateHandler: &reconcileStateHandler{resourceManager: rm, logger: logger},
		controlStateHandler:   &controlStateHandler{resourceManager: rm, sparkApplicationManager: sparkMgr, logger: logger},
	}
}

// Persist writes the updated status to the Kubernetes API server if and only if
// the status has changed from the beginning of the reconciliation loop.
func (sm *WorkflowStatusManager) Persist(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	key := client.ObjectKey{Namespace: lw.Namespace, Name: lw.Name}

	// Handle status updates with retry logic
	return retry.RetryOnConflict(waitBackOff, func() error {
		latestWl := &v1alpha1.LakeFlow{}
		if err := sm.Get(ctx, key, latestWl); err != nil {
			sm.logger.Error(err, "Failed to get latest LakeFlow for status update",
				"name", lw.Name, "namespace", lw.Namespace)
			return err
		}

		// Only update status if it actually changed
		if !equality.Semantic.DeepEqual(sm.originalStatus, &lw.Status) {
			latestWl.Status = *lw.Status.DeepCopy()
			if err := sm.Status().Update(ctx, latestWl); err != nil {
				sm.logger.Info("Status update failed, retrying on conflict",
					"name", lw.Name, "error", err.Error())
				return err
			}
			sm.logger.Info("Status updated successfully",
				"name", lw.Name, "namespace", lw.Namespace,
				"phase", lw.Status.Phase,
				"executionState", lw.Status.ExecutionState,
				"argoWorkflow", lw.Status.WorkflowName)
		}
		return nil
	})
}

// Note: Label management has been removed.
// The source of truth for workflow state is now in status fields:
// - spec.state: desired execution state (Normal/Suspend/Stop/Terminate)
// - status.phase: execution phase (Pending/Running/Succeeded/Failed/Completed)
// - status.executionState: control/scheduling state (Active/Suspended/Stopped/Terminated)
// Use field selectors or status field queries instead of labels for filtering.

// ObserveExecution synchronizes the LakeFlow status with the state of its underlying Argo resources.
func (sm *WorkflowStatusManager) ObserveExecution(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	return sm.reconcileStateHandler.SyncStatus(ctx, lw)
}

// UpdateValidation updates the validation condition in the LakeFlow status.
func (sm *WorkflowStatusManager) UpdateValidation(lw *v1alpha1.LakeFlow, validationErr error) {
	UpdateValidationCondition(lw, validationErr)
}

// RecordRerunIgnored records a condition explaining that a rerun was dropped
// because a targeted task was already running. This is the user-visible signal
// alongside the Kubernetes event.
func (sm *WorkflowStatusManager) RecordRerunIgnored(lw *v1alpha1.LakeFlow, message string) {
	updateCondition(lw, ConditionTypeRerunIgnored, corev1.ConditionTrue, "TaskAlreadyRunning", message)
}

// ApplyReconcileOutcome updates the status based on the result of the resource reconciliation process.
func (sm *WorkflowStatusManager) ApplyReconcileOutcome(ctx context.Context, lw *v1alpha1.LakeFlow, result *reconciler.ReconcileResult) error {
	return sm.reconcileStateHandler.UpdateReconciliationStatus(ctx, lw, result)
}

// ApplyControlState applies control commands like suspend or stop to the underlying resources.
func (sm *WorkflowStatusManager) ApplyControlState(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	return sm.controlStateHandler.Handle(ctx, lw)
}

// UpdateRetryStatus updates the retry status after a retry attempt
func (sm *WorkflowStatusManager) UpdateRetryStatus(lw *v1alpha1.LakeFlow, reason string) {
	// Initialize retry status if needed
	if lw.Status.RetryStatus == nil {
		lw.Status.RetryStatus = &v1alpha1.WorkflowRetryStatus{
			RetryAttempts: 0,
		}
	}

	// Increment retry attempts
	lw.Status.RetryStatus.RetryAttempts++

	// Update last retry time and reason
	now := metav1.Now()
	lw.Status.RetryStatus.LastRetryTime = &now
	lw.Status.RetryStatus.LastRetryReason = reason

	// Increment workflow retry metric
	metrics.GetMetricsManager().ObserveWorkflowRetry(lw)

	sm.logger.Info("Updated retry status (phase will be observed from Argo Workflow)",
		"lakeflow", lw.Name,
		"namespace", lw.Namespace,
		"retryAttempts", lw.Status.RetryStatus.RetryAttempts,
		"reason", reason)
}

// UpdateValidationCondition is a helper function to update the validation status condition.
func UpdateValidationCondition(lw *v1alpha1.LakeFlow, validationError error) {
	condition := v1alpha1.LakeFlowCondition{
		Type:               ConditionTypeValidation,
		LastTransitionTime: metav1.Now(),
	}
	if validationError != nil {
		condition.Status = corev1.ConditionFalse
		condition.Reason = "ValidationFailed"
		condition.Message = validationError.Error()
	} else {
		condition.Status = corev1.ConditionTrue
		condition.Reason = "ValidationSucceeded"
		condition.Message = "All validation checks passed"
	}
	setLakeFlowCondition(&lw.Status.Conditions, condition)
}
