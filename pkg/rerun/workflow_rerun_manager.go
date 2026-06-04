package rerun

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
)

// RerunOutcome classifies the terminal result of a rerun attempt so the
// controller can decide whether to clear the rerun annotations.
type RerunOutcome int

const (
	// RerunUnknown is the zero value. The controller treats it like an error
	// (keep annotations, backoff) so a code path that forgets to set an outcome
	// can never be mistaken for success.
	RerunUnknown RerunOutcome = iota
	// RerunCreated means a rerun Workflow was created (or already existed under
	// its deterministic name). Annotations are cleared and retry status updated.
	RerunCreated
	// RerunNoop means there was nothing to do (e.g. retry found no executed
	// workflow or no failed tasks). Annotations are cleared; retry status is NOT
	// incremented.
	RerunNoop
	// RerunIgnoredRunning means a targeted Spark task was observed (via a
	// successful, direct read) to already be running. The request is dropped.
	RerunIgnoredRunning
	// RerunBusyLocked means a competing reconciler holds the per-task lock. The
	// annotations are preserved so the request survives and is retried later.
	RerunBusyLocked
)

func (o RerunOutcome) String() string {
	switch o {
	case RerunCreated:
		return "Created"
	case RerunNoop:
		return "Noop"
	case RerunIgnoredRunning:
		return "IgnoredRunning"
	case RerunBusyLocked:
		return "BusyLocked"
	default:
		return "Unknown"
	}
}

// RerunResult is the structured outcome of a rerun attempt, with a
// human-readable message used for events and status conditions.
type RerunResult struct {
	Outcome RerunOutcome
	Message string
}

const (
	// DefaultMaxAttempts is the default maximum number of retry attempts
	DefaultMaxAttempts int32 = 3
	// MaxAllowedAttempts is the absolute maximum retry attempts allowed
	MaxAllowedAttempts int32 = 100

	// DefaultRerunTTLSecsAfterSuccess defines the default time-to-live in seconds for rerun workflows after a successful completion.
	DefaultRerunTTLSecsAfterSuccess int32 = 3600 * 24
	DefaultRerunTTLSecsAfterFailure int32 = 3600 * 24
)

var (
	cleanAnnotationSlices = []string{
		v1alpha1.ResubmitRequestAnnotation,
		v1alpha1.RetryRequestAnnotation,
		v1alpha1.RetryMaxAttemptsAnnotation,
		v1alpha1.RetrySkipSuccessfulAnnotation,
		v1alpha1.RerunTokenAnnotation,
	}
	logger = common.GetSharedLogger().WithName("workflow-rerun-manager")
)

func defaultAliyunProfile() cloudprofile.CloudProfile {
	profile, _ := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	return profile
}

// lockClient returns the uncached client used for lock and running-state reads,
// falling back to the cached client when no direct client is configured.
func (m *workflowRerunManager) lockClient() client.Client {
	if m.directClient != nil {
		return m.directClient
	}
	return m.k8sClient
}

// WorkflowRerunManager manages workflow-level rerun operations (retry & resubmit)
type WorkflowRerunManager interface {
	// ShouldResubmit checks if workflow should be resubmitted (fresh run)
	ShouldResubmit(lw *v1alpha1.LakeFlow) (bool, string)

	// ShouldRetry checks if workflow should be retried (failed nodes only)
	ShouldRetry(lw *v1alpha1.LakeFlow) (bool, string)

	// ResubmitWorkflow creates a new workflow from scratch (clone & create)
	ResubmitWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow, reason string) (RerunResult, error)

	// RetryWorkflow retries failed nodes in existing workflow
	RetryWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow, reason string) (RerunResult, error)

	// ClearRerunAnnotations removes both retry and resubmit annotations
	ClearRerunAnnotations(ctx context.Context, lw *v1alpha1.LakeFlow) error

	// HasActiveWorkflowInstance checks if there's any active workflow instance
	// Returns (hasActive, activeWorkflowName, error)
	HasActiveWorkflowInstance(ctx context.Context, lw *v1alpha1.LakeFlow) (bool, string, error)
}

// workflowRerunManager implements WorkflowRerunManager
type workflowRerunManager struct {
	k8sClient         client.Client
	config            *rest.Config
	resourceManager   argoclient.ArgoResourceClient
	workflowConverter adapter.WorkflowConverter

	// taskLockEnabled gates the per-task rerun lock (opt-in, default off).
	taskLockEnabled bool
	// directClient is an uncached client used for Lease operations AND the
	// SparkApplication running-check, so neither the lock nor a terminal
	// IgnoredRunning drop can be decided from stale cache. It falls back to
	// k8sClient when a direct client cannot be built (e.g. nil config in tests).
	directClient client.Client
}

// NewWorkflowRerunManager creates a new WorkflowRerunManager instance
func NewWorkflowRerunManager(k8sClient client.Client, config *rest.Config, resourceManager argoclient.ArgoResourceClient, workflowConverter adapter.WorkflowConverter, taskLockEnabled bool) WorkflowRerunManager {
	return &workflowRerunManager{
		k8sClient:         k8sClient,
		config:            config,
		resourceManager:   resourceManager,
		workflowConverter: workflowConverter,
		taskLockEnabled:   taskLockEnabled,
		directClient:      buildDirectClient(k8sClient, config),
	}
}

// buildDirectClient constructs an uncached client from config, reusing the
// cached client's scheme. It falls back to the cached client when config is nil
// or the build fails (which keeps unit tests that inject only a fake client
// working).
func buildDirectClient(cached client.Client, config *rest.Config) client.Client {
	if config == nil || cached == nil {
		return cached
	}
	direct, err := client.New(config, client.Options{Scheme: cached.Scheme()})
	if err != nil {
		logger.Error(err, "Failed to build direct client for rerun lock, falling back to cached client")
		return cached
	}
	return direct
}

// ShouldResubmit checks for resubmit annotation (higher priority)
func (m *workflowRerunManager) ShouldResubmit(lw *v1alpha1.LakeFlow) (bool, string) {
	annotations := lw.GetAnnotations()
	if annotations == nil {
		return false, ""
	}

	resubmitRequest, exists := annotations[v1alpha1.ResubmitRequestAnnotation]
	if !exists || resubmitRequest == "" {
		return false, ""
	}

	reason := m.buildRerunReason(resubmitRequest, "resubmit")
	logger.Info("Resubmit annotation detected, workflow will be resubmitted",
		"lakeflow", lw.Name,
		"namespace", lw.Namespace,
		"currentPhase", lw.Status.Phase,
		"reason", reason)

	return true, reason
}

// ShouldRetry checks for retry annotation (only for failed workflows)
func (m *workflowRerunManager) ShouldRetry(lw *v1alpha1.LakeFlow) (bool, string) {
	// Retry is only valid for failed workflows
	if lw.Status.Phase != v1alpha1.WorkflowPhaseFailed {
		return false, ""
	}

	annotations := lw.GetAnnotations()
	if annotations == nil {
		return false, ""
	}

	retryRequest, exists := annotations[v1alpha1.RetryRequestAnnotation]
	if !exists || retryRequest == "" {
		return false, ""
	}

	// Initialize retry status if needed (for tracking purposes)
	if lw.Status.RetryStatus == nil {
		lw.Status.RetryStatus = &v1alpha1.WorkflowRetryStatus{
			RetryAttempts: 0,
		}
	}

	// Note: We don't enforce max retry attempts at the workflow level
	// because each task has its own retry policy (maxRetries, backoff, etc.)
	// The workflow-level retry just recreates failed tasks, and task-level
	// retry policies control how many times each task is retried.
	currentAttempts := lw.Status.RetryStatus.RetryAttempts

	reason := m.buildRerunReason(retryRequest, "retry")
	logger.Info("Retry annotation detected, workflow eligible for retry",
		"lakeflow", lw.Name,
		"namespace", lw.Namespace,
		"retryAttempts", currentAttempts,
		"reason", reason)

	return true, reason
}

// ResubmitWorkflow creates a new workflow from scratch using the converter pattern
// This approach allows modifying the workflow spec (e.g., removing upstream dependencies and schedule)
// to enable immediate execution while preserving downstream triggering capability
// Works for:
// - Never-executed workflows (creates first execution, bypassing dependencies)
// - Already-executed workflows (creates new execution)
// - Scheduled workflows (CronWorkflow → immediate Workflow)
func (m *workflowRerunManager) ResubmitWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow, reason string) (RerunResult, error) {
	// Determine original workflow for logging (may be empty for never-executed workflows)
	originalWorkflow := lw.Status.WorkflowName
	if originalWorkflow == "" {
		originalWorkflow = lw.Status.CronWorkflow
	}
	if originalWorkflow == "" {
		originalWorkflow = "<never-executed>"
	}

	logger.Info("Resubmitting workflow via converter pattern",
		"lakeflow", lw.Name,
		"namespace", lw.Namespace,
		"originalWorkflow", originalWorkflow,
		"hasWorkflowName", lw.Status.WorkflowName != "",
		"hasCronWorkflow", lw.Status.CronWorkflow != "",
		"reason", reason)
	// Check for active workflow instances before resubmitting
	// This prevents concurrent execution and ensures only one instance runs at a time
	if hasActive, activeWorkflowName, err := m.hasActiveWorkflowInstance(ctx, lw); err != nil {
		return RerunResult{}, fmt.Errorf("failed to check for active instances: %w", err)
	} else if hasActive {
		msg := fmt.Sprintf("active workflow instance %q is still running", activeWorkflowName)
		logger.Info("Ignoring resubmit request due to active workflow instance",
			"lakeflow", lw.Name,
			"activeWorkflow", activeWorkflowName,
			"activePhase", lw.Status.Phase,
			"reason", reason)
		return RerunResult{Outcome: RerunIgnoredRunning, Message: msg}, nil
	}

	// Acquire the per-task rerun lock (no-op when the feature is disabled or
	// there are no Spark tasks) and verify no targeted Spark task is running.
	proceed, lock, outcome, lockReason, err := m.guardRerun(ctx, lw, sparkTaskNames(lw.Spec.Tasks))
	if err != nil {
		return RerunResult{}, err
	}
	// success is flipped to true only once the rerun resources are created, so
	// the lease is kept (until TTL) on success and released immediately on any
	// failure return below.
	success := false
	defer func() { lock.release(success) }()
	if !proceed {
		return RerunResult{Outcome: outcome, Message: lockReason}, nil
	}
	ctx = lock.ctx

	token := lw.GetAnnotations()[v1alpha1.RerunTokenAnnotation]

	// Create a deep copy of the LakeFlow to modify
	lwCopy := lw.DeepCopy()

	// Log original trigger configuration for debugging
	hasDependencies := len(lwCopy.Spec.WorkflowTrigger.Depend.Workflows) > 0
	hasSchedule := lwCopy.Spec.WorkflowTrigger.Schedule.Cron != ""
	logger.Info("Original trigger configuration before resubmit",
		"lakeflow", lw.Name,
		"hasDependencies", hasDependencies,
		"dependencyCount", len(lwCopy.Spec.WorkflowTrigger.Depend.Workflows),
		"hasSchedule", hasSchedule,
		"scheduleCron", lwCopy.Spec.WorkflowTrigger.Schedule.Cron)

	// CRITICAL: Clear the entire WorkflowTrigger to force standalone Workflow creation
	// For dependency-triggered workflows, keeping any part of the Trigger structure
	// causes the converter to stay in dependency mode instead of creating a Workflow instance.
	// This is the core difference between resubmit and normal execution:
	// - Normal: Waits for Lake Watcher dependency evaluation, creates WorkflowTemplate only
	// - Resubmit: Runs immediately as standalone Workflow, bypassing all triggers
	if hasDependencies {
		logger.Info("Clearing upstream dependencies for immediate execution",
			"lakeflow", lw.Name,
			"originalUpstreams", len(lwCopy.Spec.WorkflowTrigger.Depend.Workflows))
	}
	if hasSchedule {
		logger.Info("Clearing schedule trigger for immediate execution",
			"lakeflow", lw.Name,
			"originalCron", lwCopy.Spec.WorkflowTrigger.Schedule.Cron)
	}

	// Clear the entire trigger structure to force immediate Workflow creation
	lwCopy.Spec.WorkflowTrigger = v1alpha1.Trigger{}
	logger.Info("Cleared entire WorkflowTrigger structure for standalone Workflow creation",
		"lakeflow", lw.Name)

	// Use the WorkflowConverter to build the WorkflowTemplate and Workflow
	// With trigger cleared, converter will create a standalone Workflow instance
	converter := m.workflowConverter
	if converter == nil {
		converter = adapter.NewWorkflowConverter(defaultAliyunProfile())
	}
	argoResources, err := converter.Convert(lwCopy, m.config)
	if err != nil {
		logger.Error(err, "Failed to convert argo workflow", "lakeflow", lw.Name)
		return RerunResult{}, fmt.Errorf("failed to convert LakeFlow to Argo resources: %w", err)
	}

	// Debug: Log what resources were created by the converter
	logger.Info("Converter returned resources for resubmit",
		"lakeflow", lw.Name,
		"hasWorkflow", argoResources.Workflow != nil,
		"hasWorkflowTemplate", argoResources.WorkflowTemplate != nil,
		"hasCronWorkflow", argoResources.CronWorkflow != nil)

	if argoResources.Workflow == nil {
		logger.Error(nil, "CRITICAL: Converter did not create a Workflow instance for resubmit",
			"lakeflow", lw.Name,
			"hasWorkflowTemplate", argoResources.WorkflowTemplate != nil)
		return RerunResult{}, fmt.Errorf("converter failed to create Workflow instance for resubmit (WorkflowTemplate-only mode)")
	}

	// Create WorkflowTemplate and Workflow using shared method (deterministic
	// names derived from the rerun token, executed under the lock context).
	if err := m.createWorkflowWithRetry(ctx, lw, argoResources, token, false); err != nil {
		return RerunResult{}, err
	}

	logger.Info("Successfully resubmitted workflow via converter pattern",
		"lakeflow", lw.Name,
		"originalWorkflow", originalWorkflow,
		"isCronWorkflow", lw.Status.CronWorkflow != "")

	// Keep the lease alive until TTL to bridge the SparkApplication visibility gap.
	success = true
	return RerunResult{Outcome: RerunCreated, Message: fmt.Sprintf("resubmitted workflow (%s)", reason)}, nil
}

func (m *workflowRerunManager) getLastedExecutedWorkflow(ctx context.Context,
	lw *v1alpha1.LakeFlow) (*argowfv1.Workflow, error) {
	workflowList := &argowfv1.WorkflowList{}
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{
			v1alpha1.WorkflowNameLabel: lw.Name,
		},
	}
	err := m.resourceManager.List(ctx, lw.Namespace, workflowList, listOpts...)
	if err != nil {
		logger.Error(err, "RerunManager Failed to list workflows", "lakeflow", lw.Name, "namespace", lw.Namespace)
		return nil, err
	}
	if len(workflowList.Items) == 0 {
		return nil, nil
	}

	// Use common utility to find most relevant workflow (running > recently finished > recently created)
	mostRelevant := common.FindMostRelevantWorkflow(workflowList.Items)
	if mostRelevant != nil {
		logger.Info("Found most relevant workflow for rerun",
			"lakeflow", lw.Name,
			"workflow", mostRelevant.Name,
			"phase", mostRelevant.Status.Phase,
			"creationTime", mostRelevant.CreationTimestamp,
			"finishedAt", mostRelevant.Status.FinishedAt,
			"selectionReason", common.GetWorkflowSelectionReason(mostRelevant))
	}

	return mostRelevant, nil
}

// RetryWorkflow triggers the Argo Workflow retry
// Reimplemented to use custom DAG filtering (Fix & Retry pattern)
// This approach:
// 1. Fetches the failed workflow and analyzes which tasks failed
// 2. Builds a filtered DAG containing only failed tasks and their dependencies
// 3. Creates a new workflow with the filtered DAG, which uses updated templates
func (m *workflowRerunManager) RetryWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow, reason string) (RerunResult, error) {
	if lw.Status.WorkflowName == "" && lw.Status.CronWorkflow == "" {
		logger.Info("Status fields are empty, will discover workflow instances by label",
			"lakeflow", lw.Name,
			"namespace", lw.Namespace,
			"hint", "This is normal for resubmitted workflows that have never been scheduled")
	}
	workflowName := lw.Status.WorkflowName
	if workflowName == "" {
		workflowName = lw.Status.CronWorkflow
	}

	logger.Info("Triggering workflow retry (via custom DAG filtering)",
		"lakeflow", lw.Name,
		"namespace", lw.Namespace,
		"argoWorkflow", workflowName,
		"reason", reason)

	// Fetch the failed workflow to analyze its status
	failedWorkflow, fineErr := m.getLastedExecutedWorkflow(ctx, lw)
	if fineErr != nil {
		return RerunResult{}, fmt.Errorf("workflow retry failed: %w", fineErr)
	}
	if failedWorkflow == nil {
		logger.Info("No executed workflow instances were found.",
			"lakeflow", lw.Name, "workflowInstance", workflowName)
		return RerunResult{Outcome: RerunNoop, Message: "no executed workflow instance found to retry"}, nil
	}
	logger.Info("Triggering workflow retry (via custom DAG filtering) ",
		"StatusWorkflowName", workflowName, "LakeFlowName", failedWorkflow.Name)
	// get tasks failed
	failedTasks, err := m.identifyFailedTasks(ctx, failedWorkflow)
	if err != nil {
		logger.Error(err, "Failed to identify failed tasks",
			"lakeflow", lw.Name,
			"argoWorkflow", workflowName)
		return RerunResult{}, fmt.Errorf("failed to identify failed tasks: %w", err)
	}

	if len(failedTasks) == 0 {
		logger.Info("No failed tasks found in workflow, skipping retry",
			"lakeflow", lw.Name,
			"argoWorkflow", workflowName)
		return RerunResult{Outcome: RerunNoop, Message: "no failed tasks to retry"}, nil
	}

	// Build a filtered DAG with only failed tasks and their dependencies
	filteredTasks, err := m.buildFilteredDAG(lw, failedTasks)
	if err != nil {
		logger.Error(err, "Failed to build filtered DAG",
			"lakeflow", lw.Name,
			"failedTasks", failedTasks)
		return RerunResult{}, fmt.Errorf("failed to build filtered DAG: %w", err)
	}

	// Acquire the per-task rerun lock for exactly the tasks that will re-run, and
	// verify none of them already has a running Spark instance.
	proceed, lock, outcome, lockReason, err := m.guardRerun(ctx, lw, sparkTaskNames(filteredTasks))
	if err != nil {
		return RerunResult{}, err
	}
	// success is flipped to true only once the retry workflow is created, so the
	// lease is kept (until TTL) on success and released immediately on failure.
	success := false
	defer func() { lock.release(success) }()
	if !proceed {
		return RerunResult{Outcome: outcome, Message: lockReason}, nil
	}
	ctx = lock.ctx

	// resubmit failed workflow
	resubmitErr := m.createFilteredWorkflow(ctx, lw, filteredTasks)
	if resubmitErr != nil {
		logger.Error(resubmitErr, "Failed to create filtered retry workflow",
			"lakeflow", lw.Name)
		return RerunResult{}, fmt.Errorf("failed to create filtered retry workflow: %w", resubmitErr)
	}

	logger.Info("Successfully created and submitted retry workflow",
		"lakeflow", lw.Name,
		"oldArgoWorkflow", workflowName,
		"failedTaskCount", len(failedTasks),
		"filteredTaskCount", len(filteredTasks))

	// Keep the lease alive until TTL to bridge the SparkApplication visibility gap.
	success = true
	return RerunResult{Outcome: RerunCreated, Message: fmt.Sprintf("retried %d failed task(s)", len(failedTasks))}, nil
}

// rerunWorkflowTemplateGC deletes stale rerun WorkflowTemplates for this LakeFlow.
// keepName, when non-empty, is the deterministic template name for the current
// rerun token; it is never deleted so a reused token does not race its own
// template away.
func (m *workflowRerunManager) rerunWorkflowTemplateGC(ctx context.Context, lw *v1alpha1.LakeFlow, rerunLabel, keepName string) {
	logger.Info("Starting rerun WorkflowTemplate GC", "lakeWorkflow", lw.Name, "rerunLabel", rerunLabel, "keepName", keepName)
	list := &argowfv1.WorkflowTemplateList{}
	if err := m.resourceManager.List(ctx, lw.Namespace, list,
		client.MatchingLabels{
			v1alpha1.WorkflowNameLabel: lw.Name,
			v1alpha1.ManagedByLabel:    v1alpha1.ManagerByValue,
			rerunLabel:                 "true",
		},
	); err != nil {
		logger.Error(err, "Failed to list rerun WorkflowTemplate for gc",
			"lakeWorkflow", lw.Name, "rerunLabel", rerunLabel)
		return
	}

	for i := range list.Items {
		name := list.Items[i].Name
		if name == keepName {
			continue
		}
		err := m.resourceManager.Delete(ctx, name, lw.Namespace, &argowfv1.WorkflowTemplate{})
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			logger.Error(err, "Failed to delete rerun WorkflowTemplate for gc")
		}
		logger.Info("Deleted rerun WorkflowTemplate",
			"name", name, "lakeWorkflow", lw.Name, "label", rerunLabel)
	}
}

// createRerunWorkflow creates WorkflowTemplate and Workflow resources for rerun operations (retry/resubmit)
// This method encapsulates the common logic shared between ResubmitWorkflow and RetryWorkflow.
// Resource names are derived deterministically from the per-request token so that
// concurrent or restarted creates collapse onto the same objects (Create returns
// AlreadyExists, which is treated as success) — making the operation idempotent.
// Parameters:
//   - argoResources: the converted Argo resources from WorkflowConverter
//   - token: the durable per-request idempotency token (drives deterministic names)
//   - isRetry: true for retry operation, false for resubmit operation
func (m *workflowRerunManager) createRerunWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow,
	argoResources *adapter.ArgoWorkflowCR, token string, isRetry bool) error {

	operationType := "resubmit"
	workflowLabel := v1alpha1.IsResubmitWorkflowLabel
	if isRetry {
		operationType = "retry"
		workflowLabel = v1alpha1.IsRetryWorkflowLabel
	}

	// Abort early if the lock was lost while we were preparing resources.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rerun lock lost before resource creation: %w", err)
	}

	uniqueSuffix := rerunUniqueSuffix(operationType, token)

	// Step 1: Create WorkflowTemplate first if it exists
	// CRITICAL: WorkflowTemplate must be created and available before Workflow
	// to prevent "template not found" errors
	if argoResources.WorkflowTemplate != nil {
		// Rename the template with the deterministic suffix to avoid conflicts.
		originalTemplateName := argoResources.WorkflowTemplate.Name
		newTemplateName := fmt.Sprintf("%s-%s", originalTemplateName, uniqueSuffix)
		argoResources.WorkflowTemplate.Name = newTemplateName

		// retry/resubmit has at most 1 WorkflowTemplate; GC stale ones but keep
		// the current token's template so a reused token is not raced away.
		m.rerunWorkflowTemplateGC(ctx, lw, workflowLabel, newTemplateName)

		// Update the Workflow to reference the new template name
		if argoResources.Workflow != nil && argoResources.Workflow.Spec.WorkflowTemplateRef != nil {
			argoResources.Workflow.Spec.WorkflowTemplateRef.Name = newTemplateName
		}

		// Match Workflow labeling so rerun templates are discoverable alongside rerun workflows
		tmplLabels := argoResources.WorkflowTemplate.GetLabels()
		if tmplLabels == nil {
			tmplLabels = make(map[string]string)
		}
		tmplLabels[workflowLabel] = "true"
		if token != "" {
			tmplLabels[v1alpha1.RerunTokenAnnotation] = token
		}
		argoResources.WorkflowTemplate.SetLabels(tmplLabels)

		// Create the template
		logger.Info("Creating WorkflowTemplate for rerun",
			"resourceName", newTemplateName,
			"lakeflow", lw.Name,
			"operationType", operationType)

		if createErr := m.resourceManager.Create(ctx, argoResources.WorkflowTemplate); createErr != nil {
			if errors.IsAlreadyExists(createErr) || strings.Contains(createErr.Error(), "already exists") {
				// Verify the existing object is the one THIS request would have
				// created before trusting AlreadyExists as idempotent success.
				if vErr := m.verifyExistingRerunResource(ctx, &argowfv1.WorkflowTemplate{},
					newTemplateName, lw.Namespace, lw.Name, token, workflowLabel); vErr != nil {
					logger.Error(vErr, "Existing WorkflowTemplate failed rerun verification",
						"resourceName", newTemplateName,
						"lakeflow", lw.Name,
						"operationType", operationType)
					return vErr
				}
				logger.Info("WorkflowTemplate already exists and matches this rerun, treating as success",
					"resourceName", newTemplateName,
					"lakeflow", lw.Name,
					"operationType", operationType)
				// Continue - template already exists (idempotent), we can use it
			} else {
				logger.Error(createErr, "Failed to create WorkflowTemplate for rerun",
					"resourceName", newTemplateName,
					"operationType", operationType)
				return createErr
			}
		}

		// Wait for the template to be queryable before proceeding
		// This prevents race conditions where the Workflow is created before the template is available
		// Use exponential backoff to handle API server eventual consistency
		if err := common.WaitForWorkflowTemplateReady(ctx, m.resourceManager, newTemplateName, lw.Namespace); err != nil {
			logger.Error(err, "WorkflowTemplate not ready after retries, cannot proceed with Workflow creation",
				"resourceName", newTemplateName,
				"operationType", operationType,
				"maxWaitTime", common.WorkflowTemplateBackoffMaxElapsedTime)
			return fmt.Errorf("WorkflowTemplate %s not ready after waiting %v: %w",
				newTemplateName, common.WorkflowTemplateBackoffMaxElapsedTime, err)
		}
		logger.Info("WorkflowTemplate verified available",
			"resourceName", newTemplateName,
			"lakeflow", lw.Name,
			"operationType", operationType)
	}

	// Step 2: Create Workflow with appropriate label (is-retry or is-resubmit)
	if argoResources.Workflow != nil {
		// Re-check the lock right before the mutating create.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("rerun lock lost before workflow creation: %w", err)
		}

		wf := argoResources.Workflow
		labels := wf.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[workflowLabel] = "true"
		if token != "" {
			labels[v1alpha1.RerunTokenAnnotation] = token
		}

		// If resubmitting a CronWorkflow, add association label for traceability
		if lw.Status.CronWorkflow != "" {
			labels[v1alpha1.ArgoCronWorkflowLabel] = lw.Status.CronWorkflow
			// Add mutexes for cron workflow, in case two workflow running for the task
			wf.Spec.Synchronization = &argowfv1.Synchronization{
				Mutexes: []*argowfv1.Mutex{{Name: lw.Status.CronWorkflow}},
			}
		}
		wf.SetLabels(labels)

		// Apply shorter TTL for rerun workflows to prevent sensor mismatch issues
		// This ensures old successful rerun workflows are cleaned up before the next trigger cycle
		successTTL := DefaultRerunTTLSecsAfterSuccess
		failureTTL := DefaultRerunTTLSecsAfterFailure
		wf.Spec.TTLStrategy = &argowfv1.TTLStrategy{
			SecondsAfterSuccess: &successTTL,
			SecondsAfterFailure: &failureTTL,
		}
		logger.Info("Applied rerun TTL strategy to workflow",
			"lakeflow", lw.Name,
			"operationType", operationType,
			"secondsAfterSuccess", DefaultRerunTTLSecsAfterSuccess,
			"secondsAfterFailure", DefaultRerunTTLSecsAfterFailure)

		// Assign a deterministic name derived from the token so the create is
		// atomic: the first writer wins and any concurrent/retried create gets
		// AlreadyExists, which we treat as success (no duplicate workflow).
		wf.Name = rerunWorkflowName(lw.Name, uniqueSuffix)
		wf.GenerateName = ""

		logger.Info("Creating Workflow for rerun",
			"resourceName", wf.Name,
			"lakeflow", lw.Name,
			"operationType", operationType)

		if createErr := m.resourceManager.Create(ctx, wf); createErr != nil {
			if errors.IsAlreadyExists(createErr) || strings.Contains(createErr.Error(), "already exists") {
				// Verify the existing Workflow is the one THIS request would have
				// created before trusting AlreadyExists as idempotent success.
				if vErr := m.verifyExistingRerunResource(ctx, &argowfv1.Workflow{},
					wf.Name, lw.Namespace, lw.Name, token, workflowLabel); vErr != nil {
					logger.Error(vErr, "Existing rerun Workflow failed verification",
						"resourceName", wf.Name,
						"lakeflow", lw.Name,
						"operationType", operationType)
					return vErr
				}
				logger.Info("Rerun Workflow already exists for this token and matches, treating as success (idempotent)",
					"resourceName", wf.Name,
					"lakeflow", lw.Name,
					"operationType", operationType)
				return nil
			}
			logger.Error(createErr, "Failed to create Workflow for rerun",
				"resourceName", wf.Name,
				"operationType", operationType)
			return createErr
		}
		logger.Info("Workflow created successfully",
			"createdName", wf.Name,
			"lakeflow", lw.Name,
			"operationType", operationType)
	}

	return nil
}

// createWorkflowWithRetry creates WorkflowTemplate and Workflow for rerun operations
// using deterministic, token-derived names for atomic idempotency.
func (m *workflowRerunManager) createWorkflowWithRetry(ctx context.Context, lw *v1alpha1.LakeFlow,
	argoResources *adapter.ArgoWorkflowCR, token string, isRetry bool) error {
	return m.createRerunWorkflow(ctx, lw, argoResources, token, isRetry)
}

// verifyExistingRerunResource fetches a rerun resource (by deterministic name)
// that a Create reported as AlreadyExists and confirms it is the object THIS
// request would have created: it must carry the operation marker label, the
// owning LakeFlow name, and our durable rerun token. A match makes the Create
// idempotent (safe to treat as success); a mismatch is a genuine name collision
// with a stale or foreign object and is surfaced as an error rather than masked.
func (m *workflowRerunManager) verifyExistingRerunResource(
	ctx context.Context,
	obj client.Object,
	name, namespace, lakeflowName, token, markerLabel string,
) error {
	if err := m.resourceManager.Get(ctx, name, namespace, obj); err != nil {
		return fmt.Errorf("rerun resource %q reported AlreadyExists but could not be fetched for verification: %w", name, err)
	}
	labels := obj.GetLabels()
	if labels[markerLabel] != "true" {
		return fmt.Errorf("rerun resource %q already exists but is not a rerun resource (missing %s=true); refusing to reuse it", name, markerLabel)
	}
	if labels[v1alpha1.WorkflowNameLabel] != lakeflowName {
		return fmt.Errorf("rerun resource %q already exists but belongs to a different LakeFlow (%s=%q, want %q); refusing to reuse it",
			name, v1alpha1.WorkflowNameLabel, labels[v1alpha1.WorkflowNameLabel], lakeflowName)
	}
	if token != "" && labels[v1alpha1.RerunTokenAnnotation] != token {
		return fmt.Errorf("rerun resource %q already exists but carries a different rerun token (%q != %q); refusing to reuse a stale resource",
			name, labels[v1alpha1.RerunTokenAnnotation], token)
	}
	return nil
}

// ClearRerunAnnotations removes all rerun-related annotations (trigger + configuration)
func (m *workflowRerunManager) ClearRerunAnnotations(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	key := client.ObjectKey{Namespace: lw.Namespace, Name: lw.Name}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Fetch latest version to avoid conflicts
		latest := &v1alpha1.LakeFlow{}
		if err := m.k8sClient.Get(ctx, key, latest); err != nil {
			return err
		}

		annotations := latest.GetAnnotations()
		if annotations == nil {
			return nil
		}

		changed := false
		// Clear all rerun-related annotations
		for _, annotationVal := range cleanAnnotationSlices {
			if _, exists := annotations[annotationVal]; exists {
				delete(annotations, annotationVal)
				changed = true
			}
		}
		if !changed {
			return nil
		}
		latest.SetAnnotations(annotations)
		if err := m.k8sClient.Update(ctx, latest); err != nil {
			logger.Error(err, "Failed to clear rerun annotations",
				"lakeflow", lw.Name,
				"namespace", lw.Namespace)
			return err
		}

		logger.Info("Cleared rerun annotations",
			"lakeflow", lw.Name,
			"namespace", lw.Namespace, "annotations", annotations)
		return nil
	})
}

// getMaxAttempts retrieves max attempts from annotation or returns default
func (m *workflowRerunManager) getMaxAttempts(lw *v1alpha1.LakeFlow) int32 {
	annotations := lw.GetAnnotations()
	if annotations == nil {
		return DefaultMaxAttempts
	}

	maxStr, exists := annotations[v1alpha1.RetryMaxAttemptsAnnotation]
	if !exists {
		return DefaultMaxAttempts
	}

	maxVal, err := strconv.ParseInt(maxStr, 10, 32)
	if err != nil || maxVal < 1 || maxVal > int64(MaxAllowedAttempts) {
		logger.Info("Invalid retry-maxVal-attempts annotation, using default",
			"lakeflow", lw.Name,
			"invalidValue", maxStr,
			"default", DefaultMaxAttempts)
		return DefaultMaxAttempts
	}

	return int32(maxVal)
}

// getSkipSuccessful retrieves skip-successful setting from annotation
func (m *workflowRerunManager) getSkipSuccessful(lw *v1alpha1.LakeFlow) bool {
	annotations := lw.GetAnnotations()
	if annotations == nil {
		return true // default: skip successful tasks
	}
	skipStr, exists := annotations[v1alpha1.RetrySkipSuccessfulAnnotation]
	if !exists {
		return true
	}

	// Only "false" disables skipping; everything else enables it
	return skipStr != "false"
}

// buildRerunReason constructs a human-readable rerun reason
func (m *workflowRerunManager) buildRerunReason(request string, operationType string) string {
	if request == "true" {
		return fmt.Sprintf("Manual %s requested (immediate)", operationType)
	}

	if _, err := time.Parse(time.RFC3339, request); err == nil {
		return fmt.Sprintf("Manual %s requested at %s", operationType, request)
	}

	return fmt.Sprintf("Manual %s requested: %s", operationType, request)
}

// getTaskNames extracts task names from a list of tasks for logging
func getTaskNames(tasks []v1alpha1.Task) []string {
	names := make([]string, len(tasks))
	for i, task := range tasks {
		names[i] = task.Name
	}
	return names
}
