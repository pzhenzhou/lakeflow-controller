package reconciler

import (
	"context"
	"fmt"

	"k8s.io/client-go/rest"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LakeFlowResourceProcessor handles the core reconciliation logic for LakeFlow resources
// It is responsible for creating and updating Argo resources based on the declared state
type LakeFlowResourceProcessor interface {
	// Process creates or updates Argo resources based on the LakeFlow specification
	// It returns the action taken and any error that occurred
	Process(ctx context.Context, lw *v1alpha1.LakeFlow) (*ReconcileResult, error)
}

// reconcilerImpl implements the LakeFlowResourceProcessor interface
type reconcilerImpl struct {
	resourceManager   argoclient.ArgoResourceClient
	workflowConverter adapter.WorkflowConverter
	provider          cloud.Provider
	k8Client          client.Client
	// config is the cluster connection used by the converter; it is a
	// construction-time dependency, not per-reconcile state.
	config *rest.Config

	// processor owns the validator/comparator delegates; reconcilerImpl drives
	// all resource CRUD through it rather than holding those delegates directly.
	processor *resourceProcessor
}

type workflowTriggerClass string

const (
	triggerClassSchedule   workflowTriggerClass = "schedule"
	triggerClassDependency workflowTriggerClass = "dependency"
	triggerClassImmediate  workflowTriggerClass = "immediate"
)

// NewLakeFlowReconciler creates a new LakeFlowResourceProcessor
func NewLakeFlowReconciler(
	resourceManager argoclient.ArgoResourceClient,
	workflowConverter adapter.WorkflowConverter,
	provider cloud.Provider,
	k8Client client.Client,
	config *rest.Config,
) LakeFlowResourceProcessor {
	// Initialize delegate components
	comparator := newResourceComparator()
	validator := newResourceValidator(resourceManager)
	processor := newResourceProcessor(resourceManager, validator, comparator)

	return &reconcilerImpl{
		resourceManager:   resourceManager,
		workflowConverter: workflowConverter,
		provider:          provider,
		k8Client:          k8Client,
		config:            config,
		processor:         processor,
	}
}

// Process implements an optimized reconciliation logic:
// 1. Check if LakeFlow exists by verifying WorkflowTemplate presence
// 2. If not exists, convert LakeFlow to Argo resources and create them
// 3. If exists, compare generation with observedGeneration to detect changes
// 4. If no changes, return early
// 5. If changes detected, update Argo resources
// 6. Return the overall reconciliation result with action taken
//
// Note: Dependency triggering is handled by Lake Watcher (external service).
// Lakeflow Controller only manages WorkflowTemplate, CronWorkflow, and Workflow resources.
func (r *reconcilerImpl) Process(ctx context.Context, lw *v1alpha1.LakeFlow) (*ReconcileResult, error) {
	logger.Info("Starting reconciliation", "lakeflow", lw.Name, "namespace", lw.Namespace)
	exists, existingLW, err := r.lakeWorkflowExists(ctx, lw)
	if err != nil {
		return errorResult(err, "Failed to check if LakeFlow exists: %v", err), err
	}

	if !exists {
		logger.Info("LakeFlow doesn't exist yet, creating resources", "lakeflow", lw.Name)
		return r.createResources(ctx, lw)
	}

	hasChanges, err := r.hasLakeFlowChanged(lw, existingLW)
	if err != nil {
		return errorResult(err, "Failed to detect changes: %v", err), err
	}

	if !hasChanges {
		hasChanges, err = r.hasSparkPVCLabelDrift(ctx, lw, existingLW)
		if err != nil {
			return errorResult(err, "Failed to check Spark PVC label drift: %v", err), err
		}
	}

	if !hasChanges {
		logger.Info("No changes detected in LakeFlow spec", "lakeflow", lw.Name)
		return &ReconcileResult{
			Action:        ReconcileActionNoChange,
			Message:       "No changes detected, resources are up to date",
			ResourceNames: r.getExistingResourceNames(ctx, lw),
		}, nil
	}

	logger.Info("Changes detected in LakeFlow, updating resources", "lakeflow", lw.Name)
	return r.updateResources(ctx, lw, existingLW)
}

// lakeWorkflowExists checks if this LakeFlow has been processed before.
// It uses status.workflowTemplate to find the template name (supports both legacy and versioned names).
// The WorkflowTemplate is always the first resource created for any trigger type.
// If it exists, we know the LakeFlow has been processed at least once.
// Returns (exists bool, existing LakeFlow object from cluster, error)
func (r *reconcilerImpl) lakeWorkflowExists(ctx context.Context, lw *v1alpha1.LakeFlow) (bool, *v1alpha1.LakeFlow, error) {
	logger.Info("Checking if LakeFlow has been processed before", "lakeflow", lw.Name, "namespace", lw.Namespace)

	// Step 1: Fetch existing LakeFlow from cluster to check status
	existingLW := &v1alpha1.LakeFlow{}
	err := r.k8Client.Get(ctx, client.ObjectKey{Name: lw.Name, Namespace: lw.Namespace}, existingLW)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("LakeFlow not found in cluster, first time processing", "lakeflow", lw.Name)
			return false, nil, nil
		}
		logger.Error(err, "Failed to get LakeFlow from cluster", "name", lw.Name, "namespace", lw.Namespace)
		return false, nil, fmt.Errorf("failed to get LakeFlow: %w", err)
	}

	// Step 2: Check if WorkflowTemplate name is tracked in status
	if existingLW.Status.WorkflowTemplate == "" {
		// Status not initialized yet, treat as first-time creation
		logger.Info("WorkflowTemplate not tracked in status yet, treating as first time processing", "lakeflow", lw.Name)
		return false, nil, nil
	}

	// Step 3: Verify the template actually exists (using name from status - supports both legacy and versioned)
	workflowTemplateName := existingLW.Status.WorkflowTemplate
	workflowTemplate := &argowfv1.WorkflowTemplate{}
	err = r.resourceManager.Get(ctx, workflowTemplateName, lw.Namespace, workflowTemplate)
	if err != nil {
		if errors.IsNotFound(err) {
			// Template tracked in status but doesn't exist - treat as not exists
			// This can happen if template was manually deleted
			logger.Info("WorkflowTemplate tracked in status but not found in cluster, will recreate",
				"template", workflowTemplateName,
				"lakeflow", lw.Name,
				"warning", "Template may have been manually deleted")
			return false, nil, nil
		}
		logger.Error(err, "Failed to get WorkflowTemplate", "name", workflowTemplateName, "namespace", lw.Namespace)
		return false, nil, fmt.Errorf("failed to get WorkflowTemplate: %w", err)
	}

	logger.Info("LakeFlow has been processed before", "lakeflow", lw.Name,
		"workflowTemplate", workflowTemplateName,
		"currentGeneration", lw.Generation,
		"observedGeneration", existingLW.Status.ObservedGeneration)
	return true, existingLW, nil
}

// hasLakeFlowChanged determines if the LakeFlow has changes that need reconciliation.
// It uses the standard Kubernetes pattern: comparing metadata.generation with status.observedGeneration.
// When generation > observedGeneration, it means the spec has been updated but not yet reconciled.
func (r *reconcilerImpl) hasLakeFlowChanged(desired *v1alpha1.LakeFlow, existing *v1alpha1.LakeFlow) (bool, error) {
	currentGeneration := desired.Generation
	observedGeneration := existing.Status.ObservedGeneration

	logger.Info("Comparing LakeFlow generations for changes",
		"lakeflow", desired.Name,
		"currentGeneration", currentGeneration,
		"observedGeneration", observedGeneration)

	// If generation hasn't been observed yet (initial creation or old resource without observedGeneration),
	// or if generation has increased, we need to reconcile
	if currentGeneration > observedGeneration {
		logger.Info("LakeFlow has unreconciled changes",
			"lakeflow", desired.Name,
			"generationDiff", currentGeneration-observedGeneration)
		return true, nil
	}

	// No generation change. The final "no changes" decision is logged by Process
	// once the Spark PVC label-drift check (which runs next) has also been ruled out.
	return false, nil
}

// hasSparkPVCLabelDrift checks whether the tracked Spark PVC labels on the
// LakeFlow differ from those stamped on the current WorkflowTemplate.
// This catches label-only updates that do not increment metadata.generation.
func (r *reconcilerImpl) hasSparkPVCLabelDrift(ctx context.Context, desired *v1alpha1.LakeFlow, existing *v1alpha1.LakeFlow) (bool, error) {
	templateName := existing.Status.WorkflowTemplate
	if templateName == "" {
		return false, nil
	}

	wt := &argowfv1.WorkflowTemplate{}
	if err := r.resourceManager.Get(ctx, templateName, existing.Namespace, wt); err != nil {
		if errors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to get WorkflowTemplate for label drift check: %w", err)
	}

	if adapter.SparkPVCLabelsChanged(desired.GetLabels(), wt.GetLabels()) {
		logger.Info("Spark PVC label drift detected",
			"lakeflow", desired.Name,
			"template", templateName,
			"desiredLabels", adapter.ExtractTrackedSparkPVCLabels(desired.GetLabels()),
			"templateLabels", adapter.ExtractTrackedSparkPVCLabels(wt.GetLabels()))
		return true, nil
	}
	return false, nil
}

// applyInfra ensures the desired storage objects (object-storage StorageClass/PVC
// and executor local-dir StorageClass) exist before the Argo resources that
// reference them are created. PR4 moved these side effects out of the adapter
// builders; the builders now only describe the objects.
//
// Semantics are a faithful relocation of the previous create-if-not-exists
// behavior:
//   - StorageClass is shared cluster-scoped infra: get-or-create only, never
//     mutated and never owner-referenced from a namespaced LakeFlow.
//   - The OSS PVC is shared across workflows in a namespace, so it is created
//     without an owner reference (owning it would garbage-collect a PVC other
//     workflows still use).
func (r *reconcilerImpl) applyInfra(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	provider := r.provider
	if provider == nil {
		provider = cloud.NewAliyunProvider()
	}
	localDir, err := adapter.SparkLocalDirRequest(lw)
	if err != nil {
		return fmt.Errorf("failed to parse Spark local-dir storage request: %w", err)
	}
	infra, err := provider.DescribeWorkflowStorage(lw, localDir)
	if err != nil {
		return fmt.Errorf("failed to describe workflow storage: %w", err)
	}

	for _, sc := range infra.StorageClasses {
		existing := &storagev1.StorageClass{}
		getErr := r.k8Client.Get(ctx, client.ObjectKey{Name: sc.Name}, existing)
		if getErr == nil {
			continue
		}
		if !errors.IsNotFound(getErr) {
			return fmt.Errorf("failed to get StorageClass %s: %w", sc.Name, getErr)
		}
		if createErr := r.k8Client.Create(ctx, sc); createErr != nil && !errors.IsAlreadyExists(createErr) {
			return fmt.Errorf("failed to create StorageClass %s: %w", sc.Name, createErr)
		}
		logger.Info("Created StorageClass for LakeFlow", "storageClass", sc.Name, "lakeflow", lw.Name)
	}

	for _, pvc := range infra.PVCs {
		existing := &corev1.PersistentVolumeClaim{}
		getErr := r.k8Client.Get(ctx, client.ObjectKey{Name: pvc.Name, Namespace: pvc.Namespace}, existing)
		if getErr == nil {
			continue
		}
		if !errors.IsNotFound(getErr) {
			return fmt.Errorf("failed to get PVC %s/%s: %w", pvc.Namespace, pvc.Name, getErr)
		}
		if createErr := r.k8Client.Create(ctx, pvc); createErr != nil && !errors.IsAlreadyExists(createErr) {
			return fmt.Errorf("failed to create PVC %s/%s: %w", pvc.Namespace, pvc.Name, createErr)
		}
		logger.Info("Created PVC for LakeFlow", "pvc", pvc.Name, "namespace", pvc.Namespace, "lakeflow", lw.Name)
	}

	return nil
}

// preProcessForUpdate fetches existing Argo resources and reconstructs ArgoWorkflowCR
// based on the current status, then updates them with the new spec from LakeFlow.
// This ensures that only resources that should be updated are included in the result.
// It also preserves critical configurations (like PVCSpec) that may have been accidentally
// omitted in the update request (e.g. from frontend partial updates).
//
// Note: Dependency-triggered workflows only update WorkflowTemplate.
// Lake Watcher (external service) handles watching upstream completions and creating Workflow instances.
func (r *reconcilerImpl) preProcessForUpdate(ctx context.Context, lw *v1alpha1.LakeFlow, existingLW *v1alpha1.LakeFlow) (*adapter.ArgoWorkflowCR, error) {
	desiredResources, convertErr := r.workflowConverter.Convert(lw, r.config)
	if convertErr != nil {
		return nil, convertErr
	}

	// Now reconstruct ArgoWorkflowCR based on what actually exists in the cluster
	// We use existingLW.Status (actual cluster state) to determine what resources should be included
	argoResources := &adapter.ArgoWorkflowCR{}

	// WorkflowTemplate always exists and should be updated
	if existingLW.Status.WorkflowTemplate != "" && desiredResources.WorkflowTemplate != nil {
		argoResources.WorkflowTemplate = desiredResources.WorkflowTemplate
		logger.Info("Including WorkflowTemplate for update", "name", existingLW.Status.WorkflowTemplate)
	}

	// CronWorkflow - include whenever desired spec is scheduled.
	// This covers both:
	// 1) schedule -> schedule (update existing CronWorkflow)
	// 2) dependency/immediate -> schedule (create new CronWorkflow)
	if desiredResources.CronWorkflow != nil {
		argoResources.CronWorkflow = desiredResources.CronWorkflow
		if existingLW.Status.CronWorkflow != "" {
			logger.Info("Including existing CronWorkflow for update", "name", existingLW.Status.CronWorkflow)
		} else {
			logger.Info("Including CronWorkflow for creation during trigger migration", "name", desiredResources.CronWorkflow.Name)
		}
	}

	// Workflow - DO NOT include for updates (immediate execution workflows are one-time)
	// Updating the Workflow resource directly causes Argo to immediately re-execute the workflow,
	// which is NOT what we want (we want to wait for user's retry/resubmit action).
	// For "Fix & Retry" scenarios:
	// 1. We update the WorkflowTemplate (below)
	// 2. The rerun manager clears storedTemplates on the failed Workflow
	// 3. The retry uses the updated WorkflowTemplate
	// The reconstructed argoResources never carries the Workflow over from
	// desiredResources, so it is already excluded from GetResources(); we only
	// log here to make the intentional skip visible.
	if existingLW.Status.WorkflowName != "" && desiredResources.Workflow != nil {
		logger.Info("Skipping Workflow for update - immediate execution workflows are not updated to prevent auto-restart",
			"existingWorkflow", existingLW.Status.WorkflowName,
			"reason", "Use retry/resubmit annotations to re-run with updated spec")
	}

	// Note: For dependency-triggered workflows, only WorkflowTemplate is managed by Lakeflow Controller.
	// Lake Watcher (external service) watches LakeFlow status and creates Workflow instances
	// when upstream dependencies are satisfied.

	return argoResources, nil
}

// createResources handles first-time processing of a LakeFlow
func (r *reconcilerImpl) createResources(ctx context.Context, lw *v1alpha1.LakeFlow) (*ReconcileResult, error) {
	// Ensure storage (StorageClass/PVC) exists before the Argo resources that
	// reference it are created.
	if infraErr := r.applyInfra(ctx, lw); infraErr != nil {
		return errorResult(infraErr, "Failed to provision storage for LakeFlow: %v", infraErr), infraErr
	}

	// Convert LakeFlow to Argo resources
	argoResources, convertErr := r.workflowConverter.Convert(lw, r.config)
	if convertErr != nil {
		return errorResult(convertErr, "Failed to convert LakeFlow to Argo resources: %v", convertErr), convertErr
	}

	return r.processor.processResource(ctx, ResourceActionCreated, lw, argoResources, r.processor.createArgoResource)
}

// updateResources handles updates to existing LakeFlow resources
func (r *reconcilerImpl) updateResources(ctx context.Context, lw *v1alpha1.LakeFlow, existingLW *v1alpha1.LakeFlow) (*ReconcileResult, error) {
	// Step 1: Capture old template name from status (before any updates)
	oldTemplateName := existingLW.Status.WorkflowTemplate
	logger.Info("Starting resource update",
		"lakeflow", lw.Name,
		"oldTemplate", oldTemplateName,
		"generation", lw.Generation,
		"observedGeneration", existingLW.Status.ObservedGeneration)

	// Step 1.5: Clean up resources that are no longer desired after trigger-type migration.
	if cleanupErr := r.cleanupObsoleteResources(ctx, lw, existingLW); cleanupErr != nil {
		logger.Error(cleanupErr, "Failed to clean up obsolete resources during trigger migration", "lakeflow", lw.Name)
		return errorResult(cleanupErr, "Failed to clean up obsolete resources: %v", cleanupErr), cleanupErr
	}

	// Step 1.6: Ensure storage (StorageClass/PVC) reflects the (possibly updated)
	// spec before the Argo resources that reference it are updated.
	if infraErr := r.applyInfra(ctx, lw); infraErr != nil {
		logger.Error(infraErr, "Failed to provision storage during update", "lakeflow", lw.Name)
		return errorResult(infraErr, "Failed to provision storage for LakeFlow: %v", infraErr), infraErr
	}

	// Step 2: Use the update-specific preprocessing that filters based on status
	argoResources, convertErr := r.preProcessForUpdate(ctx, lw, existingLW)
	if convertErr != nil {
		logger.Error(convertErr, "Failed to convert LakeFlow for update", "lakeflow", lw.Name)
		return errorResult(convertErr, "Failed to convert LakeFlow to Argo resources: %v", convertErr), convertErr
	}

	// Step 3: Process the resource update (creates new template, updates CronWorkflow)
	result, err := r.processor.processResource(ctx, ResourceActionUpdated, lw, argoResources, r.processor.updateArgoResource)
	if err != nil {
		logger.Error(err, "Failed to process resource update",
			"lakeflow", lw.Name,
			"oldTemplate", oldTemplateName)
		return result, err
	}

	// Step 4: Log template versioning (old templates are NOT deleted to preserve history/audit trail)
	newTemplateName := ""
	if result.ResourceNames != nil {
		if name, ok := result.ResourceNames["WorkflowTemplate"]; ok {
			newTemplateName = name
		}
	}

	if oldTemplateName != "" && newTemplateName != "" && oldTemplateName != newTemplateName {
		logger.Info("WorkflowTemplate version updated (old template preserved for history)",
			"lakeflow", lw.Name,
			"oldTemplate", oldTemplateName,
			"newTemplate", newTemplateName,
			"note", "Old templates are kept for execution history and audit purposes")
	}

	return result, nil
}

func determineTriggerClass(trigger v1alpha1.Trigger) workflowTriggerClass {
	if trigger.Schedule.Cron != "" {
		return triggerClassSchedule
	}
	if len(trigger.Depend.Workflows) > 0 {
		return triggerClassDependency
	}
	return triggerClassImmediate
}

// cleanupObsoleteResources deletes Argo resources that belong to the previous trigger type
// but are no longer needed after a trigger-type migration.
//
// IMPORTANT: We detect the previous trigger type from **status fields**, NOT from
// existingLW.Spec. Both lw and existingLW are fetched from the API server AFTER
// the user's spec update, so their Spec is identical. The status, however, still
// reflects the previous reconciliation cycle:
//   - status.cronWorkflow != "" → was previously scheduled
//   - status.workflowName != "" → was previously immediate/dependency
func (r *reconcilerImpl) cleanupObsoleteResources(ctx context.Context, desiredLW *v1alpha1.LakeFlow, existingLW *v1alpha1.LakeFlow) error {
	newTriggerClass := determineTriggerClass(desiredLW.Spec.WorkflowTrigger)
	wasPreviouslyScheduled := existingLW.Status.CronWorkflow != ""

	// Leaving schedule mode: delete the old CronWorkflow to avoid stale scheduled executions.
	if wasPreviouslyScheduled && newTriggerClass != triggerClassSchedule {
		cronWorkflowName := existingLW.Status.CronWorkflow

		logger.Info("Trigger migration leaves schedule mode, deleting obsolete CronWorkflow",
			"lakeflow", desiredLW.Name,
			"namespace", desiredLW.Namespace,
			"newTriggerClass", newTriggerClass,
			"cronWorkflow", cronWorkflowName)

		cronWf := &argowfv1.CronWorkflow{}
		if err := r.resourceManager.Delete(ctx, cronWorkflowName, desiredLW.Namespace, cronWf); err != nil {
			if errors.IsNotFound(err) {
				logger.Info("Obsolete CronWorkflow already removed, continuing reconciliation",
					"cronWorkflow", cronWorkflowName,
					"lakeflow", desiredLW.Name)
				return nil
			}
			return fmt.Errorf("failed to delete obsolete CronWorkflow %s: %w", cronWorkflowName, err)
		}

		logger.Info("Deleted obsolete CronWorkflow after trigger migration",
			"cronWorkflow", cronWorkflowName,
			"lakeflow", desiredLW.Name)
	}

	return nil
}

// getExistingResourceNames retrieves the names of existing resources for status tracking
func (r *reconcilerImpl) getExistingResourceNames(ctx context.Context, lw *v1alpha1.LakeFlow) map[string]string {
	resourceNames := make(map[string]string)
	// Use the WorkflowName from status if available
	if lw.Status.WorkflowName != "" {
		resourceNames["Workflow"] = lw.Status.WorkflowName
	} else if lw.Status.CronWorkflow != "" {
		resourceNames["CronWorkflow"] = lw.Status.CronWorkflow
	} else if lw.Status.WorkflowTemplate != "" {
		resourceNames["WorkflowTemplate"] = lw.Status.WorkflowTemplate
	}
	return resourceNames
}
