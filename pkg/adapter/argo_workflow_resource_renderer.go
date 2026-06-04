package adapter

import (
	"fmt"
	"strconv"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const (
	// defaultDeleteDelayDuration is the default delay before deleting pods after GC trigger.
	// 24 hours allows sufficient time for log inspection and debugging.
	defaultDeleteDelayDuration = 24 * time.Hour
)

// ArgoResourceRenderer renders the desired Argo resources (WorkflowTemplate,
// Workflow, CronWorkflow) from a LakeFlow's state. It is a per-conversion
// desired-state renderer (not a long-lived fluent builder): all shared state
// lives on its ArgoRenderContext. The previous name ArgoResourceBuilder did not
// reflect this contract.
type ArgoResourceRenderer struct {
	ctx ArgoRenderContext
}

// NewArgoResourceRenderer creates a renderer for the given LakeFlow with a
// default (production) clock. Callers that already hold an ArgoRenderContext
// should construct the renderer directly to share the task index and resolver.
func NewArgoResourceRenderer(lw *v1alpha1.LakeFlow) *ArgoResourceRenderer {
	profile, _ := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	return &ArgoResourceRenderer{ctx: newArgoRenderContext(lw, time.Now, profile)}
}

// WithClock overrides the clock used for versioned template names.
// A nil clock is ignored so the default time.Now is preserved.
func (r *ArgoResourceRenderer) WithClock(now func() time.Time) *ArgoResourceRenderer {
	if now != nil {
		r.ctx.Clock = now
	}
	return r
}

// RenderWorkflowTemplate constructs a WorkflowTemplate with the provided templates
// Uses versioned template name with Unix timestamp for topology versioning
func (r *ArgoResourceRenderer) RenderWorkflowTemplate(entrypoint string, templates []argowfv1.Template) (*argowfv1.WorkflowTemplate, error) {
	lw := r.ctx.LakeFlow
	// Generate versioned template name with timestamp.
	// The clock is injectable (defaults to time.Now) so output is deterministic in tests.
	templateName := GenerateVersionedTemplateNameWithTimestamp(lw.Name, r.ctx.Clock().Unix())
	timestamp := GetTemplateTimestamp(templateName)

	// Add timestamp label for tracking, plus Spark PVC labels for drift detection
	additionalLabels := map[string]string{
		v1alpha1.WorkflowTemplateTimestampLabel: strconv.FormatInt(timestamp, 10),
	}
	additionalLabels = addSparkLocalDirPVCLabels(additionalLabels, lw.GetLabels())

	wfTemplate := &argowfv1.WorkflowTemplate{
		ObjectMeta: r.BuildObjectMeta(templateName, false, additionalLabels),
		Spec: argowfv1.WorkflowSpec{
			Entrypoint: entrypoint,
			Templates:  templates,
		},
	}

	// Apply parallelism if specified
	r.applyParallelism(&wfTemplate.Spec)

	// Extract and apply volumeClaimTemplates for workflow-scoped PVCs
	volumeClaimTemplates := r.extractVolumeClaimTemplates()
	if len(volumeClaimTemplates) > 0 {
		wfTemplate.Spec.VolumeClaimTemplates = volumeClaimTemplates
	}

	// Note: TTL strategy is NOT applied to WorkflowTemplate as it's a reusable template
	// TTL is only applied to workflow instances (Workflow and CronWorkflow)

	return wfTemplate, nil
}

// RenderCronWorkflow applyMemoization injects memoization configuration into templates
//
//	constructs a CronWorkflow that references the given template
func (r *ArgoResourceRenderer) RenderCronWorkflow(templateName string) (*argowfv1.CronWorkflow, error) {
	lw := r.ctx.LakeFlow
	trigger := lw.Spec.WorkflowTrigger.Schedule

	// Build labels that need to be propagated to child workflows.
	// Lake Watcher uses these labels to correlate workflow executions.
	workflowLabels := map[string]string{
		v1alpha1.WorkflowNameLabel: lw.Name,
		v1alpha1.ManagedByLabel:    v1alpha1.ManagerByValue,
	}

	// Default history limits for workflow execution history retention
	// Argo Workflows defaults are: successfulJobsHistoryLimit=3, failedJobsHistoryLimit=1
	// We use 10 to retain more execution history for debugging and auditing
	const defaultJobsHistoryLimit = 10

	cronWf := &argowfv1.CronWorkflow{
		ObjectMeta: r.BuildObjectMeta(lw.Name, false, nil),
		Spec: argowfv1.CronWorkflowSpec{
			Schedules: []string{trigger.Cron},
			Timezone:  trigger.Timezone,
			// Retain execution history for debugging and auditing
			// Without these settings, Argo defaults to keeping only 3 successful and 1 failed workflows
			SuccessfulJobsHistoryLimit: ptr.To(int32(defaultJobsHistoryLimit)),
			FailedJobsHistoryLimit:     ptr.To(int32(defaultJobsHistoryLimit)),
			// WorkflowMetadata propagates metadata to all Workflow instances created by this CronWorkflow.
			WorkflowMetadata: &metav1.ObjectMeta{
				Labels: workflowLabels,
			},
			WorkflowSpec: argowfv1.WorkflowSpec{
				WorkflowTemplateRef: &argowfv1.WorkflowTemplateRef{
					Name: templateName,
				},
			},
		},
	}

	// Apply suspend based on LakeFlow state
	if r.shouldSuspendOnCreation() {
		cronWf.Spec.Suspend = true
	}

	// Apply parallelism if specified
	r.applyParallelism(&cronWf.Spec.WorkflowSpec)

	// Extract and apply volumeClaimTemplates for workflow-scoped PVCs
	volumeClaimTemplates := r.extractVolumeClaimTemplates()
	if len(volumeClaimTemplates) > 0 {
		cronWf.Spec.WorkflowSpec.VolumeClaimTemplates = volumeClaimTemplates
	}

	// Apply concurrency policy from LakeFlow spec or default to Forbid
	// Default to Forbid to prevent resource contention when jobs take longer than schedule interval
	concurrencyPolicy := trigger.ConcurrencyPolicy
	if concurrencyPolicy == "" {
		concurrencyPolicy = "Forbid" // Safe default to prevent overlapping executions
	}

	// Convert string to Argo ConcurrencyPolicy type
	switch concurrencyPolicy {
	case "Allow":
		cronWf.Spec.ConcurrencyPolicy = argowfv1.AllowConcurrent
	case "Forbid":
		cronWf.Spec.ConcurrencyPolicy = argowfv1.ForbidConcurrent
	case "Replace":
		cronWf.Spec.ConcurrencyPolicy = argowfv1.ReplaceConcurrent
	default:
		// Default to Forbid for safety
		cronWf.Spec.ConcurrencyPolicy = argowfv1.ForbidConcurrent
	}

	// Apply common workflow spec configurations
	r.applyWorkflowSpecConfig(&cronWf.Spec.WorkflowSpec)

	// Workflow-level mutex so child Workflow instances serialize on the CronWorkflow name.
	cronWf.Spec.WorkflowSpec.Synchronization = &argowfv1.Synchronization{
		Mutexes: []*argowfv1.Mutex{{Name: cronWf.Name}},
	}

	// Apply stop strategy if specified
	if trigger.StopStrategy != nil {
		stopExpression := r.buildStopStrategyExpression(trigger.StopStrategy)
		if stopExpression != "" {
			cronWf.Spec.StopStrategy = &argowfv1.StopStrategy{
				Expression: stopExpression,
			}
		}
	}

	return cronWf, nil
}

// RenderWorkflow constructs a regular Workflow that references the given template
func (r *ArgoResourceRenderer) RenderWorkflow(templateName string) (*argowfv1.Workflow, error) {
	lw := r.ctx.LakeFlow
	workflow := &argowfv1.Workflow{
		ObjectMeta: r.BuildObjectMeta(fmt.Sprintf("%s-", lw.Name), true, nil),
		Spec: argowfv1.WorkflowSpec{
			WorkflowTemplateRef: &argowfv1.WorkflowTemplateRef{
				Name: templateName,
			},
		},
	}

	// Apply suspend based on LakeFlow state
	if r.shouldSuspendOnCreation() {
		suspend := true
		workflow.Spec.Suspend = &suspend
	}

	// Apply parallelism if specified
	r.applyParallelism(&workflow.Spec)

	// Extract and apply volumeClaimTemplates for workflow-scoped PVCs
	volumeClaimTemplates := r.extractVolumeClaimTemplates()
	if len(volumeClaimTemplates) > 0 {
		workflow.Spec.VolumeClaimTemplates = volumeClaimTemplates
	}

	// Apply common workflow spec configurations
	r.applyWorkflowSpecConfig(&workflow.Spec)

	return workflow, nil
}

// BuildObjectMeta creates ObjectMeta with common labels, names, and owner references
func (r *ArgoResourceRenderer) BuildObjectMeta(nameOrPrefix string, useGenerateName bool, additionalLabels map[string]string) metav1.ObjectMeta {
	lw := r.ctx.LakeFlow
	labels := map[string]string{
		v1alpha1.WorkflowNameLabel: lw.Name,
		v1alpha1.ManagedByLabel:    v1alpha1.ManagerByValue,
	}

	// Add control state label for state tracking (used for sensor lifecycle management)
	// This label enables visibility of the workflow's control state on all Argo resources
	state := lw.Spec.State
	if state == "" {
		state = v1alpha1.ControlStateActive // Default to Active if not specified
	}
	labels[v1alpha1.WorkflowControlStateLabel] = string(state)

	// Add additional labels if provided
	for key, value := range additionalLabels {
		labels[key] = value
	}

	objMeta := metav1.ObjectMeta{
		Namespace:       lw.Namespace,
		Labels:          labels,
		OwnerReferences: r.buildOwnerReferences(),
	}

	if useGenerateName {
		objMeta.GenerateName = nameOrPrefix
	} else {
		objMeta.Name = nameOrPrefix
	}

	return objMeta
}

// BuildTaskObjectMeta creates ObjectMeta with task-specific labels including both WorkflowNameLabel and WorkflowTaskNameLabel
// This ensures consistent labeling across all executor types for querying purposes
func (r *ArgoResourceRenderer) BuildTaskObjectMeta(nameOrPrefix string, useGenerateName bool, taskName string, additionalLabels map[string]string) metav1.ObjectMeta {
	lw := r.ctx.LakeFlow
	labels := map[string]string{
		v1alpha1.WorkflowNameLabel:     lw.Name,
		v1alpha1.WorkflowTaskNameLabel: taskName,
		v1alpha1.ManagedByLabel:        v1alpha1.ManagerByValue,
	}

	// Add additional labels if provided
	for key, value := range additionalLabels {
		labels[key] = value
	}

	objMeta := metav1.ObjectMeta{
		Namespace:       lw.Namespace,
		Labels:          labels,
		OwnerReferences: r.buildOwnerReferences(),
	}

	if useGenerateName {
		objMeta.GenerateName = nameOrPrefix
	} else {
		objMeta.Name = nameOrPrefix
	}

	return objMeta
}

// applyParallelism applies parallelism settings to WorkflowSpec if specified
func (r *ArgoResourceRenderer) applyParallelism(spec *argowfv1.WorkflowSpec) {
	if r.ctx.LakeFlow.Spec.Parallelism > 0 {
		parallelism := int64(r.ctx.LakeFlow.Spec.Parallelism)
		spec.Parallelism = &parallelism
	}
}

// applyWorkflowSpecConfig applies common WorkflowSpec configurations like TTL, PodGC, and service account
func (r *ArgoResourceRenderer) applyWorkflowSpecConfig(spec *argowfv1.WorkflowSpec) {
	// Apply TTL strategy for workflow object lifecycle
	// nil means workflows are preserved indefinitely (recommended for sensor-based workflows)
	spec.TTLStrategy = r.buildTTLStrategy()

	// Apply PodGC strategy for pod cleanup
	// This ensures pods are deleted to free resources while preserving workflow history
	spec.PodGC = r.buildPodGCStrategy()

	// Apply service account name if all tasks use the same service account
	commonServiceAccount := r.GetCommonServiceAccount()
	if commonServiceAccount != "" {
		spec.ServiceAccountName = commonServiceAccount
	}
}

// buildOwnerReferences builds owner references only if the LakeFlow has a valid UID
// This ensures compatibility with testing scenarios where UID may not be available
func (r *ArgoResourceRenderer) buildOwnerReferences() []metav1.OwnerReference {
	lw := r.ctx.LakeFlow
	// Only create owner references if UID is available and not empty
	if lw.UID != "" {
		return []metav1.OwnerReference{
			{
				APIVersion: lw.APIVersion,
				Kind:       lw.Kind,
				Name:       lw.Name,
				UID:        lw.UID,
			},
		}
	}
	// Return empty slice if UID is not available
	return []metav1.OwnerReference{}
}

func (r *ArgoResourceRenderer) GetCommonServiceAccount() string {
	return r.ctx.LakeFlow.Spec.ServiceAccountName
}

// shouldSuspendOnCreation determines if the workflow should be created in suspended state
// based on the LakeFlow's desired state
func (r *ArgoResourceRenderer) shouldSuspendOnCreation() bool {
	// Get current desired state from spec (default to Active if not set)
	desiredState := r.ctx.LakeFlow.Spec.State
	if desiredState == "" {
		desiredState = v1alpha1.ControlStateActive
	}

	// Only suspend on creation if explicitly requested
	return desiredState == v1alpha1.ControlStateSuspend
}

// buildTTLStrategy builds Argo TTL strategy for WORKFLOW OBJECT deletion.
// NEW BEHAVIOR: Returns nil by default to preserve workflow history (critical for cross-workflow dependencies).
// Pods are cleaned up separately via PodGC strategy.
// Only applies TTL if user explicitly sets non-zero values in TTLStrategy.
func (r *ArgoResourceRenderer) buildTTLStrategy() *argowfv1.TTLStrategy {
	// If user explicitly set TTL values, respect them
	if r.ctx.LakeFlow.Spec.TTLStrategy != nil {
		lakeTTL := r.ctx.LakeFlow.Spec.TTLStrategy

		// Check if user wants to disable TTL (all zeros)
		if lakeTTL.SecondsAfterCompletion == 0 &&
			lakeTTL.SecondsAfterSuccess == 0 &&
			lakeTTL.SecondsAfterFailure == 0 {
			// User explicitly disabled TTL - return nil to preserve workflow history
			return nil
		}

		// User specified non-zero TTL values, apply them
		argoTTL := &argowfv1.TTLStrategy{}
		if lakeTTL.SecondsAfterCompletion != 0 {
			value := int32(lakeTTL.SecondsAfterCompletion)
			argoTTL.SecondsAfterCompletion = &value
		}
		if lakeTTL.SecondsAfterSuccess != 0 {
			value := int32(lakeTTL.SecondsAfterSuccess)
			argoTTL.SecondsAfterSuccess = &value
		}
		if lakeTTL.SecondsAfterFailure != 0 {
			value := int32(lakeTTL.SecondsAfterFailure)
			argoTTL.SecondsAfterFailure = &value
		}
		return argoTTL
	}

	// NEW DEFAULT BEHAVIOR: nil TTL (preserve workflow history indefinitely)
	// This is critical for:
	// - Cross-workflow dependencies evaluated by Lake Watcher
	// - Audit trails and debugging
	// - Rerun/retry operations
	// Pods are cleaned up via PodGC strategy instead
	return nil
}

// buildPodGCStrategy builds Argo PodGC strategy from LakeFlow PodGC spec.
// Pod Garbage Collection (PodGC) controls when workflow pods are deleted to free cluster resources.
// This is separate from TTL which controls workflow object deletion.
// Returns a default strategy if not specified to ensure pods are cleaned up.
func (r *ArgoResourceRenderer) buildPodGCStrategy() *argowfv1.PodGC {
	// If user explicitly specified PodGC, use their configuration
	if r.ctx.LakeFlow.Spec.PodGC != nil {
		podGC := &argowfv1.PodGC{
			Strategy: argowfv1.PodGCStrategy(r.ctx.LakeFlow.Spec.PodGC.Strategy),
		}
		// Apply delete delay if specified, otherwise use default 24 hours
		if r.ctx.LakeFlow.Spec.PodGC.DeleteDelayDuration != nil {
			podGC.DeleteDelayDuration = r.ctx.LakeFlow.Spec.PodGC.DeleteDelayDuration.Duration.String()
		} else {
			podGC.DeleteDelayDuration = defaultDeleteDelayDuration.String()
		}
		return podGC
	}

	// Default behavior: clean up pods when workflow succeeds with 24-hour delay
	return &argowfv1.PodGC{
		Strategy:            argowfv1.PodGCOnWorkflowSuccess,
		DeleteDelayDuration: defaultDeleteDelayDuration.String(),
	}
}

// buildStopStrategyExpression converts LakeFlow StopStrategy to Argo CronWorkflow expression
func (r *ArgoResourceRenderer) buildStopStrategyExpression(stopStrategy *v1alpha1.StopStrategy) string {
	if stopStrategy == nil {
		return ""
	}
	switch stopStrategy.Phase {
	case "success":
		return fmt.Sprintf("cronworkflow.succeeded >= %d", stopStrategy.Count)
	case "failure":
		return fmt.Sprintf("cronworkflow.failed >= %d", stopStrategy.Count)
	case "complete":
		return fmt.Sprintf("(cronworkflow.succeeded + cronworkflow.failed) >= %d", stopStrategy.Count)
	default:
		logger.Error(fmt.Errorf("unsupported stop strategy phase: %s", stopStrategy.Phase),
			"Invalid stop strategy phase", "phase", stopStrategy.Phase)
		return ""
	}
}

// extractVolumeClaimTemplates scans all tasks for CommandExecutorSpec with PVCSpec
// and generates Argo volumeClaimTemplates for per-task PVCs.
// These PVCs are created by Argo when the workflow starts and deleted when the workflow is deleted.
//
// IMPORTANT: Each task gets its own independent PVC to support parallel execution on different nodes.
// Using a single shared PVC with ReadWriteOnce would cause Multi-Attach errors when parallel tasks
// are scheduled on different nodes (block storage like ESSD can only be mounted by one node at a time).
//
// Volume naming convention:
//   - Volume name: "workdir-{taskName}" (unique per task)
//   - Mount path: "/data" (same for all tasks - consistent application interface)
func (r *ArgoResourceRenderer) extractVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	var templates []corev1.PersistentVolumeClaim

	lw := r.ctx.LakeFlow
	for _, task := range lw.Spec.Tasks {
		// Only process command executor tasks with PVC-backed block storage
		if task.TaskSpec.CommandExecutorSpec == nil || task.TaskSpec.CommandExecutorSpec.BlockStorage == nil {
			continue
		}

		blockStorage := task.TaskSpec.CommandExecutorSpec.BlockStorage
		storageQuantity := blockStorage.Size

		// Per-task volume name to avoid Multi-Attach errors when parallel tasks run on different nodes
		// Each task gets its own PVC: "workdir-{taskName}"
		// All tasks mount to the same path "/data" for consistent application interface
		volumeName := GetWorkdirVolumeName(task.Name)

		// Create PersistentVolumeClaim template for Argo
		// Argo will create the actual PVC when the workflow starts and set OwnerReference
		templates = append(templates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: volumeName,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce, // RWO is safe now since each task has its own PVC
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storageQuantity,
					},
				},
				StorageClassName: &blockStorage.StorageClass,
			},
		})

		logger.Info("Generated volumeClaimTemplate for per-task PVC",
			"volumeName", volumeName,
			"taskName", task.Name,
			"storageClass", blockStorage.StorageClass,
			"storageSize", blockStorage.Size.String(),
			"workflow", lw.Name)
	}

	return templates
}

// GetWorkdirVolumeName returns the per-task workdir volume name
// This function is exported so task_renderer.go can use the same naming convention
func GetWorkdirVolumeName(taskName string) string {
	return fmt.Sprintf("workdir-%s", taskName)
}
