package reconciler

import (
	"context"
	"fmt"
	"sort"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const maxWorkflowTemplateCount = 3 // only include non-rerun template

// resourceProcessor handles CRUD operations for Argo resources
type resourceProcessor struct {
	resourceManager argoclient.ArgoResourceClient
	validator       *resourceValidator
	comparator      *resourceComparator
}

// newResourceProcessor creates a new resourceProcessor
func newResourceProcessor(
	resourceManager argoclient.ArgoResourceClient,
	validator *resourceValidator,
	comparator *resourceComparator,
) *resourceProcessor {
	return &resourceProcessor{
		resourceManager: resourceManager,
		validator:       validator,
		comparator:      comparator,
	}
}

// processResource iterates over all Argo resources and applies the given process function
func (p *resourceProcessor) processResource(ctx context.Context, action ResourceAction, lw *v1alpha1.LakeFlow, argoResources *adapter.ArgoWorkflowCR, process ArgoResourceProcess) (*ReconcileResult, error) {
	argoResourceList := argoResources.GetResources()
	result := &ReconcileResult{
		Action:        ReconcileActionNoChange,
		ResourceNames: make(map[string]string),
	}
	hasCreated := false
	hasUpdated := false
	for _, argoRs := range argoResourceList {
		// Process the resource (create or update)
		processRs, processErr := process(ctx, lw, argoRs)
		if processErr != nil {
			return processRs, processErr
		}

		resourceType := common.GetArgoResourceType(argoRs)
		resourceName := common.GetResourceName(argoRs)

		// If validation returned NoChange (resource already exists) and the resource
		// has GenerateName but no Name assigned, skip adding it to ResourceNames.
		// This prevents "<unnamed>" from appearing in status.
		// The actual resource name should already be in status (set during initial creation).
		if processRs != nil && processRs.Action == ReconcileActionNoChange {
			logger.Info("Resource already exists, skipping ResourceNames update",
				"type", resourceType,
				"lakeflow", lw.Name)
			continue
		}

		result.ResourceNames[resourceType] = resourceName
		// Track overall actions
		switch action {
		case ResourceActionCreated:
			hasCreated = true
		case ResourceActionUpdated:
			hasUpdated = true
		}

		logger.Info("Resource reconciled", "type", resourceType, "name", resourceName, "action", action)
	}
	if hasCreated && hasUpdated {
		result.Action = ReconcileActionCreated // Treat mixed as created
		result.Message = fmt.Sprintf("%s and updated Argo resources", action)
	} else if hasCreated {
		result.Action = ReconcileActionCreated
		result.Message = fmt.Sprintf("%s Argo resources", action)
	} else if hasUpdated {
		result.Action = ReconcileActionUpdated
		result.Message = fmt.Sprintf("%s Argo resources", action)
	} else {
		result.Action = ReconcileActionNoChange
		result.Message = "All Argo resources are up to date"
	}

	logger.Info("Reconciliation completed", "Name", lw.Name, "Action", result.Action, "ResourceNames", result.ResourceNames)
	return result, nil
}

// createArgoResource creates a new resource with pre-creation validation
func (p *resourceProcessor) createArgoResource(ctx context.Context, lw *v1alpha1.LakeFlow, resource client.Object) (*ReconcileResult, error) {
	resourceType := common.GetArgoResourceType(resource)
	resourceName := common.GetResourceName(resource)

	// Pre-creation validation - check if resource is ready to be created
	if validationResult := p.validator.validateResourceForCreation(ctx, lw, resource); validationResult != nil {
		return validationResult, validationResult.Error
	}

	// Check if this is a Workflow with WorkflowTemplateRef - needs retry logic
	// to handle Argo informer cache lag (template not found errors)
	if workflow, ok := resource.(*argowfv1.Workflow); ok && workflow.Spec.WorkflowTemplateRef != nil {
		createdName, err := argoclient.CreateWorkflowWithRetry(ctx, p.resourceManager, workflow, lw.Namespace)
		if err != nil {
			return p.handleCreationError(err, resourceType, resourceName)
		}
		logger.Info("Successfully created workflow with retry logic", "type", resourceType, "name", createdName)
		return &ReconcileResult{
			Action: ReconcileActionCreated,
		}, nil
	}

	// Normal creation for other resources (WorkflowTemplate, CronWorkflow, Workflow without TemplateRef)
	if err := p.resourceManager.Create(ctx, resource); err != nil {
		return p.handleCreationError(err, resourceType, resourceName)
	}

	// Limit WorkflowTemplate count
	if _, ok := resource.(*argowfv1.WorkflowTemplate); ok {
		p.workflowTemplateGC(ctx, lw)
	}

	logger.Info("Successfully created resource", "type", resourceType, "name", resourceName)
	return &ReconcileResult{
		Action: ReconcileActionCreated,
	}, nil
}

// updateArgoResource updates an existing resource, or creates it if it doesn't exist
func (p *resourceProcessor) updateArgoResource(ctx context.Context, lw *v1alpha1.LakeFlow, desired client.Object) (*ReconcileResult, error) {
	resourceType := common.GetArgoResourceType(desired)
	resourceName := common.GetResourceName(desired)

	logger.Info("Updating resource", "type", resourceType, "name", resourceName)
	// Try to get the existing resource first
	existing, err := p.getExistingResource(ctx, desired)
	if err != nil {
		if errors.IsNotFound(err) {
			// Resource doesn't exist, create it
			logger.Info("Resource not found, creating it", "type", resourceType, "name", resourceName)
			return p.createArgoResource(ctx, lw, desired)
		}
		return &ReconcileResult{
			Error: err,
		}, fmt.Errorf("failed to get existing resource %s %s: %w", resourceType, resourceName, err)
	}

	// Resource exists, check if it needs to be updated
	hasChanged, err := p.comparator.hasResourceChanged(existing, desired)
	if err != nil {
		return &ReconcileResult{
			Error: err,
		}, fmt.Errorf("failed to compare resource changes for %s %s: %w", resourceType, resourceName, err)
	}

	if !hasChanged {
		logger.Info("Resource is up to date", "type", resourceType, "name", resourceName)
		return &ReconcileResult{Action: ReconcileActionNoChange}, nil
	}

	// Copy resourceVersion from existing to desired (required for Kubernetes update)
	// This is essential for optimistic concurrency control
	desired.SetResourceVersion(existing.GetResourceVersion())

	logger.Info("Updating resource", "type", resourceType, "name", resourceName,
		"resourceVersion", existing.GetResourceVersion())
	// Update the existing resource
	if updateErr := p.resourceManager.Update(ctx, desired); updateErr != nil {
		logger.Error(updateErr, "Failed to update resource", "type", resourceType, "name", resourceName)
		return &ReconcileResult{
			Error: updateErr,
		}, fmt.Errorf("failed to update %s %s: %w", resourceType, resourceName, updateErr)
	}

	logger.Info("Successfully updated resource", "type", resourceType, "name", resourceName)
	return &ReconcileResult{
		Action: ReconcileActionUpdated,
	}, nil
}

// getExistingResource retrieves the existing resource from the cluster
func (p *resourceProcessor) getExistingResource(ctx context.Context, resource client.Object) (client.Object, error) {
	existing, err := newEmptyLike(resource)
	if err != nil {
		return nil, err
	}

	key := client.ObjectKeyFromObject(resource)
	getErr := p.resourceManager.Get(ctx, key.Name, key.Namespace, existing)
	return existing, getErr
}

// newEmptyLike returns a freshly allocated, empty Argo resource of the same
// concrete type as obj. It centralizes the only "construct an empty of this
// type" switch in the reconciler so getExistingResource stays a pure fetch.
// Note: this intentionally does NOT try to subsume the validation, idempotency,
// or ordering switches elsewhere, which encode distinct policy.
func newEmptyLike(obj client.Object) (client.Object, error) {
	switch obj.(type) {
	case *argowfv1.Workflow:
		return &argowfv1.Workflow{}, nil
	case *argowfv1.WorkflowTemplate:
		return &argowfv1.WorkflowTemplate{}, nil
	case *argowfv1.CronWorkflow:
		return &argowfv1.CronWorkflow{}, nil
	default:
		return nil, fmt.Errorf("unsupported resource type: %T", obj)
	}
}

// handleCreationError handles errors that occur during resource creation
func (p *resourceProcessor) handleCreationError(err error, resourceType, resourceName string) (*ReconcileResult, error) {
	// Handle "already exists" errors gracefully for idempotent resources
	if errors.IsAlreadyExists(err) {
		// For certain resource types, "already exists" is not an error (idempotent behavior)
		switch resourceType {
		case "WorkflowTemplate", "CronWorkflow":
			logger.Info("Resource already exists, treating as success", "type", resourceType, "name", resourceName)
			return &ReconcileResult{
				Action: ReconcileActionNoChange,
			}, nil
		case "Workflow":
			// Workflows should be unique, so "already exists" is a real error
			logger.Error(err, "Workflow already exists - this shouldn't happen", "type", resourceType, "name", resourceName)
			return &ReconcileResult{
				Error:   err,
				Message: fmt.Sprintf("Workflow %s already exists", resourceName),
			}, fmt.Errorf("workflow already exists: %w", err)
		}
	}

	logger.Error(err, "Failed to create resource", "type", resourceType, "name", resourceName)
	return &ReconcileResult{
		Error:   err,
		Message: fmt.Sprintf("Failed to create resource %s", resourceName),
	}, fmt.Errorf("failed to create %w", err)
}

func (p *resourceProcessor) workflowTemplateGC(ctx context.Context, lw *v1alpha1.LakeFlow) {
	logger.Info("Starting to gc workflowTemplate",
		"lakeWorkflow", lw.Name, "maxKept", maxWorkflowTemplateCount)
	list := &argowfv1.WorkflowTemplateList{}
	if err := p.resourceManager.List(ctx, lw.Namespace, list,
		client.MatchingLabels{
			v1alpha1.WorkflowNameLabel: lw.Name,
			v1alpha1.ManagedByLabel:    v1alpha1.ManagerByValue,
		},
	); err != nil {
		logger.Error(err, "Failed to list WorkflowTemplates for gc", "lakeWorkflow", lw.Name)
		return
	}

	var templates []*argowfv1.WorkflowTemplate
	for i := range list.Items {
		wt := &list.Items[i]
		if isRerunWorkflowTemplate(wt) {
			continue
		}
		templates = append(templates, wt)
	}

	sort.Slice(templates, func(i, j int) bool {
		ti := adapter.GetTemplateTimestamp(templates[i].Name)
		tj := adapter.GetTemplateTimestamp(templates[j].Name)
		if ti != tj {
			return ti > tj
		}
		return templates[i].CreationTimestamp.After(templates[j].CreationTimestamp.Time)
	})

	if len(templates) <= maxWorkflowTemplateCount {
		return
	}

	for _, wt := range templates[maxWorkflowTemplateCount:] {
		if err := p.resourceManager.Delete(ctx, wt.Name, lw.Namespace, &argowfv1.WorkflowTemplate{}); err != nil {
			logger.Error(err, "Failed to delete WorkflowTemplate",
				"name", wt.Name, "lakeWorkflow", lw.Name)
			continue
		}
		logger.Info("Delete WorkflowTemplate for GC",
			"name", wt.Name, "lakeWorkflow", lw.Name, "timestamp", adapter.GetTemplateTimestamp(wt.Name))
	}
	logger.Info("WorkflowTemplate GC completed",
		"lakeWorkflow", lw.Name, "maxKept", maxWorkflowTemplateCount)
}

func isRerunWorkflowTemplate(template *argowfv1.WorkflowTemplate) bool {
	labels := template.GetLabels()
	if labels == nil {
		return false
	}
	return labels[v1alpha1.IsRetryWorkflowLabel] == "true" || labels[v1alpha1.IsResubmitWorkflowLabel] == "true"
}
