package reconciler

import (
	"context"
	"fmt"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// resourceValidator performs pre-creation validation for Argo resources
type resourceValidator struct {
	resourceManager argoclient.ArgoResourceClient
}

// newResourceValidator creates a new resourceValidator
func newResourceValidator(resourceManager argoclient.ArgoResourceClient) *resourceValidator {
	return &resourceValidator{
		resourceManager: resourceManager,
	}
}

// validateResourceForCreation performs all pre-creation checks for a resource.
// Returns a ReconcileResult if validation fails (resource not ready), or nil if validation passes.
func (v *resourceValidator) validateResourceForCreation(ctx context.Context, lw *v1alpha1.LakeFlow, resource client.Object) *ReconcileResult {
	switch res := resource.(type) {
	case *argowfv1.Workflow:
		return v.validateWorkflowForCreation(ctx, lw, res)
	case *argowfv1.CronWorkflow:
		return v.validateCronWorkflowForCreation(ctx, lw, res)
	default:
		// No special validation needed for other resource types
		return nil
	}
}

// validateWorkflowForCreation checks if a Workflow is ready to be created
func (v *resourceValidator) validateWorkflowForCreation(ctx context.Context, lw *v1alpha1.LakeFlow, workflow *argowfv1.Workflow) *ReconcileResult {
	// For immediate execution workflows, check if we've already created one
	if lw.Status.WorkflowName != "" {
		logger.Info("Workflow already exists for immediate execution",
			"lakeflow", lw.Name, "existing-workflow", lw.Status.WorkflowName)
		return &ReconcileResult{
			Action: ReconcileActionNoChange,
		}
	}

	// Wait for WorkflowTemplate to be ready if referenced
	if workflow.Spec.WorkflowTemplateRef != nil {
		return v.validateWorkflowTemplateRef(ctx, lw, workflow.Spec.WorkflowTemplateRef.Name, "Workflow")
	}

	return nil
}

// validateCronWorkflowForCreation checks if a CronWorkflow is ready to be created
func (v *resourceValidator) validateCronWorkflowForCreation(ctx context.Context, lw *v1alpha1.LakeFlow, cronWorkflow *argowfv1.CronWorkflow) *ReconcileResult {
	// Wait for WorkflowTemplate to be ready if referenced
	if cronWorkflow.Spec.WorkflowSpec.WorkflowTemplateRef != nil {
		return v.validateWorkflowTemplateRef(ctx, lw, cronWorkflow.Spec.WorkflowSpec.WorkflowTemplateRef.Name, "CronWorkflow")
	}

	return nil
}

// validateWorkflowTemplateRef checks if a WorkflowTemplate is ready to be referenced
func (v *resourceValidator) validateWorkflowTemplateRef(ctx context.Context, lw *v1alpha1.LakeFlow, templateName, resourceType string) *ReconcileResult {
	if !v.isWorkflowTemplateReady(ctx, templateName, lw.Namespace) {
		logger.Info("WorkflowTemplate not ready yet, deferring resource creation",
			"lakeflow", lw.Name, "resourceType", resourceType, "template", templateName)
		return errorResult(
			fmt.Errorf("WorkflowTemplate %s not ready", templateName),
			"WorkflowTemplate %s not ready, retrying later", templateName,
		)
	}
	return nil
}

// isWorkflowTemplateReady checks if the WorkflowTemplate exists and is ready to be referenced.
// It uses exponential backoff with a deadline to wait for the template to be visible in the API server.
// This prevents race conditions where CronWorkflow validation fails because the Argo controller's
// informer cache hasn't synced the WorkflowTemplate yet.
func (v *resourceValidator) isWorkflowTemplateReady(ctx context.Context, templateName, namespace string) bool {
	return common.IsWorkflowTemplateReady(ctx, v.resourceManager, templateName, namespace)
}
