package adapter

import (
	"fmt"
	"time"

	"k8s.io/client-go/rest"

	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DelayDeleteTTLWhenFailed  = 7 * 24 * time.Hour // 7 days - default TTL after failed completion
	DelayDeleteTTLWhenSuccess = 24 * time.Hour     // 1 day (86400 seconds) - default TTL after successful completion
)

var (
	logger     = common.GetSharedLogger().WithName("workflow-adapter")
	successTTL = int32(DelayDeleteTTLWhenSuccess.Seconds())
	factory    = GetTaskRendererFactory()
)

type ArgoWorkflowCR struct {
	WorkflowTemplate *argowfv1.WorkflowTemplate
	Workflow         *argowfv1.Workflow
	CronWorkflow     *argowfv1.CronWorkflow
}

// GetResources returns all non-nil resources as a slice of client.Object
func (cr *ArgoWorkflowCR) GetResources() []client.Object {
	var resources []client.Object

	if cr.WorkflowTemplate != nil {
		resources = append(resources, cr.WorkflowTemplate)
	}
	if cr.Workflow != nil {
		resources = append(resources, cr.Workflow)
	}
	if cr.CronWorkflow != nil {
		resources = append(resources, cr.CronWorkflow)
	}

	return resources
}

// WorkflowConverter converts LakeFlow to appropriate Argo resources
type WorkflowConverter interface {
	// Convert LakeFlow to appropriate Argo resource based on trigger type
	// The implementation decides whether to create CronWorkflow, Workflow, or WorkflowTemplate
	Convert(lw *v1alpha1.LakeFlow, config *rest.Config) (*ArgoWorkflowCR, error)
}

// workflowConverterImpl implements WorkflowConverter interface
type workflowConverterImpl struct {
	profile cloudprofile.CloudProfile
	// now is the clock threaded into the ArgoRenderContext for versioned template
	// names. It defaults to time.Now and is overridden in tests for determinism.
	now func() time.Time
}

// NewWorkflowConverter creates a new workflow converter
func NewWorkflowConverter(profile cloudprofile.CloudProfile) WorkflowConverter {
	return NewWorkflowConverterWithClock(profile, time.Now)
}

// NewWorkflowConverterWithClock creates a workflow converter with an injected
// clock for deterministic tests.
func NewWorkflowConverterWithClock(profile cloudprofile.CloudProfile, now func() time.Time) WorkflowConverter {
	if profile.Name == "" {
		profile, _ = cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	}
	if now == nil {
		now = time.Now
	}
	return &workflowConverterImpl{profile: profile, now: now}
}

// Convert LakeFlow to appropriate Argo resources based on trigger type
// Note: Dependency-triggered workflows only create WorkflowTemplate.
// Lake Watcher (external service) handles watching upstream completions and creating Workflow instances.
func (c *workflowConverterImpl) Convert(lw *v1alpha1.LakeFlow, config *rest.Config) (*ArgoWorkflowCR, error) {
	// Build the shared render context once for this conversion (task index +
	// credential resolver + injectable clock) and a renderer that shares it.
	renderCtx := newArgoRenderContext(lw, c.now, c.profile)
	renderer := &ArgoResourceRenderer{ctx: renderCtx}

	// Always create WorkflowTemplate first (contains task definitions)
	workflowTemplate, convertWfTemplateErr := c.convertToWorkflowTemplate(&renderCtx, renderer)
	if convertWfTemplateErr != nil {
		logger.Error(convertWfTemplateErr, "Failed to create workflow template", "lakeflow", lw.Name, "namespace", lw.Namespace)
		return nil, fmt.Errorf("failed to create workflow template: %w", convertWfTemplateErr)
	}

	argoResources := &ArgoWorkflowCR{
		WorkflowTemplate: workflowTemplate,
	}
	trigger := lw.Spec.WorkflowTrigger
	if trigger.Schedule.Cron != "" {
		// Schedule trigger -> CronWorkflow
		cronWorkflow, convertCronErr := renderer.RenderCronWorkflow(workflowTemplate.Name)
		if convertCronErr != nil {
			logger.Error(convertCronErr, "Failed to create cron workflow", "Namespace", lw.Namespace, "Name", lw.Name)
			return nil, fmt.Errorf("failed to create cron workflow: %w", convertCronErr)
		}
		argoResources.CronWorkflow = cronWorkflow
	} else if len(trigger.Depend.Workflows) > 0 {
		// Dependency trigger -> Only WorkflowTemplate (Lake Watcher handles triggering)
		// Lake Watcher watches LakeFlow status changes and creates Workflow instances
		// when all upstream dependencies are satisfied.
		logger.Info("Dependency-triggered workflow - WorkflowTemplate only (Lake Watcher handles triggering)",
			"Namespace", lw.Namespace, "Name", lw.Name,
			"upstreamCount", len(trigger.Depend.Workflows))
	} else {
		// Immediate execution -> regular Workflow
		workflow, convertWfErr := renderer.RenderWorkflow(workflowTemplate.Name)
		if convertWfErr != nil {
			logger.Error(convertWfErr, "Failed to create workflow", "lakeflow", lw.Name, "namespace", lw.Namespace)
			return nil, fmt.Errorf("failed to create workflow: %w", convertWfErr)
		}
		argoResources.Workflow = workflow
	}
	logger.Info("Created workflow CR", "Namespace", lw.Namespace, "Name", lw.Name)
	return argoResources, nil
}

// convertToWorkflowTemplate converts LakeFlow tasks to Argo WorkflowTemplate
func (c *workflowConverterImpl) convertToWorkflowTemplate(ctx *ArgoRenderContext, renderer *ArgoResourceRenderer) (*argowfv1.WorkflowTemplate, error) {
	lw := ctx.LakeFlow
	// Create the main DAG template with a workflow-specific name
	entrypointTemplateName := fmt.Sprintf("%s-main", lw.Name)
	dagTemplate := &argowfv1.Template{
		Name: entrypointTemplateName,
		DAG: &argowfv1.DAGTemplate{
			Tasks: []argowfv1.DAGTask{},
		},
	}

	// Convert each LakeFlow task to Argo DAG task
	var templates []argowfv1.Template
	for i := range lw.Spec.Tasks {
		task := &lw.Spec.Tasks[i]
		// Create task template based on an executor type
		taskTemplate, err := c.convertTask(ctx, task)
		if err != nil {
			logger.Error(err, "Failed to convert task", "task", task.Name, "lakeflow", lw.Name, "namespace", lw.Namespace)
			return nil, fmt.Errorf("failed to convert task %s: %w", task.Name, err)
		}
		templates = append(templates, *taskTemplate)

		// Add task to DAG
		dagTask := argowfv1.DAGTask{
			Name:     task.Name,
			Template: task.Name,
		}
		// Pass template input parameters explicitly to the DAG task
		// This is required for memoization to work correctly, as the cache key
		// needs to resolve parameters before execution starts
		if len(taskTemplate.Inputs.Parameters) > 0 {
			dagTask.Arguments = argowfv1.Arguments{
				Parameters: []argowfv1.Parameter{},
			}
			for _, param := range taskTemplate.Inputs.Parameters {
				dagTask.Arguments.Parameters = append(dagTask.Arguments.Parameters, argowfv1.Parameter{
					Name:  param.Name,
					Value: param.Value,
				})
			}
		}

		// Only set dependencies if they exist
		if len(task.DependsOn) > 0 {
			dagTask.Dependencies = task.DependsOn
		}
		dagTemplate.DAG.Tasks = append(dagTemplate.DAG.Tasks, dagTask)
	}

	// Add main template
	templates = append([]argowfv1.Template{*dagTemplate}, templates...)

	// Use the renderer to create the WorkflowTemplate with the workflow-specific entrypoint
	return renderer.RenderWorkflowTemplate(entrypointTemplateName, templates)
}

// convertTask converts a LakeFlow task to an Argo template using the
// TaskRendererFactory strategy. The already-resolved task and shared render
// context are passed through so the renderer does not re-scan Spec.Tasks.
func (c *workflowConverterImpl) convertTask(ctx *ArgoRenderContext, task *v1alpha1.Task) (*argowfv1.Template, error) {
	lw := ctx.LakeFlow
	// Get the appropriate renderer for the executor type
	renderer, err := factory.CreateRenderer(task.Executor)
	if err != nil {
		logger.Error(err, "Failed to create renderer", "executor", task.Executor, "task", task.Name, "lakeflow", lw.Name, "namespace", lw.Namespace)
		return nil, fmt.Errorf("failed to create renderer for executor %s: %w", task.Executor, err)
	}

	template, err := renderer.RenderTask(ctx, task)
	if err != nil {
		logger.Error(err, "Failed to build template for task", "task", task.Name, "executor", task.Executor, "lakeflow", lw.Name, "namespace", lw.Namespace)
		return nil, fmt.Errorf("failed to build template for task %s: %w", task.Name, err)
	}

	if task.PodScheduling != nil {
		applyTaskSchedulingToArgoTemplate(template, task.PodScheduling, task.QueueName)
	} else {
		template.NodeSelector = defaultNodeSelectors
	}
	return template, nil
}

// Helper functions
func int32Ptr(i int32) *int32 {
	return &i
}
