package rerun

import (
	"context"
	"fmt"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v3/workflow/packer"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
)

// identifyFailedTasks analyzes the workflow status and returns a list of failed task names
func (m *workflowRerunManager) identifyFailedTasks(ctx context.Context, wf *argowfv1.Workflow) ([]string, error) {
	if wf == nil {
		return nil, fmt.Errorf("workflow is nil")
	}

	// Handle compressed nodes: Argo compresses large workflows (>1MB) using gzip+base64
	// and stores them in CompressedNodes instead of Nodes. We need to decompress them
	// before we can analyze which tasks failed.
	// When CompressedNodes is present, it is the authoritative source of truth.
	if wf.Status.CompressedNodes != "" {
		if err := packer.DecompressWorkflow(wf); err != nil {
			logger.Error(err, "Failed to decompress workflow nodes",
				"workflowName", wf.Name,
				"compressedNodesLength", len(wf.Status.CompressedNodes))
			return nil, fmt.Errorf("failed to decompress workflow nodes: %w", err)
		}
		logger.Info("Decompressed workflow nodes for retry analysis",
			"workflowName", wf.Name,
			"nodeCount", len(wf.Status.Nodes))
	}

	failedTasks := make([]string, 0)

	// Identify failed tasks
	// Note: We must handle retries correctly. If a task failed once but succeeded on retry,
	// it should NOT be considered failed.
	failedTemplates := make(map[string]bool)
	succeededTemplates := make(map[string]bool)

	// Iterate through all nodes in the workflow
	for nodeName, node := range wf.Status.Nodes {
		// Skip DAG/Step nodes - we only want task-level nodes
		if node.Type == argowfv1.NodeTypeDAG || node.Type == argowfv1.NodeTypeSteps {
			continue
		}

		templateName := node.TemplateName
		if templateName == "" {
			continue
		}

		// Record task status
		switch node.Phase {
		case argowfv1.NodeSucceeded:
			succeededTemplates[templateName] = true
		case argowfv1.NodeFailed, argowfv1.NodeError:
			failedTemplates[templateName] = true
			logger.Info("Found failed node for task",
				"nodeName", nodeName,
				"templateName", templateName,
				"phase", node.Phase,
				"message", node.Message)
		}
	}

	// Filter out tasks that eventually succeeded
	for templateName := range failedTemplates {
		if !succeededTemplates[templateName] {
			failedTasks = append(failedTasks, templateName)
		} else {
			logger.Info("Task failed but eventually succeeded (retry), skipping",
				"templateName", templateName)
		}
	}

	if len(failedTasks) == 0 {
		logger.Info("No failed tasks identified in workflow",
			"workflowName", wf.Name,
			"workflowPhase", wf.Status.Phase)
	} else {
		logger.Info("Identified failed tasks for retry",
			"workflowName", wf.Name,
			"failedTaskCount", len(failedTasks),
			"failedTasks", failedTasks)
	}

	return failedTasks, nil
}

// buildFilteredDAG creates a list of tasks that includes only failed tasks and their dependencies
func (m *workflowRerunManager) buildFilteredDAG(lw *v1alpha1.LakeFlow, failedTasks []string) ([]v1alpha1.Task, error) {
	if len(failedTasks) == 0 {
		return nil, fmt.Errorf("no failed tasks to retry")
	}

	// Create a map for quick lookup
	failedTaskMap := make(map[string]bool)
	for _, taskName := range failedTasks {
		failedTaskMap[taskName] = true
	}

	// Build a map of all tasks by name and dependency graph in a single pass
	taskMap := make(map[string]*v1alpha1.Task)
	dependents := make(map[string][]string)
	for i := range lw.Spec.Tasks {
		task := &lw.Spec.Tasks[i]
		taskMap[task.Name] = task

		// Track which tasks depend on each dependency
		for _, dep := range task.DependsOn {
			dependents[dep] = append(dependents[dep], task.Name)
		}
	}

	// Collect all tasks that need to be included (Failed tasks + Downstream dependents)
	tasksToInclude := make(map[string]bool)
	queue := make([]string, 0, len(failedTasks))

	// Start with failed tasks
	for taskName := range failedTaskMap {
		if !tasksToInclude[taskName] {
			tasksToInclude[taskName] = true
			queue = append(queue, taskName)
		}
	}

	// BFS to find all downstream dependents
	head := 0
	for head < len(queue) {
		currentTask := queue[head]
		head++

		// Find all tasks that depend on currentTask
		for _, dependentName := range dependents[currentTask] {
			if !tasksToInclude[dependentName] {
				tasksToInclude[dependentName] = true
				queue = append(queue, dependentName)
			}
		}
	}

	// Build the filtered task list maintaining original order
	// AND update dependencies to exclude skipped tasks
	filteredTasks := make([]v1alpha1.Task, 0, len(tasksToInclude))
	for _, task := range lw.Spec.Tasks {
		if tasksToInclude[task.Name] {
			// Create a deep copy of the task to modify dependencies safely
			taskCopy := task.DeepCopy()

			// Filter dependencies: only keep those that are also being re-run
			newDependsOn := make([]string, 0)
			for _, dep := range taskCopy.DependsOn {
				if tasksToInclude[dep] {
					newDependsOn = append(newDependsOn, dep)
				}
			}
			taskCopy.DependsOn = newDependsOn

			filteredTasks = append(filteredTasks, *taskCopy)
		}
	}

	logger.Info("Built filtered DAG for retry",
		"lakeflow", lw.Name,
		"originalTaskCount", len(lw.Spec.Tasks),
		"filteredTaskCount", len(filteredTasks),
		"failedTaskCount", len(failedTasks),
		"filteredTasks", getTaskNames(filteredTasks))

	return filteredTasks, nil
}

// createFilteredWorkflow builds a new WorkflowTemplate and Workflow with only the filtered tasks
func (m *workflowRerunManager) createFilteredWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow, filteredTasks []v1alpha1.Task) error {
	// Create a copy of the LakeFlow with filtered tasks
	lwCopy := lw.DeepCopy()
	lwCopy.Spec.Tasks = filteredTasks

	// Log original trigger configuration for debugging
	hasDependencies := len(lwCopy.Spec.WorkflowTrigger.Depend.Workflows) > 0
	hasSchedule := lwCopy.Spec.WorkflowTrigger.Schedule.Cron != ""
	logger.Info("Original trigger configuration before retry",
		"lakeflow", lw.Name,
		"hasDependencies", hasDependencies,
		"dependencyCount", len(lwCopy.Spec.WorkflowTrigger.Depend.Workflows),
		"hasSchedule", hasSchedule,
		"scheduleCron", lwCopy.Spec.WorkflowTrigger.Schedule.Cron,
		"filteredTaskCount", len(filteredTasks))

	// CRITICAL: Clear the entire WorkflowTrigger to force standalone Workflow creation
	// Retry must create an immediate Workflow instance with the filtered DAG,
	// rather than waiting for the original trigger path and full DAG.
	// Keeping any part of the Trigger structure causes the converter to create
	// only WorkflowTemplate (for dependency mode) or CronWorkflow (for schedule mode).
	if hasDependencies {
		logger.Info("Clearing upstream dependencies for immediate retry execution",
			"lakeflow", lw.Name,
			"originalUpstreams", len(lwCopy.Spec.WorkflowTrigger.Depend.Workflows))
	}
	if hasSchedule {
		logger.Info("Clearing schedule trigger for immediate retry execution",
			"lakeflow", lw.Name,
			"originalCron", lwCopy.Spec.WorkflowTrigger.Schedule.Cron)
	}

	// Clear the entire trigger structure to force immediate Workflow creation
	lwCopy.Spec.WorkflowTrigger = v1alpha1.Trigger{}
	logger.Info("Cleared entire WorkflowTrigger structure for standalone retry Workflow creation",
		"lakeflow", lw.Name)

	// Use the WorkflowConverter to build the WorkflowTemplate and Workflow
	// With trigger cleared, converter will create a standalone Workflow instance
	// This ensures we follow the same pattern as normal workflow creation (reconciler logic)
	// and correctly handle all executor types (Spark, Script, etc.) via task_renderer and spark_application_renderer
	converter := m.workflowConverter
	if converter == nil {
		converter = adapter.NewWorkflowConverter(defaultAliyunProfile())
	}
	argoResources, err := converter.Convert(lwCopy, m.config)
	if err != nil {
		return fmt.Errorf("failed to convert filtered LakeFlow to Argo resources: %w", err)
	}

	// Debug: Log what resources were created by the converter
	logger.Info("Converter returned resources for retry",
		"lakeflow", lw.Name,
		"hasWorkflow", argoResources.Workflow != nil,
		"hasWorkflowTemplate", argoResources.WorkflowTemplate != nil,
		"hasCronWorkflow", argoResources.CronWorkflow != nil)

	if argoResources.Workflow == nil {
		logger.Error(nil, "CRITICAL: Converter did not create a Workflow instance for retry",
			"lakeflow", lw.Name,
			"hasWorkflowTemplate", argoResources.WorkflowTemplate != nil,
			"filteredTaskCount", len(filteredTasks))
		return fmt.Errorf("converter failed to create Workflow instance for retry (WorkflowTemplate-only mode)")
	}

	// Create WorkflowTemplate and Workflow using shared method, with deterministic
	// names derived from the durable rerun token for atomic idempotency.
	token := lw.GetAnnotations()[v1alpha1.RerunTokenAnnotation]
	if err := m.createWorkflowWithRetry(ctx, lw, argoResources, token, true); err != nil {
		return err
	}

	logger.Info("Successfully created filtered workflow for retry",
		"lakeflow", lw.Name,
		"filteredTaskCount", len(filteredTasks))

	return nil
}
