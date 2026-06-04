package adapter

import (
	"fmt"
	"strings"
	"sync"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	corev1 "k8s.io/api/core/v1"
)

// Singleton instance and sync.Once for thread-safe initialization
var (
	factoryInstance TaskRendererFactory
	factoryOnce     sync.Once
)

// TaskRenderer renders the Argo Workflow template for a single task. It is the
// per-executor Strategy used by the converter: the already-resolved task and the
// shared ArgoRenderContext (task index, credential resolver, namespace) are passed
// in, so renderers stay stateless and never re-scan Spec.Tasks or re-create
// per-task cluster clients.
type TaskRenderer interface {
	// RenderTask creates the Argo Workflow template for the given task.
	RenderTask(ctx *ArgoRenderContext, task *v1alpha1.Task) (*argowfv1.Template, error)
}

// TaskRendererFactory creates the appropriate renderer for a given executor type.
// Singleton pattern: only one instance should exist throughout the application.
type TaskRendererFactory interface {
	// CreateRenderer returns the appropriate renderer for the given executor type
	CreateRenderer(executorType v1alpha1.TaskExecutor) (TaskRenderer, error)
	// RegisterRenderer registers a new task renderer
	RegisterRenderer(executorType v1alpha1.TaskExecutor, renderer TaskRenderer)
}

// taskRendererFactory implements TaskRendererFactory
type taskRendererFactory struct {
	renderers map[v1alpha1.TaskExecutor]TaskRenderer
}

var _ TaskRenderer = (*commandTaskRenderer)(nil)

// commandTaskRenderer implements TaskRenderer for command-based tasks (bash, python).
// It is stateless; everything it needs comes from the ArgoRenderContext.
type commandTaskRenderer struct {
}

// GetTaskRendererFactory returns the singleton instance of TaskRendererFactory
// Thread-safe initialization using sync.Once
func GetTaskRendererFactory() TaskRendererFactory {
	factoryOnce.Do(func() {
		factoryInstance = createTaskRendererFactory()
	})
	return factoryInstance
}

// createTaskRendererFactory creates and initializes the factory with default renderers
// This is called only once by the singleton pattern
func createTaskRendererFactory() TaskRendererFactory {
	rendererFactory := &taskRendererFactory{
		renderers: make(map[v1alpha1.TaskExecutor]TaskRenderer),
	}

	// Register default renderers (all stateless; per-conversion state is carried
	// by the ArgoRenderContext passed to RenderTask).
	rendererFactory.RegisterRenderer(v1alpha1.TaskExecutorSpark, newSparkTaskRenderer())
	rendererFactory.RegisterRenderer(v1alpha1.TaskExecutorBash, newCommandTaskRenderer())
	rendererFactory.RegisterRenderer(v1alpha1.TaskExecutorPython, newCommandTaskRenderer())

	return rendererFactory
}

// newCommandTaskRenderer creates a new command task renderer
func newCommandTaskRenderer() TaskRenderer {
	return &commandTaskRenderer{}
}

// CreateRenderer returns the appropriate renderer for the given executor type
func (f *taskRendererFactory) CreateRenderer(executorType v1alpha1.TaskExecutor) (TaskRenderer, error) {
	renderer, exists := f.renderers[executorType]
	if !exists {
		err := fmt.Errorf("no renderer registered for executor type: %s", executorType)
		logger.Error(err, "No renderer registered for executor type", "executorType", executorType)
		return nil, err
	}
	return renderer, nil
}

// RegisterRenderer registers a new task renderer
func (f *taskRendererFactory) RegisterRenderer(executorType v1alpha1.TaskExecutor, renderer TaskRenderer) {
	f.renderers[executorType] = renderer
}

// RenderTask renders the Argo template for an already-resolved command task. The
// task comes from the shared render context, so no Spec.Tasks rescan is needed and
// command tasks need no credential resolver.
func (c *commandTaskRenderer) RenderTask(ctx *ArgoRenderContext, task *v1alpha1.Task) (*argowfv1.Template, error) {
	return c.build(ctx, task)
}

// build renders the Argo template for an already-resolved command task.
func (c *commandTaskRenderer) build(ctx *ArgoRenderContext, task *v1alpha1.Task) (*argowfv1.Template, error) {
	workflow := ctx.LakeFlow
	spec := task.TaskSpec.CommandExecutorSpec

	// Initialize template with basic metadata and configuration
	template := c.initializeTemplate(workflow, task)

	if task.Executor != v1alpha1.TaskExecutorBash && task.Executor != v1alpha1.TaskExecutorPython {
		err := fmt.Errorf("unsupported command executor type: %s", task.Executor)
		logger.Error(err, "Unsupported command executor type", "executor", task.Executor, "task", task.Name, "lakeflow", workflow.Name, "namespace", workflow.Namespace)
		return nil, err
	}

	// Image is a required, user-specified field; the operator does not derive it.
	image := spec.Image

	// Build command and arguments based on execution mode using Argo parameters
	var command []string
	var args []string

	// Handle different execution modes
	switch {
	case spec.ScriptFileMode != nil:
		// ScriptFile mode: execute script files from mounted volumes (OSS PVC, ConfigMap, etc.)
		err := c.handleScriptFileMode(task.Executor, spec.ScriptFileMode, template, &command, &args)
		if err != nil {
			return nil, err
		}

	case spec.InlineMode != nil:
		// Inline mode: execute command directly using parameters
		err := c.handleInlineMode(spec.InlineMode, template, &command, &args)
		if err != nil {
			return nil, err
		}

	case spec.PythonProjectMode != nil:
		// Python project mode: extract archive and run entry point
		// Only valid for python executor type
		if task.Executor != v1alpha1.TaskExecutorPython {
			err := fmt.Errorf("pythonProjectMode is only valid for python executor, got: %s", task.Executor)
			logger.Error(err, "Invalid executor type for pythonProjectMode", "task", task.Name, "executor", task.Executor)
			return nil, err
		}
		err := c.handlePythonProjectMode(spec.PythonProjectMode, template, &command, &args)
		if err != nil {
			return nil, err
		}

	default:
		err := fmt.Errorf("no execution mode specified for task %s: must specify scriptFileMode, inlineMode, or pythonProjectMode", task.Name)
		logger.Error(err, "No execution mode specified for task", "task", task.Name, "lakeflow", workflow.Name, "namespace", workflow.Namespace)
		return nil, err
	}

	// Set up container
	template.Container = &corev1.Container{
		Name:            task.Name,
		Image:           image,
		Command:         command,
		Args:            args,
		ImagePullPolicy: "IfNotPresent",
	}

	if spec.Resources != nil {
		template.Container.Resources = *spec.Resources
	}

	// Apply task-level service account if specified
	if task.TaskSpec.ServiceAccountName != "" {
		template.ServiceAccountName = task.TaskSpec.ServiceAccountName
	}
	logger.Info("CommonTaskExecutorBuilder Build", "objectStorageIsNil", spec.ObjectStorage == nil, "blockStorageIsNil", spec.BlockStorage == nil)
	// Handle storage: create OSS PVC and configure volume mounts
	if spec.ObjectStorage != nil || spec.BlockStorage != nil {
		if err := c.handleCommandTaskPVC(ctx, task, spec, template); err != nil {
			return nil, err
		}
	}
	// Add user-specified volume mounts (from spec.volumeMounts)
	if len(spec.VolumeMounts) > 0 {
		template.Container.VolumeMounts = append(template.Container.VolumeMounts, spec.VolumeMounts...)
	}
	// Add user-specified volumes (from spec.volumes)
	if len(spec.Volumes) > 0 {
		template.Volumes = append(template.Volumes, spec.Volumes...)
	}

	return template, nil
}

// applyRerunRetryLabels maps the LakeFlow rerun/retry request annotations onto the
// corresponding marker labels on target, in place. Shared by every per-task label
// builder so the resubmit/retry detection stays consistent across executor types.
func applyRerunRetryLabels(target map[string]string, workflow *v1alpha1.LakeFlow) {
	annotations := workflow.GetAnnotations()
	if annotations == nil {
		return
	}
	if v, ok := annotations[v1alpha1.ResubmitRequestAnnotation]; ok && v != "" {
		target[v1alpha1.IsResubmitWorkflowLabel] = "true"
	}
	if v, ok := annotations[v1alpha1.RetryRequestAnnotation]; ok && v != "" {
		target[v1alpha1.IsRetryWorkflowLabel] = "true"
	}
}

func buildArgoTemplateLabels(workflow *v1alpha1.LakeFlow, task *v1alpha1.Task) map[string]string {
	labels := map[string]string{
		v1alpha1.WorkflowNameLabel:     workflow.Name,
		v1alpha1.WorkflowTaskNameLabel: task.Name,
	}
	applyRerunRetryLabels(labels, workflow)
	return labels
}

// initializeTemplate creates and configures the basic Argo template structure
// including metadata, labels, Volcano configuration, and retry policy
func (c *commandTaskRenderer) initializeTemplate(workflow *v1alpha1.LakeFlow, task *v1alpha1.Task) *argowfv1.Template {
	template := &argowfv1.Template{
		Name: task.Name,
		Inputs: argowfv1.Inputs{
			Parameters: []argowfv1.Parameter{},
		},
		// Add metadata with both workflow and task labels for consistent querying
		Metadata: argowfv1.Metadata{
			Labels: buildArgoTemplateLabels(workflow, task),
		},
	}

	// Add Volcano scheduler and queue annotation if queue is specified
	if task.TaskSpec.QueueName != "" {
		// Set scheduler name for Volcano
		template.SchedulerName = v1alpha1.VolcanoSchedulerName

		// Initialize annotations map if not exists
		if template.Metadata.Annotations == nil {
			template.Metadata.Annotations = make(map[string]string)
		}

		// Set Volcano queue annotation
		template.Metadata.Annotations[v1alpha1.VolcanoQueueAnnotationKey] = task.TaskSpec.QueueName
	}

	// Add retry policy if specified
	template.RetryStrategy = buildRetryStrategy(task)

	return template
}

// handleCommandTaskPVC processes PVC specifications for command tasks
// This unified method handles both OSS PVC creation and volume mount configuration
// It should be called after template initialization to ensure proper ordering
func (c *commandTaskRenderer) handleCommandTaskPVC(
	ctx *ArgoRenderContext,
	task *v1alpha1.Task,
	spec *v1alpha1.CommandExecutorSpec,
	template *argowfv1.Template) error {

	workflow := ctx.LakeFlow
	// Mount the shared OSS object storage if configured. The PVC/StorageClass
	// itself is provisioned by the reconciler (see reconciler.applyInfra); this
	// builder only wires the mount into the template and stays side-effect free.
	if objStorage := spec.ObjectStorage; objStorage != nil {
		ossMountPath := "/mnt/oss"
		ossClaimName := ""
		if objStorage.MountSpec != nil {
			// Resolve via the shared helper so command tasks reference exactly the
			// PVC the reconciler provisions (explicit pvcName, or a derived
			// namespace-shared name when pvcName is empty).
			ossClaimName = cloud.EffectiveObjectStoragePVCName(objStorage, spec.CredentialsRef, ctx.Namespace, ctx.CloudProfile)
			if objStorage.MountSpec.MountPath != "" {
				ossMountPath = objStorage.MountSpec.MountPath
			}
		}

		// Add OSS PVC mount (namespace-scoped, shared across workflows)
		template.Container.VolumeMounts = append(template.Container.VolumeMounts, corev1.VolumeMount{
			Name:      "oss-pvc",
			MountPath: ossMountPath,
		})
		// Add OSS PVC volume reference
		template.Volumes = append(template.Volumes, corev1.Volume{
			Name: "oss-pvc",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: ossClaimName,
				},
			},
		})
		logger.Info("Successfully configured OSS storage for command task",
			"ossPVC", ossClaimName, "ossPVCMount", ossMountPath, "task", task.Name, "workflow", workflow.Name)
	}

	// Add per-task Workflow PVC mount (created by Argo volumeClaimTemplates from BlockStorage)
	// Each task gets its own independent PVC to support parallel execution on different nodes
	// Volume name: "workdir-{taskName}" (unique per task)
	// Mount path: "/data" (same for all tasks - consistent application interface)
	if spec.BlockStorage != nil {
		workdirVolumeName := GetWorkdirVolumeName(task.Name)
		template.Container.VolumeMounts = append(template.Container.VolumeMounts, corev1.VolumeMount{
			Name:      workdirVolumeName,
			MountPath: "/data",
		})
		logger.Info("Configured workdir PVC volume mount for command task",
			"workdirPVC", workdirVolumeName, "workdirPVCMount", "/data", "taskName", task.Name)
	}

	return nil
}

// handleScriptFileMode processes script file mode execution - execute scripts from mounted volumes
// This is the recommended mode for OSS PVC-mounted scripts for easy updates without CRD changes
func (c *commandTaskRenderer) handleScriptFileMode(
	executorType v1alpha1.TaskExecutor,
	scriptFileMode *v1alpha1.ScriptFileMode,
	template *argowfv1.Template, command *[]string, args *[]string) error {

	logger.Info("CommandTaskExecutor build handlerScriptFileMode", "scriptFileMode", scriptFileMode)
	// Add script path as parameter
	template.Inputs.Parameters = append(template.Inputs.Parameters, argowfv1.Parameter{
		Name:  "scriptPath",
		Value: argowfv1.AnyStringPtr(scriptFileMode.ScriptPath),
	})

	// Set command based on executor type
	// Note: Script file mode executes files directly, so we don't use -c flag
	// (unlike inline mode which uses -c to execute command strings)
	switch executorType {
	case v1alpha1.TaskExecutorBash:
		*command = []string{"/bin/bash"}
	case v1alpha1.TaskExecutorPython:
		*command = []string{"python3"}
	default:
		return fmt.Errorf("unsupported executor type for scriptFileMode: %s", executorType)
	}
	*args = []string{"{{inputs.parameters.scriptPath}}"}
	// Add arguments directly to args to ensure they are passed as separate arguments
	if len(scriptFileMode.Arguments) > 0 {
		*args = append(*args, scriptFileMode.Arguments...)
	}
	logger.Info("CommandTaskExecutor Final", "template", *template)
	return nil
}

// handleInlineMode processes inline mode execution using Argo parameters
func (c *commandTaskRenderer) handleInlineMode(inlineMode *v1alpha1.InlineMode,
	template *argowfv1.Template, command *[]string, args *[]string) error {

	// Set command directly (typically ["/bin/bash", "-c"] or ["python", "-c"])
	if len(inlineMode.Command) > 0 {
		*command = inlineMode.Command
	}

	// Add arguments as parameter and use in args
	if len(inlineMode.Arguments) > 0 {
		template.Inputs.Parameters = append(template.Inputs.Parameters, argowfv1.Parameter{
			Name:  "script",
			Value: argowfv1.AnyStringPtr(c.joinCommandArgs(inlineMode.Arguments)),
		})
		// Use parameter in args
		*args = []string{"{{inputs.parameters.script}}"}
	}

	return nil
}

// joinCommandArgs joins command arguments into a single string for parameter passing
func (c *commandTaskRenderer) joinCommandArgs(args []string) string {
	if len(args) == 0 {
		return ""
	}
	if len(args) == 1 {
		return args[0]
	}

	// For multiple arguments, join them with spaces
	// This preserves the original structure of the arguments
	result := ""
	for i, arg := range args {
		if i > 0 {
			result += " "
		}
		result += arg
	}
	return result
}

// handlePythonProjectMode processes Python project mode execution
// This mode extracts a compressed archive (zip or tar.gz) to /data and runs the specified entry point
// The project directory is derived from the archive name (e.g., simple-etl.zip -> /data/simple-etl)
func (c *commandTaskRenderer) handlePythonProjectMode(
	projectMode *v1alpha1.PythonProjectMode,
	template *argowfv1.Template,
	command *[]string,
	args *[]string) error {

	logger.Info("CommandTaskExecutor build handlePythonProjectMode", "projectMode", projectMode)

	// Runtime validation (defense in depth - should be caught by CRD validation)
	hasEntryPoint := projectMode.EntryPoint != ""
	hasCommand := projectMode.Command != ""

	if hasEntryPoint && hasCommand {
		err := fmt.Errorf("pythonProjectMode: both entryPoint and cmd are specified, only one is allowed")
		logger.Error(err, "Invalid pythonProjectMode configuration",
			"entryPoint", projectMode.EntryPoint,
			"cmd", projectMode.Command)
		return err
	}

	if !hasEntryPoint && !hasCommand {
		err := fmt.Errorf("pythonProjectMode: neither entryPoint nor cmd is specified, exactly one is required")
		logger.Error(err, "Invalid pythonProjectMode configuration")
		return err
	}

	// Warn if args are specified with cmd (they will be ignored)
	if hasCommand && len(projectMode.Arguments) > 0 {
		err := fmt.Errorf("pythonProjectMode: args cannot be used with cmd; either use entryPoint with args, or include all arguments in the cmd string")
		logger.Error(err, "Invalid pythonProjectMode configuration",
			"cmd", projectMode.Command,
			"args", projectMode.Arguments)
		return err
	}

	// Add parameters for archive path and entry point
	template.Inputs.Parameters = append(template.Inputs.Parameters,
		argowfv1.Parameter{
			Name:  "archivePath",
			Value: argowfv1.AnyStringPtr(projectMode.ArchivePath),
		},
		argowfv1.Parameter{
			Name:  "entryPoint",
			Value: argowfv1.AnyStringPtr(projectMode.EntryPoint),
		},
	)

	// Build the extraction and execution script
	script := c.buildPythonProjectScript(projectMode)

	template.Inputs.Parameters = append(template.Inputs.Parameters, argowfv1.Parameter{
		Name:  "projectScript",
		Value: argowfv1.AnyStringPtr(script),
	})

	// Use bash to execute the generated script
	*command = []string{"/bin/bash", "-c"}
	*args = []string{"{{inputs.parameters.projectScript}}"}

	logger.Info("CommandTaskExecutor PythonProjectMode configured",
		"archivePath", projectMode.ArchivePath,
		"entryPoint", projectMode.EntryPoint,
		"command", projectMode.Command,
		"dependencyScript", projectMode.DependencyScript)

	return nil
}

// buildPythonProjectScript generates a shell script for Python project execution
// The script extracts archive, intelligently detects working directory, and runs entry point or command
// Supports both archive structures:
// - With top-level dir: myproject.zip/myproject/* -> working dir: /data/myproject-extract/myproject/
// - Files at root: myproject.zip/*.py -> working dir: /data/myproject-extract/
func (c *commandTaskRenderer) buildPythonProjectScript(projectMode *v1alpha1.PythonProjectMode) string {
	// Build arguments array (only used with EntryPoint)
	// Use proper shell array to handle arguments with spaces
	argsArray := ""
	if len(projectMode.Arguments) > 0 {
		argsArray = "("
		for i, arg := range projectMode.Arguments {
			if i > 0 {
				argsArray += " "
			}
			// Escape quotes in arguments and wrap in quotes
			escapedArg := arg
			escapedArg = strings.ReplaceAll(escapedArg, `"`, `\"`)
			argsArray += `"` + escapedArg + `"`
		}
		argsArray += ")"
	}

	// Build the script
	// The script intelligently detects archive structure and provides stable working directory:
	// - If archive contains single top-level dir: myproject.zip/myproject/* -> /data/myproject-extract/myproject/
	// - If archive has files at root: myproject.zip/*.py -> /data/myproject-extract/
	script := `set -e
echo "=== Python Project Mode Execution ==="
echo "Archive: ` + projectMode.ArchivePath + `"

# Extract project name from archive path (remove path and extension)
ARCHIVE="` + projectMode.ArchivePath + `"
ARCHIVE_BASENAME=$(basename "$ARCHIVE")
# Remove all known extensions (.zip, .tar.gz, .tgz, .tar)
PROJECT_NAME="${ARCHIVE_BASENAME%.zip}"
PROJECT_NAME="${PROJECT_NAME%.tar.gz}"
PROJECT_NAME="${PROJECT_NAME%.tgz}"
PROJECT_NAME="${PROJECT_NAME%.tar}"

# Extract to temporary directory first
EXTRACT_DIR="/data/${PROJECT_NAME}-extract"
mkdir -p "$EXTRACT_DIR"
echo "Extracting to temporary directory: $EXTRACT_DIR"

# Extract archive based on file extension
if [[ "$ARCHIVE" == *.zip ]]; then
    echo "Extracting ZIP archive..."
    unzip -o "$ARCHIVE" -d "$EXTRACT_DIR"
elif [[ "$ARCHIVE" == *.tar.gz ]] || [[ "$ARCHIVE" == *.tgz ]]; then
    echo "Extracting TAR.GZ archive..."
    tar -xzf "$ARCHIVE" -C "$EXTRACT_DIR"
elif [[ "$ARCHIVE" == *.tar ]]; then
    echo "Extracting TAR archive..."
    tar -xf "$ARCHIVE" -C "$EXTRACT_DIR"
else
    echo "Error: Unsupported archive format. Supported: .zip, .tar.gz, .tgz, .tar"
    exit 1
fi

# Intelligently detect working directory based on archive structure
# Check if extraction resulted in a single top-level directory
shopt -s nullglob dotglob
TOP_LEVEL_ITEMS=("$EXTRACT_DIR"/*)
shopt -u nullglob dotglob

if [ ${#TOP_LEVEL_ITEMS[@]} -eq 0 ]; then
    echo "Error: Archive is empty"
    exit 1
elif [ ${#TOP_LEVEL_ITEMS[@]} -eq 1 ] && [ -d "${TOP_LEVEL_ITEMS[0]}" ]; then
    # Single top-level directory - use it as working directory
    # Example: myproject.zip contains myproject/main.py
    PROJECT_DIR="${TOP_LEVEL_ITEMS[0]}"
    echo "Archive contains single top-level directory"
    echo "Working directory: $PROJECT_DIR"
else
    # Multiple items at root or single file - use extraction directory as working directory
    # Example: myproject.zip contains main.py, config.py at root
    PROJECT_DIR="$EXTRACT_DIR"
    echo "Archive contains files at root level"
    echo "Working directory: $PROJECT_DIR"
fi

echo "Project structure:"
ls -la "$PROJECT_DIR"

# Change to project working directory
cd "$PROJECT_DIR"
echo "Current working directory: $(pwd)"
`

	// Add dependency installation if script is provided
	if projectMode.DependencyScript != "" {
		script += `
# Run dependency installation script
DEP_SCRIPT="$PROJECT_DIR/` + projectMode.DependencyScript + `"
if [ -f "$DEP_SCRIPT" ]; then
    echo "Running dependency installation script: $DEP_SCRIPT"
    chmod +x "$DEP_SCRIPT"
    bash "$DEP_SCRIPT"
else
    echo "Error: Dependency script not found: $DEP_SCRIPT"
    exit 1
fi
`
	}

	// Add execution command - either Command (full command) or EntryPoint (with python3)
	if projectMode.Command != "" {
		// Command mode: run the command directly in project directory
		script += `
# Execute command
echo "Executing command: ` + projectMode.Command + `"
` + projectMode.Command + `
`
	} else {
		// EntryPoint mode: run python3 with entry point script and arguments
		script += `
# Execute entry point with python3
ENTRY_POINT="$PROJECT_DIR/` + projectMode.EntryPoint + `"
if [ ! -f "$ENTRY_POINT" ]; then
    echo "Error: Entry point not found: $ENTRY_POINT"
    exit 1
fi
`
		if len(projectMode.Arguments) > 0 {
			script += `
# Prepare arguments array
ARGS=` + argsArray + `
echo "Executing: python3 $ENTRY_POINT ${ARGS[@]}"
python3 "$ENTRY_POINT" "${ARGS[@]}"
`
		} else {
			script += `
echo "Executing: python3 $ENTRY_POINT"
python3 "$ENTRY_POINT"
`
		}
	}

	script += `
echo "=== Python Project Execution Complete ==="
`

	return script
}
