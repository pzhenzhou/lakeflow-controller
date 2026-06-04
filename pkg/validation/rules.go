package validation

import (
	"reflect"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Webhook/admission limits. These are the source of truth shared by the
// validating webhook (admission-time, fail fast) and the in-reconcile safety
// net (validationManager.Validate). The CRD kubebuilder markers mirror the
// task-count and task-name limits so OpenAPI/clients agree; the cross-field
// and immutability rules below are webhook-only because they cannot be
// expressed as static CRD markers.
const (
	// MaxTasksPerWorkflow caps the number of tasks in a single LakeFlow.
	// Large DAGs inflate the Argo WorkflowTemplate/Workflow object size and
	// per-node pod/PodGroup overhead, which puts significant load on the Argo
	// controller and etcd.
	MaxTasksPerWorkflow = 30
	// MaxTaskNameLength caps a task name. The task name is the prefix of the
	// generated Spark driver Service (an RFC1035/RFC1123 label hard-capped at
	// 63 chars); 40 leaves headroom for the generated suffixes.
	MaxTaskNameLength = 40
	// MaxWorkflowNameLength caps the LakeFlow name. rerunWorkflowName truncates
	// at 40 chars, and "<name>-template-<unix>" stays well under 253.
	MaxWorkflowNameLength = 40
)

// ValidateCreate runs all stateless (single-object) checks against a LakeFlow.
// It is used by the webhook on CREATE and, as the new value, on UPDATE, and by
// the in-reconcile safety net. It returns a field.ErrorList so callers can
// surface path-aware errors or aggregate them into a single error.
func ValidateCreate(lw *v1alpha1.LakeFlow) field.ErrorList {
	var allErrs field.ErrorList

	allErrs = append(allErrs, validateWorkflowName(lw)...)
	allErrs = append(allErrs, validateTrigger(lw)...)
	allErrs = append(allErrs, validateTasks(lw)...)
	allErrs = append(allErrs, validateObjectStorageClasses(lw)...)

	return allErrs
}

// ValidateUpdate runs the full create-time validation against the new object
// plus the immutability diffs that require the old object.
func ValidateUpdate(oldLw, newLw *v1alpha1.LakeFlow) field.ErrorList {
	allErrs := ValidateCreate(newLw)
	allErrs = append(allErrs, validateImmutableFields(oldLw, newLw)...)
	return allErrs
}

// validateWorkflowName enforces the workflow-name length cap and RFC1123
// subdomain shape. (The API server already enforces a valid metadata.name; the
// webhook ties it to the 40-char operator limit.)
func validateWorkflowName(lw *v1alpha1.LakeFlow) field.ErrorList {
	var allErrs field.ErrorList
	namePath := field.NewPath("metadata", "name")
	if len(lw.Name) > MaxWorkflowNameLength {
		allErrs = append(allErrs, field.TooLong(namePath, lw.Name, MaxWorkflowNameLength))
	}
	for _, msg := range validation.IsDNS1123Subdomain(lw.Name) {
		allErrs = append(allErrs, field.Invalid(namePath, lw.Name, msg))
	}
	return allErrs
}

// validateTrigger enforces that schedule and dependency triggers are mutually
// exclusive. Neither trigger set means "immediate" mode, which is allowed.
func validateTrigger(lw *v1alpha1.LakeFlow) field.ErrorList {
	var allErrs field.ErrorList
	trigger := lw.Spec.WorkflowTrigger
	triggerPath := field.NewPath("spec", "trigger")

	if len(trigger.Depend.Workflows) > 0 {
		workflowsPath := triggerPath.Child("depend", "workflows")
		for i, upstream := range trigger.Depend.Workflows {
			if upstream.Name == "" {
				allErrs = append(allErrs, field.Required(workflowsPath.Index(i).Child("name"), "upstream workflow name is required"))
			}
			if upstream.Namespace == "" {
				allErrs = append(allErrs, field.Required(workflowsPath.Index(i).Child("namespace"), "upstream workflow namespace is required"))
			}
		}
	}

	hasSchedule := trigger.Schedule.Cron != ""
	hasDependency := len(trigger.Depend.Workflows) > 0
	if hasSchedule && hasDependency {
		allErrs = append(allErrs, field.Forbidden(triggerPath,
			"schedule trigger and dependency trigger are mutually exclusive; set at most one (an empty trigger means immediate)"))
	}

	return allErrs
}

// validateTasks enforces the task-count limit, per-task name rules (length,
// RFC1123 label, uniqueness), executor/spec consistency, executor-specific
// required fields, and dependsOn graph integrity.
func validateTasks(lw *v1alpha1.LakeFlow) field.ErrorList {
	var allErrs field.ErrorList
	tasksPath := field.NewPath("spec", "tasks")
	tasks := lw.Spec.Tasks

	if len(tasks) == 0 {
		allErrs = append(allErrs, field.Required(tasksPath, "at least one task is required"))
		return allErrs
	}
	if len(tasks) > MaxTasksPerWorkflow {
		allErrs = append(allErrs, field.TooMany(tasksPath, len(tasks), MaxTasksPerWorkflow))
	}

	seenNames := make(map[string]struct{}, len(tasks))
	for i := range tasks {
		task := &tasks[i]
		taskPath := tasksPath.Index(i)
		allErrs = append(allErrs, validateTaskName(task, taskPath, seenNames)...)
		allErrs = append(allErrs, validateTaskExecutor(task, taskPath)...)
	}

	allErrs = append(allErrs, validateDependsOn(tasks, tasksPath)...)
	return allErrs
}

func validateTaskName(task *v1alpha1.Task, taskPath *field.Path, seen map[string]struct{}) field.ErrorList {
	var allErrs field.ErrorList
	namePath := taskPath.Child("name")
	if task.Name == "" {
		allErrs = append(allErrs, field.Required(namePath, "task name is required"))
		return allErrs
	}
	if len(task.Name) > MaxTaskNameLength {
		allErrs = append(allErrs, field.TooLong(namePath, task.Name, MaxTaskNameLength))
	}
	for _, msg := range validation.IsDNS1123Label(task.Name) {
		allErrs = append(allErrs, field.Invalid(namePath, task.Name, msg))
	}
	if _, ok := seen[task.Name]; ok {
		allErrs = append(allErrs, field.Duplicate(namePath, task.Name))
	}
	seen[task.Name] = struct{}{}
	return allErrs
}

// validateTaskExecutor enforces "exactly one executor spec", executor<->spec
// consistency, and executor-specific required fields.
func validateTaskExecutor(task *v1alpha1.Task, taskPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	spark := task.TaskSpec.SparkExecutorSpec
	command := task.TaskSpec.CommandExecutorSpec

	switch {
	case spark == nil && command == nil:
		allErrs = append(allErrs, field.Required(taskPath,
			"either sparkExecutor or commandExecutor must be specified"))
	case spark != nil && command != nil:
		allErrs = append(allErrs, field.Forbidden(taskPath,
			"exactly one of sparkExecutor or commandExecutor must be specified, not both"))
	}

	executorPath := taskPath.Child("executor")
	switch task.Executor {
	case v1alpha1.TaskExecutorSpark:
		if spark == nil {
			allErrs = append(allErrs, field.Invalid(executorPath, task.Executor,
				"executor 'spark' requires sparkExecutor"))
		}
	case v1alpha1.TaskExecutorBash, v1alpha1.TaskExecutorPython:
		if command == nil {
			allErrs = append(allErrs, field.Invalid(executorPath, task.Executor,
				"executor 'bash'/'python' requires commandExecutor"))
		}
	case "":
		allErrs = append(allErrs, field.Required(executorPath, "task executor is required"))
	default:
		allErrs = append(allErrs, field.NotSupported(executorPath, task.Executor,
			[]string{string(v1alpha1.TaskExecutorSpark), string(v1alpha1.TaskExecutorBash), string(v1alpha1.TaskExecutorPython)}))
	}

	if spark != nil {
		allErrs = append(allErrs, validateSparkExecutor(spark, taskPath.Child("sparkExecutor"))...)
	}
	if command != nil {
		allErrs = append(allErrs, validateCommandExecutor(task, command, taskPath.Child("commandExecutor"))...)
	}

	return allErrs
}

func validateSparkExecutor(spark *v1alpha1.SparkExecutorSpec, sparkPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if spark.Image == "" {
		allErrs = append(allErrs, field.Required(sparkPath.Child("image"), "image is required"))
	}
	if spark.MainApplicationFile == "" {
		allErrs = append(allErrs, field.Required(sparkPath.Child("mainApplicationFile"), "mainApplicationFile is required"))
	}
	if spark.QueueName == "" {
		allErrs = append(allErrs, field.Required(sparkPath.Child("queueName"), "queueName is required"))
	}
	if !hasResourceRequirements(spark.DriverResource.Resources) {
		allErrs = append(allErrs, field.Required(sparkPath.Child("driverResource", "resources"),
			"driver resource requests or limits are required"))
	}
	if !hasResourceRequirements(spark.ExecutorResource.Resources) {
		allErrs = append(allErrs, field.Required(sparkPath.Child("executorResource", "resources"),
			"executor resource requests or limits are required"))
	}

	// SparkAppType defaults to Java when empty.
	appType := spark.SparkAppType
	if appType == "" {
		appType = v1alpha1.SparkAppTypeJava
	}
	switch appType {
	case v1alpha1.SparkAppTypeJava:
		if spark.MainClass == "" {
			allErrs = append(allErrs, field.Required(sparkPath.Child("mainClass"),
				"mainClass is required for Java Spark applications"))
		}
	case v1alpha1.SparkAppTypePython:
		if spark.MainClass != "" {
			allErrs = append(allErrs, field.Forbidden(sparkPath.Child("mainClass"),
				"mainClass must be empty for Python Spark applications"))
		}
	}

	return allErrs
}

func validateCommandExecutor(task *v1alpha1.Task, command *v1alpha1.CommandExecutorSpec, cmdPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if command.Image == "" {
		allErrs = append(allErrs, field.Required(cmdPath.Child("image"), "image is required"))
	}

	modes := 0
	if command.ScriptFileMode != nil {
		modes++
	}
	if command.InlineMode != nil {
		modes++
	}
	if command.PythonProjectMode != nil {
		modes++
	}
	if modes != 1 {
		allErrs = append(allErrs, field.Forbidden(cmdPath,
			"exactly one execution mode must be specified: scriptFileMode, inlineMode, or pythonProjectMode"))
	}

	if command.PythonProjectMode != nil && task.Executor != v1alpha1.TaskExecutorPython {
		allErrs = append(allErrs, field.Forbidden(cmdPath.Child("pythonProjectMode"),
			"pythonProjectMode is only valid when executor is 'python'"))
	}

	return allErrs
}

// validateDependsOn rejects self-dependencies, dangling references to unknown
// tasks, and cycles in the dependsOn DAG.
func validateDependsOn(tasks []v1alpha1.Task, tasksPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	index := make(map[string]int, len(tasks))
	for i := range tasks {
		index[tasks[i].Name] = i
	}

	for i := range tasks {
		task := &tasks[i]
		dependsOnPath := tasksPath.Index(i).Child("dependsOn")
		for j, dep := range task.DependsOn {
			depPath := dependsOnPath.Index(j)
			if dep == task.Name {
				allErrs = append(allErrs, field.Invalid(depPath, dep, "task cannot depend on itself"))
				continue
			}
			if _, ok := index[dep]; !ok {
				allErrs = append(allErrs, field.NotFound(depPath, dep))
			}
		}
	}

	if hasDependencyCycle(tasks, index) {
		allErrs = append(allErrs, field.Invalid(tasksPath, "dependsOn",
			"dependsOn graph contains a cycle"))
	}

	return allErrs
}

// hasDependencyCycle reports whether the dependsOn graph (restricted to edges
// pointing at known tasks) contains a cycle, using iterative-free DFS coloring.
func hasDependencyCycle(tasks []v1alpha1.Task, index map[string]int) bool {
	const (
		white = 0 // unvisited
		gray  = 1 // on the current DFS stack
		black = 2 // fully explored
	)
	color := make([]int, len(tasks))

	var visit func(i int) bool
	visit = func(i int) bool {
		color[i] = gray
		for _, dep := range tasks[i].DependsOn {
			j, ok := index[dep]
			if !ok || j == i {
				continue
			}
			switch color[j] {
			case gray:
				return true
			case white:
				if visit(j) {
					return true
				}
			}
		}
		color[i] = black
		return false
	}

	for i := range tasks {
		if color[i] == white {
			if visit(i) {
				return true
			}
		}
	}
	return false
}

// validateObjectStorageClasses rejects reusing one explicit
// objectStorage.storageClass name for two different storage identities within a
// single LakeFlow. An explicit StorageClass encodes
// bucket/path/endpoint/region/credentials in its parameters, so the same name
// pointing at two different buckets would otherwise silently bind to whichever
// identity created the cluster-scoped StorageClass first (a correctness bug).
//
// Scope is intentionally per-LakeFlow: StorageClass is cluster-scoped and
// applyInfra is get-or-create, so cross-LakeFlow consistency of explicit names
// is the user's responsibility (documented). Omitting storageClass yields an
// operator-derived, content-addressed name that avoids this class of bug.
func validateObjectStorageClasses(lw *v1alpha1.LakeFlow) field.ErrorList {
	var allErrs field.ErrorList
	tasksPath := field.NewPath("spec", "tasks")
	seen := make(map[string]cloud.ObjectStorageIdentity)

	check := func(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, scPath *field.Path) {
		if objStorage == nil || objStorage.StorageClass == "" {
			return
		}
		// Compare StorageClass parameter identity only: size is a per-PVC
		// property, not a StorageClass one, so it must not count toward an
		// SC-name collision.
		id := cloud.NewObjectStorageIdentity(objStorage, credRef, lw.Namespace, cloudprofile.CloudProfile{})
		id.StorageSize = ""
		if prev, ok := seen[objStorage.StorageClass]; ok && prev != id {
			allErrs = append(allErrs, field.Invalid(scPath, objStorage.StorageClass,
				"storageClass is reused for two different object-storage identities within this LakeFlow; an explicit storageClass name must map to a single bucket/path/endpoint/region/credential identity"))
			return
		}
		seen[objStorage.StorageClass] = id
	}

	for i := range lw.Spec.Tasks {
		task := &lw.Spec.Tasks[i]
		if spark := task.TaskSpec.SparkExecutorSpec; spark != nil {
			check(spark.ObjectStorage, spark.CredentialsRef,
				tasksPath.Index(i).Child("sparkExecutor", "objectStorage", "storageClass"))
		}
		if cmd := task.TaskSpec.CommandExecutorSpec; cmd != nil {
			check(cmd.ObjectStorage, cmd.CredentialsRef,
				tasksPath.Index(i).Child("commandExecutor", "objectStorage", "storageClass"))
		}
	}

	return allErrs
}

// validateImmutableFields rejects changes to fields that cannot safely be
// mutated in place on an existing LakeFlow.
func validateImmutableFields(oldLw, newLw *v1alpha1.LakeFlow) field.ErrorList {
	var allErrs field.ErrorList
	specPath := field.NewPath("spec")

	// tenantKey feeds shared-resource isolation and content-addressed
	// PVC/StorageClass naming; changing it re-keys shared storage identity.
	if oldLw.Spec.TenantKey != newLw.Spec.TenantKey {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("tenantKey"),
			"tenantKey is immutable"))
	}

	// ttlStrategy is immutable once set (nil -> value allowed; value -> any
	// different value, including clearing, is rejected).
	if oldLw.Spec.TTLStrategy != nil && !reflect.DeepEqual(oldLw.Spec.TTLStrategy, newLw.Spec.TTLStrategy) {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("ttlStrategy"),
			"ttlStrategy is immutable once set"))
	}

	allErrs = append(allErrs, validateImmutableTasks(oldLw, newLw)...)
	return allErrs
}

// validateImmutableTasks applies per-task immutability for tasks that exist in
// both old and new (matched by name). A rename is represented as remove+add and
// is not separately rejected; add/remove are allowed.
func validateImmutableTasks(oldLw, newLw *v1alpha1.LakeFlow) field.ErrorList {
	var allErrs field.ErrorList
	tasksPath := field.NewPath("spec", "tasks")

	oldByName := make(map[string]*v1alpha1.Task, len(oldLw.Spec.Tasks))
	for i := range oldLw.Spec.Tasks {
		oldByName[oldLw.Spec.Tasks[i].Name] = &oldLw.Spec.Tasks[i]
	}

	for i := range newLw.Spec.Tasks {
		newTask := &newLw.Spec.Tasks[i]
		oldTask, ok := oldByName[newTask.Name]
		if !ok {
			// Added task: exempt from immutability (validated by create rules).
			continue
		}
		taskPath := tasksPath.Index(i)

		oldPath := executorPathKind(oldTask)
		newPath := executorPathKind(newTask)
		if oldPath != newPath {
			allErrs = append(allErrs, field.Forbidden(taskPath.Child("executor"),
				"executor path (spark/command) is immutable for an existing task name"))
			// Storage-class comparison below is only meaningful on a matching
			// executor path, so skip it when the path changed.
			continue
		}

		oldStorage, _ := taskObjectStorage(oldTask)
		newStorage, _ := taskObjectStorage(newTask)
		if oldStorage != nil && oldStorage.StorageClass != "" {
			newSC := ""
			if newStorage != nil {
				newSC = newStorage.StorageClass
			}
			if newSC != oldStorage.StorageClass {
				scPath := taskPath.Child(executorChildName(newPath), "objectStorage", "storageClass")
				allErrs = append(allErrs, field.Forbidden(scPath,
					"objectStorage.storageClass is immutable once explicitly set"))
			}
		}
	}

	return allErrs
}

// executorPathKind returns which executor spec a task uses, used as the task's
// stable shape for immutability checks.
func executorPathKind(task *v1alpha1.Task) string {
	switch {
	case task.TaskSpec.SparkExecutorSpec != nil && task.TaskSpec.CommandExecutorSpec != nil:
		return "both"
	case task.TaskSpec.SparkExecutorSpec != nil:
		return "spark"
	case task.TaskSpec.CommandExecutorSpec != nil:
		return "command"
	default:
		return "none"
	}
}

func executorChildName(kind string) string {
	if kind == "command" {
		return "commandExecutor"
	}
	return "sparkExecutor"
}

// taskObjectStorage returns the ObjectStorage and credential ref for whichever
// executor spec the task uses.
func taskObjectStorage(task *v1alpha1.Task) (*v1alpha1.ObjectStorage, *corev1.SecretReference) {
	if spark := task.TaskSpec.SparkExecutorSpec; spark != nil {
		return spark.ObjectStorage, spark.CredentialsRef
	}
	if cmd := task.TaskSpec.CommandExecutorSpec; cmd != nil {
		return cmd.ObjectStorage, cmd.CredentialsRef
	}
	return nil, nil
}

func hasResourceRequirements(r corev1.ResourceRequirements) bool {
	return len(r.Requests) > 0 || len(r.Limits) > 0
}
