package validation

import (
	"strings"
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validSparkExecutor() *v1alpha1.SparkExecutorSpec {
	res := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}
	return &v1alpha1.SparkExecutorSpec{
		Image:               "spark:3.5.6",
		MainClass:           "com.example.Main",
		MainApplicationFile: "local:///mnt/spark/app.jar",
		QueueName:           "default",
		DriverResource:      v1alpha1.SparkResource{Resources: res},
		ExecutorResource:    v1alpha1.SparkResource{Resources: res},
	}
}

func validCommandExecutor() *v1alpha1.CommandExecutorSpec {
	return &v1alpha1.CommandExecutorSpec{
		Image:      "bash:5",
		InlineMode: &v1alpha1.InlineMode{Command: []string{"/bin/bash", "-c", "echo hi"}},
	}
}

func validSparkTask(name string) v1alpha1.Task {
	return v1alpha1.Task{
		Name:     name,
		Executor: v1alpha1.TaskExecutorSpark,
		TaskSpec: v1alpha1.TaskSpec{SparkExecutorSpec: validSparkExecutor()},
	}
}

func validLakeFlow() *v1alpha1.LakeFlow {
	return &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf", Namespace: "ns"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{validSparkTask("task-a")},
		},
	}
}

// assertHasFieldError fails unless an error whose field path contains the given
// substring is present in the list.
func assertHasFieldError(t *testing.T, lw *v1alpha1.LakeFlow, fieldSubstr string) {
	t.Helper()
	errs := ValidateCreate(lw)
	require.NotEmpty(t, errs, "expected validation errors")
	for _, e := range errs {
		if strings.Contains(e.Field, fieldSubstr) {
			return
		}
	}
	t.Fatalf("expected a field error containing %q, got: %v", fieldSubstr, errs)
}

func TestValidateCreate_Valid(t *testing.T) {
	assert.Empty(t, ValidateCreate(validLakeFlow()))
}

func TestValidateCreate_EmptyTriggerIsImmediate(t *testing.T) {
	lw := validLakeFlow() // no schedule, no dependency
	assert.Empty(t, ValidateCreate(lw))
}

func TestValidateCreate_TooManyTasks(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks = nil
	for i := 0; i < MaxTasksPerWorkflow+1; i++ {
		lw.Spec.Tasks = append(lw.Spec.Tasks, validSparkTask("task-"+string(rune('a'+i%26))+string(rune('a'+i/26))))
	}
	assertHasFieldError(t, lw, "spec.tasks")
}

func TestValidateCreate_NoTasks(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks = nil
	assertHasFieldError(t, lw, "spec.tasks")
}

func TestValidateCreate_TaskNameTooLong(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].Name = strings.Repeat("a", MaxTaskNameLength+1)
	assertHasFieldError(t, lw, "tasks[0].name")
}

func TestValidateCreate_TaskNameNotRFC1123(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].Name = "Bad_Name"
	assertHasFieldError(t, lw, "tasks[0].name")
}

func TestValidateCreate_DuplicateTaskNames(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks = []v1alpha1.Task{validSparkTask("dup"), validSparkTask("dup")}
	assertHasFieldError(t, lw, "tasks[1].name")
}

func TestValidateCreate_WorkflowNameTooLong(t *testing.T) {
	lw := validLakeFlow()
	lw.Name = strings.Repeat("a", MaxWorkflowNameLength+1)
	assertHasFieldError(t, lw, "metadata.name")
}

func TestValidateCreate_ScheduleAndDependencyMutuallyExclusive(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.WorkflowTrigger.Schedule.Cron = "* * * * *"
	lw.Spec.WorkflowTrigger.Depend.Workflows = []v1alpha1.Upstream{{Name: "u", Namespace: "ns"}}
	assertHasFieldError(t, lw, "spec.trigger")
}

func TestValidateCreate_DependUpstreamRequiresNameNamespace(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.WorkflowTrigger.Depend.Workflows = []v1alpha1.Upstream{{Name: "", Namespace: ""}}
	errs := ValidateCreate(lw)
	require.NotEmpty(t, errs)
}

func TestValidateCreate_NoExecutorSpec(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec = nil
	assertHasFieldError(t, lw, "tasks[0]")
}

func TestValidateCreate_BothExecutorSpecs(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].TaskSpec.CommandExecutorSpec = validCommandExecutor()
	assertHasFieldError(t, lw, "tasks[0]")
}

func TestValidateCreate_ExecutorSpecMismatch(t *testing.T) {
	lw := validLakeFlow()
	// executor=spark but only a command spec is set
	lw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec = nil
	lw.Spec.Tasks[0].TaskSpec.CommandExecutorSpec = validCommandExecutor()
	assertHasFieldError(t, lw, "tasks[0].executor")
}

func TestValidateCreate_SparkMissingRequiredFields(t *testing.T) {
	cases := map[string]func(*v1alpha1.SparkExecutorSpec){
		"sparkExecutor.image":                      func(s *v1alpha1.SparkExecutorSpec) { s.Image = "" },
		"sparkExecutor.mainApplicationFile":        func(s *v1alpha1.SparkExecutorSpec) { s.MainApplicationFile = "" },
		"sparkExecutor.queueName":                  func(s *v1alpha1.SparkExecutorSpec) { s.QueueName = "" },
		"sparkExecutor.driverResource.resources":   func(s *v1alpha1.SparkExecutorSpec) { s.DriverResource = v1alpha1.SparkResource{} },
		"sparkExecutor.executorResource.resources": func(s *v1alpha1.SparkExecutorSpec) { s.ExecutorResource = v1alpha1.SparkResource{} },
	}
	for fieldName, mutate := range cases {
		t.Run(fieldName, func(t *testing.T) {
			lw := validLakeFlow()
			mutate(lw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec)
			assertHasFieldError(t, lw, fieldName)
		})
	}
}

func TestValidateCreate_JavaRequiresMainClass(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.SparkAppType = v1alpha1.SparkAppTypeJava
	lw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.MainClass = ""
	assertHasFieldError(t, lw, "sparkExecutor.mainClass")
}

func TestValidateCreate_PythonForbidsMainClass(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.SparkAppType = v1alpha1.SparkAppTypePython
	lw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.MainClass = "Main"
	assertHasFieldError(t, lw, "sparkExecutor.mainClass")
}

func TestValidateCreate_CommandExactlyOneMode(t *testing.T) {
	lw := validLakeFlow()
	cmd := validCommandExecutor()
	cmd.ScriptFileMode = &v1alpha1.ScriptFileMode{ScriptPath: "/x.sh"} // now two modes
	lw.Spec.Tasks[0] = v1alpha1.Task{Name: "c", Executor: v1alpha1.TaskExecutorBash, TaskSpec: v1alpha1.TaskSpec{CommandExecutorSpec: cmd}}
	assertHasFieldError(t, lw, "commandExecutor")
}

func TestValidateCreate_PythonProjectModeRequiresPythonExecutor(t *testing.T) {
	lw := validLakeFlow()
	cmd := &v1alpha1.CommandExecutorSpec{
		Image:             "py:3",
		PythonProjectMode: &v1alpha1.PythonProjectMode{ArchivePath: "/a.zip", EntryPoint: "main.py"},
	}
	lw.Spec.Tasks[0] = v1alpha1.Task{Name: "c", Executor: v1alpha1.TaskExecutorBash, TaskSpec: v1alpha1.TaskSpec{CommandExecutorSpec: cmd}}
	assertHasFieldError(t, lw, "commandExecutor.pythonProjectMode")
}

func TestValidateCreate_DependsOnSelf(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].DependsOn = []string{lw.Spec.Tasks[0].Name}
	assertHasFieldError(t, lw, "dependsOn")
}

func TestValidateCreate_DependsOnDangling(t *testing.T) {
	lw := validLakeFlow()
	lw.Spec.Tasks[0].DependsOn = []string{"nope"}
	assertHasFieldError(t, lw, "dependsOn")
}

func TestValidateCreate_DependsOnCycle(t *testing.T) {
	a := validSparkTask("a")
	b := validSparkTask("b")
	a.DependsOn = []string{"b"}
	b.DependsOn = []string{"a"}
	lw := validLakeFlow()
	lw.Spec.Tasks = []v1alpha1.Task{a, b}
	assertHasFieldError(t, lw, "spec.tasks")
}

func TestValidateCreate_StorageClassCollision(t *testing.T) {
	lw := validLakeFlow()
	a := validSparkTask("a")
	b := validSparkTask("b")
	a.TaskSpec.SparkExecutorSpec.ObjectStorage = &v1alpha1.ObjectStorage{Bucket: "ba", Path: "p", StorageClass: "shared"}
	b.TaskSpec.SparkExecutorSpec.ObjectStorage = &v1alpha1.ObjectStorage{Bucket: "bb", Path: "p", StorageClass: "shared"}
	lw.Spec.Tasks = []v1alpha1.Task{a, b}
	assertHasFieldError(t, lw, "storageClass")
}

func TestValidateUpdate_TenantKeyImmutable(t *testing.T) {
	oldLw := validLakeFlow()
	oldLw.Spec.TenantKey = "t1"
	newLw := validLakeFlow()
	newLw.Spec.TenantKey = "t2"
	errs := ValidateUpdate(oldLw, newLw)
	requireFieldError(t, errs, "spec.tenantKey")
}

func TestValidateUpdate_TriggerMutable(t *testing.T) {
	oldLw := validLakeFlow() // immediate
	newLw := validLakeFlow()
	newLw.Spec.WorkflowTrigger.Schedule.Cron = "* * * * *"
	assert.Empty(t, ValidateUpdate(oldLw, newLw))
}

func TestValidateUpdate_TTLImmutableOnceSet(t *testing.T) {
	oldLw := validLakeFlow()
	oldLw.Spec.TTLStrategy = &v1alpha1.LakeFlowTTLStrategy{SecondsAfterCompletion: 100}
	newLw := validLakeFlow()
	newLw.Spec.TTLStrategy = &v1alpha1.LakeFlowTTLStrategy{SecondsAfterCompletion: 200}
	requireFieldError(t, ValidateUpdate(oldLw, newLw), "spec.ttlStrategy")
}

func TestValidateUpdate_TTLNilToValueAllowed(t *testing.T) {
	oldLw := validLakeFlow() // nil TTL
	newLw := validLakeFlow()
	newLw.Spec.TTLStrategy = &v1alpha1.LakeFlowTTLStrategy{SecondsAfterCompletion: 200}
	assert.Empty(t, ValidateUpdate(oldLw, newLw))
}

func TestValidateUpdate_TTLClearingRejected(t *testing.T) {
	oldLw := validLakeFlow()
	oldLw.Spec.TTLStrategy = &v1alpha1.LakeFlowTTLStrategy{SecondsAfterCompletion: 100}
	newLw := validLakeFlow() // cleared
	requireFieldError(t, ValidateUpdate(oldLw, newLw), "spec.ttlStrategy")
}

func TestValidateUpdate_ExecutorPathImmutable(t *testing.T) {
	oldLw := validLakeFlow() // task-a is spark
	newLw := validLakeFlow()
	newLw.Spec.Tasks[0] = v1alpha1.Task{
		Name:     "task-a",
		Executor: v1alpha1.TaskExecutorBash,
		TaskSpec: v1alpha1.TaskSpec{CommandExecutorSpec: validCommandExecutor()},
	}
	requireFieldError(t, ValidateUpdate(oldLw, newLw), "tasks[0].executor")
}

func TestValidateUpdate_StorageClassImmutableOnceSet(t *testing.T) {
	oldLw := validLakeFlow()
	oldLw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.ObjectStorage = &v1alpha1.ObjectStorage{Bucket: "b", Path: "p", StorageClass: "sc1"}
	newLw := validLakeFlow()
	newLw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.ObjectStorage = &v1alpha1.ObjectStorage{Bucket: "b", Path: "p", StorageClass: "sc2"}
	requireFieldError(t, ValidateUpdate(oldLw, newLw), "storageClass")
}

func TestValidateUpdate_StorageClassEmptyToSetAllowed(t *testing.T) {
	oldLw := validLakeFlow()
	oldLw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.ObjectStorage = &v1alpha1.ObjectStorage{Bucket: "b", Path: "p"}
	newLw := validLakeFlow()
	newLw.Spec.Tasks[0].TaskSpec.SparkExecutorSpec.ObjectStorage = &v1alpha1.ObjectStorage{Bucket: "b", Path: "p", StorageClass: "sc1"}
	assert.Empty(t, ValidateUpdate(oldLw, newLw))
}

func TestValidateUpdate_AddRemoveTaskAllowed(t *testing.T) {
	oldLw := validLakeFlow() // task-a
	newLw := validLakeFlow()
	newLw.Spec.Tasks = []v1alpha1.Task{validSparkTask("task-b")} // removed a, added b
	assert.Empty(t, ValidateUpdate(oldLw, newLw))
}

func requireFieldError(t *testing.T, errs field.ErrorList, fieldSubstr string) {
	t.Helper()
	agg := errs.ToAggregate()
	require.Error(t, agg)
	assert.Contains(t, agg.Error(), fieldSubstr)
}
