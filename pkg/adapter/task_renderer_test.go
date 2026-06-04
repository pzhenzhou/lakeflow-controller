package adapter

import (
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// renderTask is a small helper that mirrors the converter hot path: build the
// shared ArgoRenderContext once and dispatch through the TaskRendererFactory.
func renderTaskForTest(t *testing.T, lw *v1alpha1.LakeFlow, taskIdx int) (*argowfv1.Template, error) {
	t.Helper()
	profile, _ := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	ctx := newArgoRenderContext(lw, nil, profile)
	task := &lw.Spec.Tasks[taskIdx]
	renderer, err := GetTaskRendererFactory().CreateRenderer(task.Executor)
	require.NoError(t, err)
	require.NotNil(t, renderer)
	return renderer.RenderTask(&ctx, task)
}

func commandLakeFlow(executor v1alpha1.TaskExecutor, spec *v1alpha1.CommandExecutorSpec) *v1alpha1.LakeFlow {
	return &v1alpha1.LakeFlow{
		TypeMeta:   metav1.TypeMeta{APIVersion: "lakeflow.io/v1alpha1", Kind: "LakeFlow"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-wf", Namespace: "test-namespace"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{
				{Name: "task", Executor: executor, TaskSpec: v1alpha1.TaskSpec{CommandExecutorSpec: spec}},
			},
		},
	}
}

func sparkSpec() *v1alpha1.SparkExecutorSpec {
	return &v1alpha1.SparkExecutorSpec{
		MainClass:           "com.example.Main",
		MainApplicationFile: "local:///app.jar",
		QueueName:           "default",
		DriverResource: v1alpha1.SparkResource{
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}},
		},
		ExecutorResource: v1alpha1.SparkResource{
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}},
			Replicas: 1,
		},
	}
}

// TestTaskRendererFactory_DispatchAllExecutors verifies the factory dispatches by
// executor type and each renderer produces a template named after the task with
// the standard workflow/task labels. This replaces the factory-dispatch coverage
// previously provided by the deleted Build-based suites.
func TestTaskRendererFactory_DispatchAllExecutors(t *testing.T) {
	cases := []struct {
		name string
		lw   *v1alpha1.LakeFlow
	}{
		{
			name: "bash",
			lw: commandLakeFlow(v1alpha1.TaskExecutorBash, &v1alpha1.CommandExecutorSpec{
				InlineMode: &v1alpha1.InlineMode{Command: []string{"/bin/bash", "-c"}, Arguments: []string{"echo hi"}},
			}),
		},
		{
			name: "python",
			lw: commandLakeFlow(v1alpha1.TaskExecutorPython, &v1alpha1.CommandExecutorSpec{
				InlineMode: &v1alpha1.InlineMode{Command: []string{"python3", "-c"}, Arguments: []string{"print('hi')"}},
			}),
		},
		{
			name: "spark",
			lw: &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wf", Namespace: "test-namespace"},
				Spec: v1alpha1.LakeFlowSpec{Tasks: []v1alpha1.Task{
					{Name: "task", Executor: v1alpha1.TaskExecutorSpark, TaskSpec: v1alpha1.TaskSpec{SparkExecutorSpec: sparkSpec()}},
				}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			template, err := renderTaskForTest(t, tc.lw, 0)
			require.NoError(t, err)
			require.NotNil(t, template)
			assert.Equal(t, "task", template.Name)
			assert.Equal(t, "test-wf", template.Metadata.Labels[v1alpha1.WorkflowNameLabel])
			assert.Equal(t, "task", template.Metadata.Labels[v1alpha1.WorkflowTaskNameLabel])
		})
	}
}

// TestTaskRendererFactory_UnsupportedExecutor ensures the factory returns a
// non-nil error (and no renderer) for an unknown executor, so the converter's
// preserved error handling cannot nil-deref.
func TestTaskRendererFactory_UnsupportedExecutor(t *testing.T) {
	renderer, err := GetTaskRendererFactory().CreateRenderer(v1alpha1.TaskExecutor("bogus"))
	assert.Error(t, err)
	assert.Nil(t, renderer)
}

func TestCommandTaskRenderer_InlineMode(t *testing.T) {
	lw := commandLakeFlow(v1alpha1.TaskExecutorBash, &v1alpha1.CommandExecutorSpec{
		InlineMode: &v1alpha1.InlineMode{
			Command:   []string{"/bin/bash", "-c"},
			Arguments: []string{"echo 'Hello World'"},
		},
	})

	template, err := renderTaskForTest(t, lw, 0)
	require.NoError(t, err)
	require.NotNil(t, template.Container)
	assert.Equal(t, []string{"/bin/bash", "-c"}, template.Container.Command)
	assert.Equal(t, []string{"{{inputs.parameters.script}}"}, template.Container.Args)
	require.Len(t, template.Inputs.Parameters, 1)
	assert.Equal(t, "script", template.Inputs.Parameters[0].Name)
	require.NotNil(t, template.Inputs.Parameters[0].Value)
	assert.Equal(t, "echo 'Hello World'", string(*template.Inputs.Parameters[0].Value))
}

func TestCommandTaskRenderer_ScriptFileModeWithPVC(t *testing.T) {
	lw := commandLakeFlow(v1alpha1.TaskExecutorPython, &v1alpha1.CommandExecutorSpec{
		ScriptFileMode: &v1alpha1.ScriptFileMode{
			ScriptPath: "/mnt/oss/scripts/etl.py",
			Arguments:  []string{"--input=/data/raw"},
		},
		ObjectStorage: &v1alpha1.ObjectStorage{
			Bucket:       "my-bucket",
			Endpoint:     "oss-cn-hangzhou.aliyuncs.com",
			AccessMode:   "mount",
			StorageClass: "alicloud-oss",
			MountSpec:    &v1alpha1.ObjectStorageMount{PVCName: "shared-oss-pvc", AutoCreatePVC: true},
		},
	})

	template, err := renderTaskForTest(t, lw, 0)
	require.NoError(t, err)
	require.NotNil(t, template.Container)
	// Python executor runs scripts via python3, with scriptPath passed as a parameter.
	assert.Equal(t, []string{"python3"}, template.Container.Command)
	assert.Equal(t, "{{inputs.parameters.scriptPath}}", template.Container.Args[0])

	var ossMount *corev1.VolumeMount
	for i := range template.Container.VolumeMounts {
		if template.Container.VolumeMounts[i].Name == "oss-pvc" {
			ossMount = &template.Container.VolumeMounts[i]
			break
		}
	}
	require.NotNil(t, ossMount, "expected oss-pvc volume mount on container")
	assert.Equal(t, "/mnt/oss", ossMount.MountPath)
}

func TestCommandTaskRenderer_PythonProjectMode(t *testing.T) {
	lw := commandLakeFlow(v1alpha1.TaskExecutorPython, &v1alpha1.CommandExecutorSpec{
		PythonProjectMode: &v1alpha1.PythonProjectMode{
			ArchivePath: "/mnt/oss/projects/simple-etl.zip",
			EntryPoint:  "main.py",
			Arguments:   []string{"--flag"},
		},
	})

	template, err := renderTaskForTest(t, lw, 0)
	require.NoError(t, err)
	require.NotNil(t, template.Container)
	assert.Equal(t, []string{"/bin/bash", "-c"}, template.Container.Command)
	assert.Equal(t, []string{"{{inputs.parameters.projectScript}}"}, template.Container.Args)

	paramNames := map[string]bool{}
	for _, p := range template.Inputs.Parameters {
		paramNames[p.Name] = true
	}
	assert.True(t, paramNames["archivePath"], "expected archivePath parameter")
	assert.True(t, paramNames["entryPoint"], "expected entryPoint parameter")
	assert.True(t, paramNames["projectScript"], "expected projectScript parameter")
}

func TestCommandTaskRenderer_VolcanoQueueAnnotation(t *testing.T) {
	lw := commandLakeFlow(v1alpha1.TaskExecutorBash, &v1alpha1.CommandExecutorSpec{
		InlineMode: &v1alpha1.InlineMode{Command: []string{"/bin/bash", "-c"}, Arguments: []string{"echo hi"}},
	})
	lw.Spec.Tasks[0].TaskSpec.QueueName = "lakeflow-queue"

	template, err := renderTaskForTest(t, lw, 0)
	require.NoError(t, err)
	assert.Equal(t, v1alpha1.VolcanoSchedulerName, template.SchedulerName)
	require.NotNil(t, template.Metadata.Annotations)
	assert.Equal(t, "lakeflow-queue", template.Metadata.Annotations[v1alpha1.VolcanoQueueAnnotationKey])
}

// TestSparkTaskRenderer_TTLAndResourceTemplate verifies the Spark renderer wires
// the unified TTL strategy into the SparkApplication manifest carried by a
// ResourceTemplate.
func TestSparkTaskRenderer_TTLAndResourceTemplate(t *testing.T) {
	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-wf", Namespace: "test-namespace"},
		Spec: v1alpha1.LakeFlowSpec{
			TTLStrategy: &v1alpha1.LakeFlowTTLStrategy{SecondsAfterCompletion: 3600},
			Tasks: []v1alpha1.Task{
				{Name: "task", Executor: v1alpha1.TaskExecutorSpark, TaskSpec: v1alpha1.TaskSpec{SparkExecutorSpec: sparkSpec()}},
			},
		},
	}

	template, err := renderTaskForTest(t, lw, 0)
	require.NoError(t, err)
	require.NotNil(t, template.Resource)
	assert.Equal(t, "create", template.Resource.Action)
	assert.Contains(t, template.Resource.Manifest, "timeToLiveSeconds")
}

// TestTaskRenderer_RetryPolicy preserves the cross-executor maxRetries=0 coverage
// from the deleted retry_policy_integration_test.go: an explicit MaxRetries=0 must
// yield a RetryStrategy with Limit=0 (so Argo does not apply its default of 3),
// while a nil RetryPolicy yields no RetryStrategy.
func TestTaskRenderer_RetryPolicy(t *testing.T) {
	build := func(executor v1alpha1.TaskExecutor, retry *v1alpha1.TaskRetryPolicy) *argowfv1.Template {
		var spec v1alpha1.TaskSpec
		switch executor {
		case v1alpha1.TaskExecutorSpark:
			spec = v1alpha1.TaskSpec{SparkExecutorSpec: sparkSpec()}
		default:
			spec = v1alpha1.TaskSpec{CommandExecutorSpec: &v1alpha1.CommandExecutorSpec{
				InlineMode: &v1alpha1.InlineMode{Command: []string{"/bin/bash", "-c"}, Arguments: []string{"echo hi"}},
			}}
		}
		lw := &v1alpha1.LakeFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "test-wf", Namespace: "test-namespace"},
			Spec: v1alpha1.LakeFlowSpec{Tasks: []v1alpha1.Task{
				{Name: "task", Executor: executor, RetryPolicy: retry, TaskSpec: spec},
			}},
		}
		template, err := renderTaskForTest(t, lw, 0)
		require.NoError(t, err)
		return template
	}

	for _, executor := range []v1alpha1.TaskExecutor{v1alpha1.TaskExecutorBash, v1alpha1.TaskExecutorPython, v1alpha1.TaskExecutorSpark} {
		t.Run(string(executor)+"_maxRetries0", func(t *testing.T) {
			template := build(executor, &v1alpha1.TaskRetryPolicy{MaxRetries: 0})
			require.NotNil(t, template.RetryStrategy, "RetryStrategy must be set when maxRetries=0")
			require.NotNil(t, template.RetryStrategy.Limit)
			assert.Equal(t, int32(0), template.RetryStrategy.Limit.IntVal)
			assert.Equal(t, argowfv1.RetryPolicyOnFailure, template.RetryStrategy.RetryPolicy)
		})
	}

	t.Run("command_nil_retry_policy", func(t *testing.T) {
		template := build(v1alpha1.TaskExecutorBash, nil)
		assert.Nil(t, template.RetryStrategy, "RetryStrategy must be nil when retryPolicy is nil")
	})
}
