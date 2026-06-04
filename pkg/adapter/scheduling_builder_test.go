package adapter

import (
	"encoding/json"
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func sampleAffinity() *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{Key: "nodepool", Operator: corev1.NodeSelectorOpIn, Values: []string{"np-a"}},
						},
					},
				},
			},
		},
	}
}

func sampleSpreadConstraints() []corev1.TopologySpreadConstraint {
	return []corev1.TopologySpreadConstraint{
		{
			MaxSkew:           1,
			TopologyKey:       "topology.kubernetes.io/zone",
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{v1alpha1.WorkflowNameLabel: "wf-a"},
			},
		},
	}
}

func TestApplyTaskSchedulingToSparkApplication_PodLevelNodeSelectorAndPassthrough(t *testing.T) {
	sparkApp := &sparkv1beta2.SparkApplication{}
	podScheduling := &v1alpha1.TaskPodSchedulingSpec{
		NodeSelector: map[string]string{"nodepool": "np-a"},
		Affinity:     sampleAffinity(),
		Tolerations: []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "batch", Effect: corev1.TaintEffectNoSchedule},
		},
		TopologySpreadConstraints: sampleSpreadConstraints(),
		PriorityClassName:         "high-priority",
	}

	applyTaskSchedulingToSparkApplication(sparkApp, podScheduling, "")

	// Pod-level node selectors merge defaults + user; app-level must stay unset.
	assert.Nil(t, sparkApp.Spec.NodeSelector, "deprecated app-level nodeSelector must not be set")
	assert.Equal(t, "np-a", sparkApp.Spec.Driver.NodeSelector["nodepool"])
	assert.Equal(t, "volcano-batch-job", sparkApp.Spec.Driver.NodeSelector["volcano.sh/nodegroup-name"])
	assert.Equal(t, "np-a", sparkApp.Spec.Executor.NodeSelector["nodepool"])
	assert.Equal(t, "volcano-batch-job", sparkApp.Spec.Executor.NodeSelector["volcano.sh/nodegroup-name"])

	assert.Equal(t, podScheduling.Affinity, sparkApp.Spec.Driver.Affinity)
	assert.Equal(t, podScheduling.Affinity, sparkApp.Spec.Executor.Affinity)
	assert.Len(t, sparkApp.Spec.Driver.Tolerations, 1)
	assert.Len(t, sparkApp.Spec.Executor.Tolerations, 1)

	if assert.NotNil(t, sparkApp.Spec.Driver.Template) {
		assert.Len(t, sparkApp.Spec.Driver.Template.Spec.TopologySpreadConstraints, 1)
	}
	if assert.NotNil(t, sparkApp.Spec.Executor.Template) {
		assert.Len(t, sparkApp.Spec.Executor.Template.Spec.TopologySpreadConstraints, 1)
	}

	if assert.NotNil(t, sparkApp.Spec.Driver.PriorityClassName) {
		assert.Equal(t, "high-priority", *sparkApp.Spec.Driver.PriorityClassName)
	}
	if assert.NotNil(t, sparkApp.Spec.Executor.PriorityClassName) {
		assert.Equal(t, "high-priority", *sparkApp.Spec.Executor.PriorityClassName)
	}
}

func TestApplyTaskSchedulingToSparkApplication_SchedulerNameIgnoredWhenQueueSet(t *testing.T) {
	sparkApp := &sparkv1beta2.SparkApplication{}
	podScheduling := &v1alpha1.TaskPodSchedulingSpec{SchedulerName: "custom-scheduler"}

	applyTaskSchedulingToSparkApplication(sparkApp, podScheduling, "my-queue")
	assert.Nil(t, sparkApp.Spec.Driver.SchedulerName, "schedulerName must be ignored when QueueName is set")
	assert.Nil(t, sparkApp.Spec.Executor.SchedulerName)

	sparkApp = &sparkv1beta2.SparkApplication{}
	applyTaskSchedulingToSparkApplication(sparkApp, podScheduling, "")
	if assert.NotNil(t, sparkApp.Spec.Driver.SchedulerName) {
		assert.Equal(t, "custom-scheduler", *sparkApp.Spec.Driver.SchedulerName)
	}
	if assert.NotNil(t, sparkApp.Spec.Executor.SchedulerName) {
		assert.Equal(t, "custom-scheduler", *sparkApp.Spec.Executor.SchedulerName)
	}
}

func TestApplyTaskSchedulingToArgoTemplate_Passthrough(t *testing.T) {
	template := &argowfv1.Template{}
	podScheduling := &v1alpha1.TaskPodSchedulingSpec{
		NodeSelector:              map[string]string{"nodepool": "np-a"},
		Affinity:                  sampleAffinity(),
		Tolerations:               []corev1.Toleration{{Key: "dedicated", Value: "batch", Effect: corev1.TaintEffectNoSchedule}},
		TopologySpreadConstraints: sampleSpreadConstraints(),
		SchedulerName:             "custom-scheduler",
		PriorityClassName:         "high-priority",
	}

	applyTaskSchedulingToArgoTemplate(template, podScheduling, "")

	assert.Equal(t, "np-a", template.NodeSelector["nodepool"])
	assert.Equal(t, "volcano-batch-job", template.NodeSelector["volcano.sh/nodegroup-name"])
	assert.Equal(t, podScheduling.Affinity, template.Affinity)
	assert.Len(t, template.Tolerations, 1)
	assert.Equal(t, "custom-scheduler", template.SchedulerName)
	assert.Equal(t, "high-priority", template.PriorityClassName)
	assert.NotEmpty(t, template.PodSpecPatch)

	var patch map[string]any
	assert.NoError(t, json.Unmarshal([]byte(template.PodSpecPatch), &patch))
	assert.Contains(t, patch, topologySpreadConstraints)
}

func TestApplyTaskSchedulingToArgoTemplate_QueueOwnsSchedulerName(t *testing.T) {
	template := &argowfv1.Template{SchedulerName: "volcano"}
	podScheduling := &v1alpha1.TaskPodSchedulingSpec{SchedulerName: "custom-scheduler"}

	applyTaskSchedulingToArgoTemplate(template, podScheduling, "my-queue")
	assert.Equal(t, "volcano", template.SchedulerName, "QueueName-driven scheduler must not be overridden")
}
