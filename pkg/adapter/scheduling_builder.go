package adapter

import (
	"encoding/json"
	"maps"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

const topologySpreadConstraints = "topologySpreadConstraints"
const sparkDriverContainerName = "spark-kubernetes-driver"
const sparkExecutorContainerName = "spark-kubernetes-executor"

var defaultNodeSelectors = map[string]string{
	"volcano.sh/nodegroup-name": "volcano-batch-job",
}

// mergedNodeSelector returns the operator default node selectors overlaid with
// the caller-provided selector. The result is always a fresh map so callers can
// mutate it without touching the shared defaults.
func mergedNodeSelector(nodeSelector map[string]string) map[string]string {
	merged := maps.Clone(defaultNodeSelectors)
	for key, value := range nodeSelector {
		merged[key] = value
	}
	return merged
}

// applyTaskSchedulingToSparkApplication maps the native pod scheduling fields onto
// the Spark driver and executor pods. Node selectors are written at the pod level
// (driver/executor), not the deprecated app-level spec.nodeSelector. queueName is
// the resolved Volcano queue; when non-empty it owns scheduler selection and any
// podScheduling.schedulerName is ignored.
func applyTaskSchedulingToSparkApplication(
	sparkApp *sparkv1beta2.SparkApplication,
	podScheduling *v1alpha1.TaskPodSchedulingSpec,
	queueName string,
) {
	nodeSelector := mergedNodeSelector(podScheduling.NodeSelector)
	sparkApp.Spec.Driver.NodeSelector = maps.Clone(nodeSelector)
	sparkApp.Spec.Executor.NodeSelector = maps.Clone(nodeSelector)

	sparkApp.Spec.Driver.Affinity = podScheduling.Affinity
	sparkApp.Spec.Executor.Affinity = podScheduling.Affinity

	sparkApp.Spec.Driver.Tolerations = append(sparkApp.Spec.Driver.Tolerations, podScheduling.Tolerations...)
	sparkApp.Spec.Executor.Tolerations = append(sparkApp.Spec.Executor.Tolerations, podScheduling.Tolerations...)

	// QueueName implies the Volcano batch scheduler; only honor an explicit
	// schedulerName when no queue is configured to avoid two writers.
	if queueName == "" && podScheduling.SchedulerName != "" {
		schedulerName := podScheduling.SchedulerName
		sparkApp.Spec.Driver.SchedulerName = &schedulerName
		sparkApp.Spec.Executor.SchedulerName = &schedulerName
	}
	if podScheduling.PriorityClassName != "" {
		priorityClassName := podScheduling.PriorityClassName
		sparkApp.Spec.Driver.PriorityClassName = &priorityClassName
		sparkApp.Spec.Executor.PriorityClassName = &priorityClassName
	}

	logger.Info(
		"Applied task scheduling to SparkApplication",
		"nodeSelectorCount", len(nodeSelector),
		"tolerationCount", len(podScheduling.Tolerations),
		"spreadConstraintCount", len(podScheduling.TopologySpreadConstraints),
	)

	if len(podScheduling.TopologySpreadConstraints) == 0 {
		return
	}

	if sparkApp.Spec.Driver.Template == nil {
		sparkApp.Spec.Driver.Template = &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: sparkDriverContainerName},
				},
			},
		}
	}
	if sparkApp.Spec.Executor.Template == nil {
		sparkApp.Spec.Executor.Template = &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: sparkExecutorContainerName},
				},
			},
		}
	}
	sparkApp.Spec.Driver.Template.Spec.TopologySpreadConstraints = append(
		sparkApp.Spec.Driver.Template.Spec.TopologySpreadConstraints,
		podScheduling.TopologySpreadConstraints...,
	)
	sparkApp.Spec.Executor.Template.Spec.TopologySpreadConstraints = append(
		sparkApp.Spec.Executor.Template.Spec.TopologySpreadConstraints,
		podScheduling.TopologySpreadConstraints...,
	)
}

// applyTaskSchedulingToArgoTemplate maps the native pod scheduling fields onto an
// Argo workflow template. Topology spread is applied via PodSpecPatch because the
// Argo template has no native topologySpreadConstraints field. queueName is the
// resolved Volcano queue; when non-empty the caller has already set the Volcano
// scheduler, so podScheduling.schedulerName is ignored here.
func applyTaskSchedulingToArgoTemplate(
	template *argowfv1.Template,
	podScheduling *v1alpha1.TaskPodSchedulingSpec,
	queueName string,
) {
	template.NodeSelector = mergedNodeSelector(podScheduling.NodeSelector)
	template.Affinity = podScheduling.Affinity
	template.Tolerations = podScheduling.Tolerations

	if queueName == "" && podScheduling.SchedulerName != "" {
		template.SchedulerName = podScheduling.SchedulerName
	}
	if podScheduling.PriorityClassName != "" {
		template.PriorityClassName = podScheduling.PriorityClassName
	}

	logger.Info(
		"Applied task scheduling to Argo template",
		"templateName", template.Name,
		"nodeSelectorCount", len(template.NodeSelector),
		"tolerationCount", len(podScheduling.Tolerations),
		"spreadConstraintCount", len(podScheduling.TopologySpreadConstraints),
	)

	if len(podScheduling.TopologySpreadConstraints) == 0 {
		return
	}

	patchStruct := map[string]any{
		topologySpreadConstraints: podScheduling.TopologySpreadConstraints,
	}
	patchBytes, err := json.Marshal(patchStruct)
	if err != nil {
		logger.Error(err, "Failed to build topology spread constraints")
		return
	}
	template.PodSpecPatch = string(patchBytes)
}
