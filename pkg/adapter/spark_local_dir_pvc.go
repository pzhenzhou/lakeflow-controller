package adapter

import (
	"fmt"
	"strings"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	sparkLocalDirPVCClaimName = "OnDemand"
)

// trackedSparkPVCLabelKeys is the single source of truth for which labels
// the controller predicate and reconciler drift check must track.
var trackedSparkPVCLabelKeys = []string{
	v1alpha1.SparkLocalDirPVCLabel,
	v1alpha1.SparkLocalDirPVCSizeLabel,
}

type sparkLocalDirPVCOptions struct {
	Enabled   bool
	SizeLimit string
}

// SparkLocalDirRequest parses LakeFlow labels into the cloud-layer request used
// for workflow storage description.
func SparkLocalDirRequest(workflow *v1alpha1.LakeFlow) (*cloud.BlockStorageRequest, error) {
	options, err := getSparkLocalDirPVCOptions(workflow)
	if err != nil || options == nil {
		return nil, err
	}
	return &cloud.BlockStorageRequest{
		Enabled:   options.Enabled,
		SizeLimit: options.SizeLimit,
	}, nil
}

func getSparkLocalDirPVCOptions(workflow *v1alpha1.LakeFlow) (*sparkLocalDirPVCOptions, error) {
	if workflow == nil {
		return nil, nil
	}

	return getSparkLocalDirPVCOptionsFromLabels(workflow.GetLabels())
}

func getSparkLocalDirPVCOptionsFromLabels(labels map[string]string) (*sparkLocalDirPVCOptions, error) {
	if len(labels) == 0 {
		return nil, nil
	}

	pvcType := strings.TrimSpace(labels[v1alpha1.SparkLocalDirPVCLabel])
	sizeLimit := strings.TrimSpace(labels[v1alpha1.SparkLocalDirPVCSizeLabel])
	if pvcType == "" && sizeLimit == "" {
		return nil, nil
	}

	if pvcType == "" {
		return nil, fmt.Errorf("%s requires %s", v1alpha1.SparkLocalDirPVCSizeLabel, v1alpha1.SparkLocalDirPVCLabel)
	}
	if pvcType != v1alpha1.SparkLocalDirPVCTypeEBS {
		return nil, fmt.Errorf("%s only supports %q, got %q", v1alpha1.SparkLocalDirPVCLabel, v1alpha1.SparkLocalDirPVCTypeEBS, pvcType)
	}
	if sizeLimit == "" {
		return nil, fmt.Errorf("%s=%s requires %s", v1alpha1.SparkLocalDirPVCLabel, pvcType, v1alpha1.SparkLocalDirPVCSizeLabel)
	}
	if _, err := resource.ParseQuantity(sizeLimit); err != nil {
		return nil, fmt.Errorf("invalid %s value %q: %w", v1alpha1.SparkLocalDirPVCSizeLabel, sizeLimit, err)
	}

	return &sparkLocalDirPVCOptions{
		Enabled:   true,
		SizeLimit: sizeLimit,
	}, nil
}

// ExtractTrackedSparkPVCLabels returns only the Spark PVC labels from a label set.
func ExtractTrackedSparkPVCLabels(labels map[string]string) map[string]string {
	tracked := make(map[string]string, len(trackedSparkPVCLabelKeys))
	for _, key := range trackedSparkPVCLabelKeys {
		if value, ok := labels[key]; ok {
			tracked[key] = value
		}
	}
	return tracked
}

// SparkPVCLabelsChanged reports whether the tracked Spark PVC labels differ
// between two label sets (e.g. LakeFlow vs WorkflowTemplate).
func SparkPVCLabelsChanged(oldLabels, newLabels map[string]string) bool {
	oldTracked := ExtractTrackedSparkPVCLabels(oldLabels)
	newTracked := ExtractTrackedSparkPVCLabels(newLabels)

	if len(oldTracked) != len(newTracked) {
		return true
	}
	for key, value := range oldTracked {
		if newTracked[key] != value {
			return true
		}
	}
	return false
}

func addSparkLocalDirPVCLabels(target, source map[string]string) map[string]string {
	if target == nil {
		target = make(map[string]string)
	}
	if len(source) == 0 {
		return target
	}

	for _, key := range trackedSparkPVCLabelKeys {
		if value := strings.TrimSpace(source[key]); value != "" {
			target[key] = value
		}
	}

	return target
}

func buildSparkLocalDirPVCConfig(options *sparkLocalDirPVCOptions, storageClass string) map[string]string {
	if options == nil || !options.Enabled {
		return nil
	}

	return map[string]string{
		"spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName":      sparkLocalDirPVCClaimName,
		"spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass":   storageClass,
		"spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit":      options.SizeLimit,
		"spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path":             DriverLocalDir,
		"spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly":         "false",
		"spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName":    sparkLocalDirPVCClaimName,
		"spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass": storageClass,
		"spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit":    options.SizeLimit,
		"spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path":           ExecutorLocalDir,
		"spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly":       "false",
		// Driver and executor use different mount paths, so rely on role-specific env vars
		// instead of a single spark.local.dir value that would be wrong for one side.
		"spark.kubernetes.driverEnv.SPARK_LOCAL_DIRS":   DriverLocalDir,
		"spark.kubernetes.executorEnv.SPARK_LOCAL_DIRS": ExecutorLocalDir,
	}
}
