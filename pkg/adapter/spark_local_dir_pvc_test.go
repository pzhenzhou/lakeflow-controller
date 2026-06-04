package adapter

import (
	"testing"

	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetSparkLocalDirPVCOptionsFromLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		wantNil  bool
		wantErr  bool
		wantSize string
	}{
		{
			name:    "no labels",
			labels:  map[string]string{},
			wantNil: true,
		},
		{
			name: "valid ebs labels",
			labels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			wantSize: "20Gi",
		},
		{
			name: "missing type",
			labels: map[string]string{
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			wantErr: true,
		},
		{
			name: "unsupported type",
			labels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     "oss",
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			wantErr: true,
		},
		{
			name: "invalid size",
			labels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "bad-size",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, err := getSparkLocalDirPVCOptionsFromLabels(tt.labels)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			if tt.wantNil {
				assert.Nil(t, options)
				return
			}

			assert.NotNil(t, options)
			assert.True(t, options.Enabled)
			assert.Equal(t, tt.wantSize, options.SizeLimit)
		})
	}
}

func TestSetSparkSpecConfigWithSparkLocalDirPVCLabels(t *testing.T) {
	builder := &sparkTaskRenderer{}
	sparkApp := &sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
		},
	}

	profile := testAliyunProfile()
	builder.setSparkSpecConfig(sparkApp, v1alpha1.SparkExecutorSpec{}, newK8sCredentialResolver(""), profile)

	assert.Equal(t, sparkLocalDirPVCClaimName, sparkApp.Spec.SparkConf["spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName"])
	assert.Equal(t, profile.BlockStorage.DefaultStorageClass, sparkApp.Spec.SparkConf["spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass"])
	assert.Equal(t, "20Gi", sparkApp.Spec.SparkConf["spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit"])
	assert.Equal(t, DriverLocalDir, sparkApp.Spec.SparkConf["spark.kubernetes.driverEnv.SPARK_LOCAL_DIRS"])
	assert.Equal(t, sparkLocalDirPVCClaimName, sparkApp.Spec.SparkConf["spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName"])
	assert.Equal(t, profile.BlockStorage.DefaultStorageClass, sparkApp.Spec.SparkConf["spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass"])
	assert.Equal(t, "20Gi", sparkApp.Spec.SparkConf["spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit"])
	assert.Equal(t, ExecutorLocalDir, sparkApp.Spec.SparkConf["spark.kubernetes.executorEnv.SPARK_LOCAL_DIRS"])
	assert.NotContains(t, sparkApp.Spec.SparkConf, "spark.kubernetes.driver.volumes.emptyDir.spark-local-dir-1.mount.path")
	assert.NotContains(t, sparkApp.Spec.SparkConf, "spark.local.dir")
}

func TestSetSparkSpecVolumesPreservesOSSPVC(t *testing.T) {
	builder := &sparkTaskRenderer{}
	sparkApp := &sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
		},
	}

	ctx := &ArgoRenderContext{Namespace: "default", CloudProfile: testAliyunProfile()}
	builder.setSparkSpecVolumes(ctx, sparkApp, v1alpha1.SparkExecutorSpec{
		ObjectStorage: &v1alpha1.ObjectStorage{
			MountSpec: &v1alpha1.ObjectStorageMount{
				PVCName: "shared-oss-pvc",
			},
		},
	})

	if assert.Len(t, sparkApp.Spec.Volumes, 1) {
		if assert.NotNil(t, sparkApp.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim) {
			assert.Equal(t, "shared-oss-pvc", sparkApp.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName)
		}
	}
}

func TestExtractTrackedSparkPVCLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   map[string]string
	}{
		{
			name:   "nil labels",
			labels: nil,
			want:   map[string]string{},
		},
		{
			name:   "no tracked labels",
			labels: map[string]string{"other": "value"},
			want:   map[string]string{},
		},
		{
			name: "only spark-pvc",
			labels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS,
				"unrelated":                    "ignored",
			},
			want: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS,
			},
		},
		{
			name: "both tracked labels",
			labels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
				"app":                              "test",
			},
			want: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractTrackedSparkPVCLabels(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSparkPVCLabelsChanged(t *testing.T) {
	tests := []struct {
		name    string
		old     map[string]string
		new     map[string]string
		changed bool
	}{
		{
			name:    "both empty",
			old:     map[string]string{},
			new:     map[string]string{},
			changed: false,
		},
		{
			name: "identical tracked labels",
			old: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			new: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			changed: false,
		},
		{
			name: "unrelated labels differ, tracked same",
			old: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS,
				"app":                          "v1",
			},
			new: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS,
				"app":                          "v2",
			},
			changed: false,
		},
		{
			name: "label added",
			old:  map[string]string{},
			new: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			changed: true,
		},
		{
			name: "label removed",
			old: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			new:     map[string]string{},
			changed: true,
		},
		{
			name: "size changed",
			old: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			new: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "50Gi",
			},
			changed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SparkPVCLabelsChanged(tt.old, tt.new)
			assert.Equal(t, tt.changed, got)
		})
	}
}
