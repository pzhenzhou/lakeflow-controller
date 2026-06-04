package adapter

import (
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// TestSparkAndCommandShareDerivedOSSPVCName is the critical cross-renderer guard:
// when pvcName is omitted, the Spark renderer, the command renderer, and the
// provider must all compute the SAME derived claim name from the storage identity,
// or pods would reference a PVC the reconciler never created.
func TestSparkAndCommandShareDerivedOSSPVCName(t *testing.T) {
	profile := testAliyunProfile()
	ctx := &ArgoRenderContext{Namespace: "team-ns", CloudProfile: profile}

	obj := &v1alpha1.ObjectStorage{
		Bucket:    "shared-bucket",
		Path:      "envs/jars",
		MountSpec: &v1alpha1.ObjectStorageMount{}, // derived name
	}
	expected := cloud.EffectiveObjectStoragePVCName(obj, nil, ctx.Namespace, profile)
	require.NotEmpty(t, expected)

	// Spark renderer.
	sparkApp := &sparkv1beta2.SparkApplication{}
	(&sparkTaskRenderer{}).setSparkSpecVolumes(ctx, sparkApp, v1alpha1.SparkExecutorSpec{ObjectStorage: obj})
	require.Len(t, sparkApp.Spec.Volumes, 1)
	require.NotNil(t, sparkApp.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim)
	assert.Equal(t, expected, sparkApp.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName)

	// Command renderer.
	cmdCtx := &ArgoRenderContext{
		LakeFlow:     &v1alpha1.LakeFlow{},
		Namespace:    "team-ns",
		CloudProfile: profile,
	}
	template := &argowfv1.Template{Container: &corev1.Container{}}
	err := (&commandTaskRenderer{}).handleCommandTaskPVC(
		cmdCtx,
		&v1alpha1.Task{Name: "cmd"},
		&v1alpha1.CommandExecutorSpec{ObjectStorage: obj},
		template,
	)
	require.NoError(t, err)
	require.Len(t, template.Volumes, 1)
	require.NotNil(t, template.Volumes[0].VolumeSource.PersistentVolumeClaim)
	assert.Equal(t, expected, template.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName,
		"command and spark renderers must agree on the derived claim name")
}

func TestExplicitOSSPVCNameHonoredByBothRenderers(t *testing.T) {
	profile := testAliyunProfile()
	ctx := &ArgoRenderContext{Namespace: "ns", CloudProfile: profile}
	obj := &v1alpha1.ObjectStorage{
		Bucket:    "b",
		Path:      "p",
		MountSpec: &v1alpha1.ObjectStorageMount{PVCName: "explicit-pvc"},
	}

	sparkApp := &sparkv1beta2.SparkApplication{}
	(&sparkTaskRenderer{}).setSparkSpecVolumes(ctx, sparkApp, v1alpha1.SparkExecutorSpec{ObjectStorage: obj})
	require.Len(t, sparkApp.Spec.Volumes, 1)
	assert.Equal(t, "explicit-pvc", sparkApp.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName)

	cmdCtx := &ArgoRenderContext{LakeFlow: &v1alpha1.LakeFlow{}, Namespace: "ns", CloudProfile: profile}
	template := &argowfv1.Template{Container: &corev1.Container{}}
	require.NoError(t, (&commandTaskRenderer{}).handleCommandTaskPVC(
		cmdCtx, &v1alpha1.Task{Name: "cmd"},
		&v1alpha1.CommandExecutorSpec{ObjectStorage: obj}, template,
	))
	require.Len(t, template.Volumes, 1)
	assert.Equal(t, "explicit-pvc", template.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName)
}
