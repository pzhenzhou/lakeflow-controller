package validation

import (
	"context"
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sparkExecutorWithStorage builds a fully-valid Spark executor so these tests
// exercise only the storageClass-collision rule, not unrelated required-field
// rules now shared with the webhook.
func sparkExecutorWithStorage(objStorage *v1alpha1.ObjectStorage) *v1alpha1.SparkExecutorSpec {
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
		ObjectStorage:       objStorage,
	}
}

func lakeFlowWithStorageClasses(a, b *v1alpha1.ObjectStorage) *v1alpha1.LakeFlow {
	return &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf", Namespace: "ns"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{
				{Name: "a", Executor: v1alpha1.TaskExecutorSpark, TaskSpec: v1alpha1.TaskSpec{SparkExecutorSpec: sparkExecutorWithStorage(a)}},
				{Name: "b", Executor: v1alpha1.TaskExecutorSpark, TaskSpec: v1alpha1.TaskSpec{SparkExecutorSpec: sparkExecutorWithStorage(b)}},
			},
		},
	}
}

func TestValidateRejectsStorageClassIdentityCollision(t *testing.T) {
	mgr := NewManager()
	// Same explicit storageClass name, different buckets -> collision.
	lw := lakeFlowWithStorageClasses(
		&v1alpha1.ObjectStorage{Bucket: "bucket-a", Path: "p", StorageClass: "shared-sc"},
		&v1alpha1.ObjectStorage{Bucket: "bucket-b", Path: "p", StorageClass: "shared-sc"},
	)
	err := mgr.Validate(context.Background(), lw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shared-sc")
}

func TestValidateAllowsSameStorageClassSameIdentity(t *testing.T) {
	mgr := NewManager()
	// Same explicit storageClass + same identity (size differs only) -> allowed.
	lw := lakeFlowWithStorageClasses(
		&v1alpha1.ObjectStorage{Bucket: "bucket", Path: "p", StorageClass: "shared-sc",
			MountSpec: &v1alpha1.ObjectStorageMount{StorageSize: resource.MustParse("20Gi")}},
		&v1alpha1.ObjectStorage{Bucket: "bucket", Path: "p", StorageClass: "shared-sc",
			MountSpec: &v1alpha1.ObjectStorageMount{StorageSize: resource.MustParse("50Gi")}},
	)
	assert.NoError(t, mgr.Validate(context.Background(), lw))
}

func TestValidateIgnoresDerivedStorageClasses(t *testing.T) {
	mgr := NewManager()
	// No explicit storageClass -> derived, no collision possible even with different buckets.
	lw := lakeFlowWithStorageClasses(
		&v1alpha1.ObjectStorage{Bucket: "bucket-a", Path: "p"},
		&v1alpha1.ObjectStorage{Bucket: "bucket-b", Path: "p"},
	)
	assert.NoError(t, mgr.Validate(context.Background(), lw))
}
