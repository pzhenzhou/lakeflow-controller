package cloud

import (
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProviderDescribeWorkflowStorageDeepCopiesProfileMaps(t *testing.T) {
	profile, err := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	require.NoError(t, err)
	provider := NewProvider(profile)

	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{
				{
					Name: "spark",
					TaskSpec: v1alpha1.TaskSpec{
						SparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
							ObjectStorage: &v1alpha1.ObjectStorage{
								Bucket:       "bucket",
								Path:         "path",
								StorageClass: "oss-sc",
								MountSpec: &v1alpha1.ObjectStorageMount{
									PVCName:       "oss-pvc",
									AutoCreatePVC: true,
								},
							},
						},
					},
				},
			},
		},
	}

	first, err := provider.DescribeWorkflowStorage(lw, &BlockStorageRequest{Enabled: true, SizeLimit: "20Gi"})
	require.NoError(t, err)
	require.Len(t, first.StorageClasses, 2)
	first.StorageClasses[0].Parameters["bucket"] = "mutated"
	first.StorageClasses[1].Parameters["type"] = "mutated"

	second, err := provider.DescribeWorkflowStorage(lw, &BlockStorageRequest{Enabled: true, SizeLimit: "20Gi"})
	require.NoError(t, err)
	assert.Equal(t, "bucket", second.StorageClasses[0].Parameters["bucket"])
	assert.Equal(t, "cloud_essd", second.StorageClasses[1].Parameters["type"])
}

func sparkTaskWithObjectStorage(name string, obj *v1alpha1.ObjectStorage) v1alpha1.Task {
	return v1alpha1.Task{
		Name: name,
		TaskSpec: v1alpha1.TaskSpec{
			SparkExecutorSpec: &v1alpha1.SparkExecutorSpec{ObjectStorage: obj},
		},
	}
}

func TestDescribeWorkflowStorageSharesDerivedPVCAcrossTasks(t *testing.T) {
	provider := NewAliyunProvider()

	// Two tasks with identical object storage and NO explicit pvcName/storageClass.
	// The derived, content-addressed names must collapse to a single shared PVC and
	// a single shared StorageClass for the whole workflow.
	newObj := func() *v1alpha1.ObjectStorage {
		return &v1alpha1.ObjectStorage{
			Bucket:    "bucket",
			Path:      "path",
			MountSpec: &v1alpha1.ObjectStorageMount{}, // no pvcName -> derived
		}
	}
	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf", Namespace: "default"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{
				sparkTaskWithObjectStorage("a", newObj()),
				sparkTaskWithObjectStorage("b", newObj()),
			},
		},
	}

	storage, err := provider.DescribeWorkflowStorage(lw, nil)
	require.NoError(t, err)
	require.Len(t, storage.PVCs, 1, "identical derived object storage must share one PVC")
	require.Len(t, storage.StorageClasses, 1, "identical derived object storage must share one StorageClass")

	pvc := storage.PVCs[0]
	assert.Equal(t, "true", pvc.Labels[v1alpha1.SharedObjectStoragePVCLabel])
	require.NotNil(t, pvc.Spec.StorageClassName)
	assert.Equal(t, storage.StorageClasses[0].Name, *pvc.Spec.StorageClassName,
		"derived PVC must reference the derived StorageClass")
}

func TestDescribeWorkflowStorageDerivedPVCCreatedRegardlessOfAutoCreate(t *testing.T) {
	provider := NewAliyunProvider()

	// Derived (empty pvcName) PVC: AutoCreatePVC=false must be ignored (operator-owned).
	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf", Namespace: "default"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{
				sparkTaskWithObjectStorage("a", &v1alpha1.ObjectStorage{
					Bucket:    "bucket",
					Path:      "path",
					MountSpec: &v1alpha1.ObjectStorageMount{AutoCreatePVC: false},
				}),
			},
		},
	}
	storage, err := provider.DescribeWorkflowStorage(lw, nil)
	require.NoError(t, err)
	assert.Len(t, storage.PVCs, 1, "derived PVC must always be created even when autoCreatePVC=false")
}

func TestDescribeWorkflowStorageExplicitPVCRespectsAutoCreate(t *testing.T) {
	provider := NewAliyunProvider()

	// Explicit pvcName + AutoCreatePVC=false: PVC must NOT be created (may pre-exist),
	// but the explicit StorageClass is still described.
	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf", Namespace: "default"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{
				sparkTaskWithObjectStorage("a", &v1alpha1.ObjectStorage{
					Bucket:       "bucket",
					Path:         "path",
					StorageClass: "explicit-sc",
					MountSpec:    &v1alpha1.ObjectStorageMount{PVCName: "explicit-pvc", AutoCreatePVC: false},
				}),
			},
		},
	}
	storage, err := provider.DescribeWorkflowStorage(lw, nil)
	require.NoError(t, err)
	assert.Empty(t, storage.PVCs, "explicit pvcName with autoCreatePVC=false must not be created")
	require.Len(t, storage.StorageClasses, 1)
	assert.Equal(t, "explicit-sc", storage.StorageClasses[0].Name)
}
