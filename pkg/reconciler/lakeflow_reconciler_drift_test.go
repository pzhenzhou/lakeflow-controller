package reconciler

import (
	"context"
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// mockDriftArgoResourceClient returns a pre-configured WorkflowTemplate from Get.
type mockDriftArgoResourceClient struct {
	workflowTemplate *argowfv1.WorkflowTemplate
	getNotFound      bool
}

func (m *mockDriftArgoResourceClient) Get(ctx context.Context, name, namespace string, obj client.Object) error {
	if m.getNotFound {
		return errors.NewNotFound(schema.GroupResource{Group: "argoproj.io", Resource: "workflowtemplates"}, name)
	}
	if wt, ok := obj.(*argowfv1.WorkflowTemplate); ok && m.workflowTemplate != nil {
		m.workflowTemplate.DeepCopyInto(wt)
	}
	return nil
}

func (m *mockDriftArgoResourceClient) Create(ctx context.Context, obj client.Object) error {
	return nil
}
func (m *mockDriftArgoResourceClient) List(ctx context.Context, namespace string, obj client.ObjectList, opts ...client.ListOption) error {
	return nil
}
func (m *mockDriftArgoResourceClient) Update(ctx context.Context, obj client.Object) error {
	return nil
}
func (m *mockDriftArgoResourceClient) Delete(ctx context.Context, name, namespace string, obj client.Object) error {
	return nil
}
func (m *mockDriftArgoResourceClient) CreateOrUpdate(ctx context.Context, obj client.Object) error {
	return nil
}
func (m *mockDriftArgoResourceClient) Patch(ctx context.Context, name, namespace string, obj client.Object, patchData []byte, patchType types.PatchType) error {
	return nil
}

func TestHasSparkPVCLabelDrift(t *testing.T) {
	tests := []struct {
		name             string
		desiredLabels    map[string]string
		templateLabels   map[string]string
		templateNotFound bool
		templateName     string
		wantDrift        bool
	}{
		{
			name:          "no template in status",
			desiredLabels: map[string]string{v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS},
			templateName:  "",
			wantDrift:     false,
		},
		{
			name: "labels match -- no drift",
			desiredLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			templateLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			templateName: "test-template-123",
			wantDrift:    false,
		},
		{
			name: "label added -- drift",
			desiredLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			templateLabels: map[string]string{},
			templateName:   "test-template-123",
			wantDrift:      true,
		},
		{
			name:          "label removed -- drift",
			desiredLabels: map[string]string{},
			templateLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			templateName: "test-template-123",
			wantDrift:    true,
		},
		{
			name: "size changed -- drift",
			desiredLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "50Gi",
			},
			templateLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			templateName: "test-template-123",
			wantDrift:    true,
		},
		{
			name: "unrelated labels differ -- no drift",
			desiredLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS,
				"app":                          "v2",
			},
			templateLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS,
				"app":                          "v1",
			},
			templateName: "test-template-123",
			wantDrift:    false,
		},
		{
			name:             "template not found -- drift",
			desiredLabels:    map[string]string{},
			templateNotFound: true,
			templateName:     "deleted-template",
			wantDrift:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockDriftArgoResourceClient{
				getNotFound: tt.templateNotFound,
			}
			if tt.templateLabels != nil {
				mock.workflowTemplate = &argowfv1.WorkflowTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Name:   tt.templateName,
						Labels: tt.templateLabels,
					},
				}
			}

			r := &reconcilerImpl{resourceManager: mock}

			desired := &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-workflow",
					Namespace: "default",
					Labels:    tt.desiredLabels,
				},
			}
			existing := &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-workflow",
					Namespace: "default",
				},
			}
			existing.Status.WorkflowTemplate = tt.templateName

			drift, err := r.hasSparkPVCLabelDrift(context.Background(), desired, existing)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantDrift, drift)
		})
	}
}
