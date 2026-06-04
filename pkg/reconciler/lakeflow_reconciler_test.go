package reconciler

import (
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
)

// TestReconcilerResourceComparison tests the resource comparison logic
func TestReconcilerResourceComparison(t *testing.T) {
	comparator := newResourceComparator()

	t.Run("WorkflowTemplate comparison - identical", func(t *testing.T) {
		existing := &argowfv1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
				Labels:    map[string]string{"app": "test"},
			},
			Spec: argowfv1.WorkflowSpec{
				Entrypoint: "main",
			},
		}

		// Same content
		desired := existing.DeepCopy()
		changed := comparator.hasWorkflowTemplateChanged(existing, desired)
		assert.False(t, changed, "Identical templates should not be considered changed")
	})

	t.Run("WorkflowTemplate comparison - different entrypoint", func(t *testing.T) {
		existing := &argowfv1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
				Labels:    map[string]string{"app": "test"},
			},
			Spec: argowfv1.WorkflowSpec{
				Entrypoint: "main",
			},
		}

		desired := existing.DeepCopy()
		desired.Spec.Entrypoint = "different"
		changed := comparator.hasWorkflowTemplateChanged(existing, desired)
		assert.True(t, changed, "Different entrypoint should be considered changed")
	})

	t.Run("CronWorkflow comparison - identical", func(t *testing.T) {
		existing := &argowfv1.CronWorkflow{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cron",
				Namespace: "default",
			},
			Spec: argowfv1.CronWorkflowSpec{
				Schedules: []string{"0 0 * * *"},
			},
		}

		desired := existing.DeepCopy()
		changed := comparator.hasCronWorkflowChanged(existing, desired)
		assert.False(t, changed, "Identical CronWorkflows should not be considered changed")
	})

	t.Run("CronWorkflow comparison - different schedule", func(t *testing.T) {
		existing := &argowfv1.CronWorkflow{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cron",
				Namespace: "default",
			},
			Spec: argowfv1.CronWorkflowSpec{
				Schedules: []string{"0 0 * * *"},
			},
		}

		desired := existing.DeepCopy()
		desired.Spec.Schedules = []string{"0 12 * * *"}
		changed := comparator.hasCronWorkflowChanged(existing, desired)
		assert.True(t, changed, "Different schedules should be considered changed")
	})
}

// TestResourceHelpers tests the resource helper functions
func TestResourceHelpers(t *testing.T) {
	tests := []struct {
		name         string
		resource     client.Object
		expectedType string
		expectedName string
	}{
		{
			name: "WorkflowTemplate",
			resource: &argowfv1.WorkflowTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "test-template"},
			},
			expectedType: "WorkflowTemplate",
			expectedName: "test-template",
		},
		{
			name: "Workflow with name",
			resource: &argowfv1.Workflow{
				ObjectMeta: metav1.ObjectMeta{Name: "test-workflow"},
			},
			expectedType: "Workflow",
			expectedName: "test-workflow",
		},
		{
			name: "Workflow with generateName",
			resource: &argowfv1.Workflow{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "test-workflow-"},
			},
			expectedType: "Workflow",
			expectedName: "<unnamed>",
		},
		{
			name: "CronWorkflow",
			resource: &argowfv1.CronWorkflow{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cronworkflow"},
			},
			expectedType: "CronWorkflow",
			expectedName: "test-cronworkflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resourceType := common.GetArgoResourceType(tt.resource)
			resourceName := common.GetResourceName(tt.resource)

			assert.Equal(t, tt.expectedType, resourceType)
			assert.Equal(t, tt.expectedName, resourceName)
		})
	}
}

// TestLabelsEqual tests the label comparison logic
func TestLabelsEqual(t *testing.T) {
	comparator := newResourceComparator()

	t.Run("identical labels", func(t *testing.T) {
		labels1 := map[string]string{"app": "test", "version": "v1"}
		labels2 := map[string]string{"app": "test", "version": "v1"}
		assert.True(t, comparator.labelsEqual(labels1, labels2))
	})

	t.Run("different labels", func(t *testing.T) {
		labels1 := map[string]string{"app": "test", "version": "v1"}
		labels2 := map[string]string{"app": "test", "version": "v2"}
		assert.False(t, comparator.labelsEqual(labels1, labels2))
	})

	t.Run("different label count", func(t *testing.T) {
		labels1 := map[string]string{"app": "test"}
		labels2 := map[string]string{"app": "test", "version": "v1"}
		assert.False(t, comparator.labelsEqual(labels1, labels2))
	})

	t.Run("nil labels", func(t *testing.T) {
		assert.True(t, comparator.labelsEqual(nil, nil))
		assert.True(t, comparator.labelsEqual(nil, map[string]string{}))
		assert.True(t, comparator.labelsEqual(map[string]string{}, nil))
	})
}

// TestOwnerReferencesEqual tests owner reference comparison
func TestOwnerReferencesEqual(t *testing.T) {
	comparator := newResourceComparator()

	t.Run("identical owner references", func(t *testing.T) {
		refs1 := []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "LakeFlow", Name: "test", UID: "uid-123"},
		}
		refs2 := []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "LakeFlow", Name: "test", UID: "uid-123"},
		}
		assert.True(t, comparator.ownerReferencesEqual(refs1, refs2))
	})

	t.Run("different UIDs", func(t *testing.T) {
		refs1 := []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "LakeFlow", Name: "test", UID: "uid-123"},
		}
		refs2 := []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "LakeFlow", Name: "test", UID: "uid-456"},
		}
		assert.False(t, comparator.ownerReferencesEqual(refs1, refs2))
	})

	t.Run("different count", func(t *testing.T) {
		refs1 := []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "LakeFlow", Name: "test1", UID: "uid-123"},
		}
		refs2 := []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "LakeFlow", Name: "test1", UID: "uid-123"},
			{APIVersion: "v1", Kind: "LakeFlow", Name: "test2", UID: "uid-456"},
		}
		assert.False(t, comparator.ownerReferencesEqual(refs1, refs2))
	})

	t.Run("empty owner references", func(t *testing.T) {
		assert.True(t, comparator.ownerReferencesEqual(nil, nil))
		assert.True(t, comparator.ownerReferencesEqual([]metav1.OwnerReference{}, []metav1.OwnerReference{}))
	})
}
