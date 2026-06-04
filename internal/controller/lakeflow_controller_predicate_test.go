package controller

import (
	"context"
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestReconcileLakeFlowNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	reconciler := &LakeFlowController{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestSparkPVCLabelPredicate(t *testing.T) {
	predicate := sparkPVCLabelPredicate()

	tests := []struct {
		name        string
		oldLabels   map[string]string
		newLabels   map[string]string
		shouldMatch bool
	}{
		{
			name:        "no relevant label changes",
			oldLabels:   map[string]string{"other": "value"},
			newLabels:   map[string]string{"other": "updated"},
			shouldMatch: false,
		},
		{
			name:        "spark pvc label added",
			oldLabels:   map[string]string{},
			newLabels:   map[string]string{v1alpha1.SparkLocalDirPVCLabel: v1alpha1.SparkLocalDirPVCTypeEBS},
			shouldMatch: true,
		},
		{
			name: "spark pvc size changed",
			oldLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "20Gi",
			},
			newLabels: map[string]string{
				v1alpha1.SparkLocalDirPVCLabel:     v1alpha1.SparkLocalDirPVCTypeEBS,
				v1alpha1.SparkLocalDirPVCSizeLabel: "40Gi",
			},
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldObj := &v1alpha1.LakeFlow{ObjectMeta: metav1.ObjectMeta{Labels: tt.oldLabels}}
			newObj := &v1alpha1.LakeFlow{ObjectMeta: metav1.ObjectMeta{Labels: tt.newLabels}}

			matched := predicate.UpdateFunc(event.TypedUpdateEvent[client.Object]{
				ObjectOld: oldObj,
				ObjectNew: newObj,
			})
			assert.Equal(t, tt.shouldMatch, matched)
		})
	}
}

func TestAnnotationSelectPredicate(t *testing.T) {
	predicate := annotationSelectPredicate()

	tests := []struct {
		name           string
		oldAnnotations map[string]string
		newAnnotations map[string]string
		shouldMatch    bool
	}{
		{
			name: "ignored annotation change",
			oldAnnotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "old",
			},
			newAnnotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "new",
			},
			shouldMatch: false,
		},
		{
			name:           "meaningful annotation added",
			oldAnnotations: map[string]string{},
			newAnnotations: map[string]string{v1alpha1.ResubmitRequestAnnotation: "true"},
			shouldMatch:    true,
		},
		{
			name:           "meaningful annotation changed",
			oldAnnotations: map[string]string{v1alpha1.RetryRequestAnnotation: "first"},
			newAnnotations: map[string]string{v1alpha1.RetryRequestAnnotation: "second"},
			shouldMatch:    true,
		},
		{
			name:           "no meaningful change",
			oldAnnotations: map[string]string{"custom": "value"},
			newAnnotations: map[string]string{"custom": "value"},
			shouldMatch:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldObj := &v1alpha1.LakeFlow{ObjectMeta: metav1.ObjectMeta{Annotations: tt.oldAnnotations}}
			newObj := &v1alpha1.LakeFlow{ObjectMeta: metav1.ObjectMeta{Annotations: tt.newAnnotations}}

			matched := predicate.UpdateFunc(event.TypedUpdateEvent[client.Object]{
				ObjectOld: oldObj,
				ObjectNew: newObj,
			})
			assert.Equal(t, tt.shouldMatch, matched)
		})
	}
}

func TestArgoWorkflowStatePredicate(t *testing.T) {
	predicate := argoWorkflowStatePredicate()

	t.Run("manual intervention create triggers", func(t *testing.T) {
		workflow := &argowfv1.Workflow{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "wf-resubmit",
				Namespace: "default",
				Labels: map[string]string{
					v1alpha1.IsResubmitWorkflowLabel: "true",
				},
			},
		}

		assert.True(t, predicate.CreateFunc(event.CreateEvent{Object: workflow}))
	})

	t.Run("normal create does not trigger", func(t *testing.T) {
		workflow := &argowfv1.Workflow{
			ObjectMeta: metav1.ObjectMeta{Name: "wf", Namespace: "default"},
		}

		assert.False(t, predicate.CreateFunc(event.CreateEvent{Object: workflow}))
	})

	t.Run("phase change triggers", func(t *testing.T) {
		oldWorkflow := &argowfv1.Workflow{Status: argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowRunning}}
		newWorkflow := &argowfv1.Workflow{Status: argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowSucceeded}}

		assert.True(t, predicate.UpdateFunc(event.UpdateEvent{ObjectOld: oldWorkflow, ObjectNew: newWorkflow}))
	})

	t.Run("same phase does not trigger", func(t *testing.T) {
		oldWorkflow := &argowfv1.Workflow{Status: argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowRunning}}
		newWorkflow := &argowfv1.Workflow{Status: argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowRunning}}

		assert.False(t, predicate.UpdateFunc(event.UpdateEvent{ObjectOld: oldWorkflow, ObjectNew: newWorkflow}))
	})
}

func TestArgoCronWorkflowStatePredicate(t *testing.T) {
	predicate := argoCronWorkflowStatePredicate()

	tests := []struct {
		name        string
		oldStatus   argowfv1.CronWorkflowStatus
		newStatus   argowfv1.CronWorkflowStatus
		shouldMatch bool
	}{
		{
			name:        "phase changed",
			oldStatus:   argowfv1.CronWorkflowStatus{Phase: argowfv1.CronWorkflowPhase("Running")},
			newStatus:   argowfv1.CronWorkflowStatus{Phase: argowfv1.CronWorkflowPhase("Stopped")},
			shouldMatch: true,
		},
		{
			name: "active jobs changed",
			oldStatus: argowfv1.CronWorkflowStatus{
				Active: []corev1.ObjectReference{{Name: "wf-1"}},
			},
			newStatus: argowfv1.CronWorkflowStatus{
				Active: []corev1.ObjectReference{{Name: "wf-1"}, {Name: "wf-2"}},
			},
			shouldMatch: true,
		},
		{
			name:        "counters changed",
			oldStatus:   argowfv1.CronWorkflowStatus{Succeeded: 1},
			newStatus:   argowfv1.CronWorkflowStatus{Succeeded: 2},
			shouldMatch: true,
		},
		{
			name:        "no relevant status change",
			oldStatus:   argowfv1.CronWorkflowStatus{Succeeded: 1, Failed: 1},
			newStatus:   argowfv1.CronWorkflowStatus{Succeeded: 1, Failed: 1},
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldObj := &argowfv1.CronWorkflow{Status: tt.oldStatus}
			newObj := &argowfv1.CronWorkflow{Status: tt.newStatus}

			matched := predicate.UpdateFunc(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj})
			assert.Equal(t, tt.shouldMatch, matched)
		})
	}
}

func TestLakeFlowRequestsForObject(t *testing.T) {
	t.Run("owner reference wins", func(t *testing.T) {
		workflow := &argowfv1.Workflow{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Labels: map[string]string{
					v1alpha1.WorkflowNameLabel: "from-label",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: v1alpha1.GroupVersion.String(),
						Kind:       "LakeFlow",
						Name:       "from-owner",
					},
				},
			},
		}

		requests := lakeFlowRequestsForObject(workflow)
		require.Len(t, requests, 1)
		assert.Equal(t, types.NamespacedName{Name: "from-owner", Namespace: "default"}, requests[0].NamespacedName)
	})

	t.Run("label fallback", func(t *testing.T) {
		workflow := &argowfv1.Workflow{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Labels: map[string]string{
					v1alpha1.WorkflowNameLabel: "from-label",
				},
			},
		}

		requests := lakeFlowRequestsForObject(workflow)
		require.Len(t, requests, 1)
		assert.Equal(t, types.NamespacedName{Name: "from-label", Namespace: "default"}, requests[0].NamespacedName)
	})

	t.Run("no owner or label", func(t *testing.T) {
		requests := lakeFlowRequestsForObject(&argowfv1.Workflow{})
		assert.Nil(t, requests)
	})
}
