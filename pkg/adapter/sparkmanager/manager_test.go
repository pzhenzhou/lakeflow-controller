package sparkmanager

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// restConfigForUnreachableAPI returns a config whose API server is not listening,
// so pod exec fails deterministically without a test-only hook.
func restConfigForUnreachableAPI(t *testing.T) *rest.Config {
	t.Helper()
	return &rest.Config{
		Host: "https://127.0.0.1:65432",
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}
}

func TestTerminateRunningForLakeFlow_SkipsCompletedSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, sparkv1beta2.AddToScheme(scheme))

	sparkApp := sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-1",
			Namespace: "ns1",
			Labels: map[string]string{
				v1alpha1.WorkflowNameLabel:     "lw1",
				v1alpha1.WorkflowTaskNameLabel: "t1",
			},
		},
		Status: sparkv1beta2.SparkApplicationStatus{
			AppState: sparkv1beta2.ApplicationState{
				State: sparkv1beta2.ApplicationStateCompleted,
			},
			DriverInfo: sparkv1beta2.DriverInfo{PodName: "driver-1"},
		},
	}
	driverPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "driver-1", Namespace: "ns1"},
		Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&sparkApp, &driverPod).Build()
	m, err := NewSparkApplicationManager(restConfigForUnreachableAPI(t), c, logr.Discard())
	require.NoError(t, err)

	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "lw1", Namespace: "ns1"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{{Name: "t1", Executor: v1alpha1.TaskExecutorSpark}},
		},
	}

	require.NoError(t, m.TerminateRunningForLakeFlow(context.Background(), lw))

	var apps sparkv1beta2.SparkApplicationList
	require.NoError(t, c.List(context.Background(), &apps, client.InNamespace("ns1")))
	assert.Len(t, apps.Items, 1, "completed SparkApplication must not be touched")
}

func TestTerminateRunningForLakeFlow_SkipsFailedSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, sparkv1beta2.AddToScheme(scheme))

	sparkApp := sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-1",
			Namespace: "ns1",
			Labels: map[string]string{
				v1alpha1.WorkflowNameLabel:     "lw1",
				v1alpha1.WorkflowTaskNameLabel: "t1",
			},
		},
		Status: sparkv1beta2.SparkApplicationStatus{
			AppState: sparkv1beta2.ApplicationState{
				State: sparkv1beta2.ApplicationStateFailed,
			},
			DriverInfo: sparkv1beta2.DriverInfo{PodName: "driver-1"},
		},
	}
	driverPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "driver-1", Namespace: "ns1"},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&sparkApp, &driverPod).Build()
	m, err := NewSparkApplicationManager(restConfigForUnreachableAPI(t), c, logr.Discard())
	require.NoError(t, err)

	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "lw1", Namespace: "ns1"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{{Name: "t1", Executor: v1alpha1.TaskExecutorSpark}},
		},
	}

	require.NoError(t, m.TerminateRunningForLakeFlow(context.Background(), lw))

	var apps sparkv1beta2.SparkApplicationList
	require.NoError(t, c.List(context.Background(), &apps, client.InNamespace("ns1")))
	assert.Len(t, apps.Items, 1, "failed SparkApplication must not be touched")
}

func TestTerminateRunningForLakeFlow_EmptyDriverPodName_DeletesSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, sparkv1beta2.AddToScheme(scheme))

	sparkApp := sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-1",
			Namespace: "ns1",
			Labels: map[string]string{
				v1alpha1.WorkflowNameLabel:     "lw1",
				v1alpha1.WorkflowTaskNameLabel: "t1",
			},
		},
		Status: sparkv1beta2.SparkApplicationStatus{
			AppState: sparkv1beta2.ApplicationState{
				State: sparkv1beta2.ApplicationStateSubmitted,
			},
			DriverInfo: sparkv1beta2.DriverInfo{PodName: ""},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&sparkApp).Build()
	m, err := NewSparkApplicationManager(restConfigForUnreachableAPI(t), c, logr.Discard())
	require.NoError(t, err)

	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "lw1", Namespace: "ns1"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{{Name: "t1", Executor: v1alpha1.TaskExecutorSpark}},
		},
	}

	require.NoError(t, m.TerminateRunningForLakeFlow(context.Background(), lw))

	var apps sparkv1beta2.SparkApplicationList
	require.NoError(t, c.List(context.Background(), &apps, client.InNamespace("ns1")))
	assert.Empty(t, apps.Items, "SparkApplication should be deleted when driver pod name is empty")
}

func TestTerminateRunningForLakeFlow_ExecFailure_DeletesSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, sparkv1beta2.AddToScheme(scheme))

	sparkApp := sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-1",
			Namespace: "ns1",
			Labels: map[string]string{
				v1alpha1.WorkflowNameLabel:     "lw1",
				v1alpha1.WorkflowTaskNameLabel: "t1",
			},
		},
		Status: sparkv1beta2.SparkApplicationStatus{
			AppState: sparkv1beta2.ApplicationState{
				State: sparkv1beta2.ApplicationStateRunning,
			},
			DriverInfo: sparkv1beta2.DriverInfo{PodName: "driver-1"},
		},
	}
	driverPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "driver-1", Namespace: "ns1"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&sparkApp, &driverPod).Build()
	m, err := NewSparkApplicationManager(restConfigForUnreachableAPI(t), c, logr.Discard())
	require.NoError(t, err)

	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "lw1", Namespace: "ns1"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{{Name: "t1", Executor: v1alpha1.TaskExecutorSpark}},
		},
	}

	require.NoError(t, m.TerminateRunningForLakeFlow(context.Background(), lw))

	var apps sparkv1beta2.SparkApplicationList
	require.NoError(t, c.List(context.Background(), &apps, client.InNamespace("ns1")))
	assert.Empty(t, apps.Items, "SparkApplication should be deleted when exec fails")

	var pods corev1.PodList
	require.NoError(t, c.List(context.Background(), &pods, client.InNamespace("ns1")))
	assert.Len(t, pods.Items, 1, "fake client does not cascade-delete pods when CR is removed")
}
