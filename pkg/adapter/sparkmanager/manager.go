// Package sparkmanager performs runtime operations on SparkApplication resources
// (driver termination / CR deletion) during LakeFlow control actions. It is
// independent of the conversion/rendering code in pkg/adapter so the status and
// controller layers can depend on just the runtime manager.
package sparkmanager

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const driverExecTimeout = 30 * time.Second

// SparkApplicationManager performs runtime operations on SparkApplication resources and their driver pods,
// including pods/exec; if there is no driver pod name, or exec fails, it deletes the SparkApplication CR.
type SparkApplicationManager struct {
	client     client.Client
	logger     logr.Logger
	restConfig *rest.Config
	clientset  kubernetes.Interface
}

// NewSparkApplicationManager returns a manager that lists SparkApplications and terminates drivers via exec,
// falling back to deleting the SparkApplication CR when there is no driver pod name or exec fails.
func NewSparkApplicationManager(restCfg *rest.Config, c client.Client, log logr.Logger) (*SparkApplicationManager, error) {
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("adapter: kubernetes client: %w", err)
	}
	return &SparkApplicationManager{
		client:     c,
		logger:     log,
		restConfig: restCfg,
		clientset:  cs,
	}, nil
}

// killSparkDriver runs `pkill -9 -f spark` in the Spark driver pod (equivalent to kubectl exec … -- pkill …).
func (m *SparkApplicationManager) killSparkDriver(ctx context.Context, namespace, podName string) error {
	if m.clientset == nil || m.restConfig == nil {
		return fmt.Errorf("pod exec not configured")
	}

	req := m.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: []string{"pkill", "-9", "-f", "spark"},
			Stdin:   false,
			Stdout:  true,
			Stderr:  true,
			TTY:     false,
		}, clientscheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(m.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("create exec executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	}); err != nil {
		return fmt.Errorf("exec stream: %w (stderr: %s)", err, stderr.String())
	}
	return nil
}

// TerminateRunningForLakeFlow lists the LakeFlow's SparkApplications with a single
// label query and, for each app belonging to a Spark task whose state is not
// Completed or Failed, runs pkill in the driver pod when a driver pod exists;
// otherwise, or if exec fails, it deletes the SparkApplication object.
//
// A single List by WorkflowNameLabel (then in-memory grouping by task) replaces the
// previous one-List-per-task loop, so an N-Spark-task workflow costs one API call
// instead of N.
func (m *SparkApplicationManager) TerminateRunningForLakeFlow(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	// Collect the Spark task names once so the single List result can be filtered
	// in memory (preserves the previous behavior of only acting on current Spark
	// tasks, while dropping the per-task API calls).
	sparkTasks := make(map[string]struct{})
	for i := range lw.Spec.Tasks {
		if lw.Spec.Tasks[i].Executor == v1alpha1.TaskExecutorSpark {
			sparkTasks[lw.Spec.Tasks[i].Name] = struct{}{}
		}
	}
	if len(sparkTasks) == 0 {
		return nil
	}

	sparkAppList := &sparkv1beta2.SparkApplicationList{}
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{v1alpha1.WorkflowNameLabel: lw.Name},
	}
	if err := m.client.List(ctx, sparkAppList, listOpts...); err != nil {
		m.logger.Error(err, "Failed to list SparkApplications for LakeFlow", "lakeWorkflow", lw.Name)
		return nil
	}

	for i := range sparkAppList.Items {
		sparkApp := &sparkAppList.Items[i]
		taskName := sparkApp.GetLabels()[v1alpha1.WorkflowTaskNameLabel]
		if _, ok := sparkTasks[taskName]; !ok {
			// Not a current Spark task of this LakeFlow; skip (defensive).
			continue
		}
		m.terminateSparkApplication(ctx, lw, sparkApp, taskName)
	}

	return nil
}

// terminateSparkApplication terminates a single non-terminal SparkApplication:
// pkill in the driver pod when present, otherwise (or on exec failure) delete the CR.
func (m *SparkApplicationManager) terminateSparkApplication(ctx context.Context, lw *v1alpha1.LakeFlow, sparkApp *sparkv1beta2.SparkApplication, taskName string) {
	state := sparkApp.Status.AppState.State
	if state == sparkv1beta2.ApplicationStateCompleted || state == sparkv1beta2.ApplicationStateFailed {
		m.logger.V(1).Info("Skipping terminal SparkApplication",
			"sparkApplication", sparkApp.Name,
			"state", state,
			"task", taskName)
		return
	}

	driverPodName := sparkApp.Status.DriverInfo.PodName
	if driverPodName == "" {
		m.logger.Info("SparkApplication has no driver pod name, deleting SparkApplication",
			"sparkApplication", sparkApp.Name,
			"task", taskName,
			"lakeWorkflow", lw.Name)
		if delErr := m.deleteSparkApplication(ctx, sparkApp); delErr != nil && !errors.IsNotFound(delErr) {
			m.logger.Error(delErr, "Failed to delete SparkApplication (no driver pod)",
				"sparkApplication", sparkApp.Name)
		}
		return
	}

	execCtx, cancel := context.WithTimeout(ctx, driverExecTimeout)
	err := m.killSparkDriver(execCtx, lw.Namespace, driverPodName)
	cancel()

	if err == nil {
		m.logger.Info("Terminated Spark driver via exec",
			"driverPod", driverPodName,
			"sparkApplication", sparkApp.Name,
			"task", taskName,
			"lakeWorkflow", lw.Name)
		return
	}

	m.logger.Error(err, "Spark driver exec failed, deleting SparkApplication",
		"driverPod", driverPodName,
		"sparkApplication", sparkApp.Name,
		"task", taskName,
		"lakeWorkflow", lw.Name)

	if delErr := m.deleteSparkApplication(ctx, sparkApp); delErr != nil && !errors.IsNotFound(delErr) {
		m.logger.Error(delErr, "Failed to delete SparkApplication after exec failure",
			"sparkApplication", sparkApp.Name)
	}
}

// deleteSparkApplication removes the SparkApplication CR with grace-period 0 and background propagation.
func (m *SparkApplicationManager) deleteSparkApplication(ctx context.Context, sparkApp *sparkv1beta2.SparkApplication) error {
	obj := &sparkv1beta2.SparkApplication{}
	obj.Name = sparkApp.Name
	obj.Namespace = sparkApp.Namespace
	// client.Delete returns once the API server accepts the delete (like kubectl delete --wait=false);
	// Background propagation avoids blocking the request on foreground garbage collection when applicable.
	return m.client.Delete(ctx, obj,
		client.GracePeriodSeconds(0),
		client.PropagationPolicy(metav1.DeletePropagationBackground),
	)
}
