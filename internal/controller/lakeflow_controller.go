/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"time"

	"k8s.io/client-go/rest"

	"github.com/lithammer/shortuuid/v4"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/sparkmanager"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"github.com/pzhenzhou/lakeflow-controller/pkg/metrics"
	"github.com/pzhenzhou/lakeflow-controller/pkg/reconciler"
	"github.com/pzhenzhou/lakeflow-controller/pkg/rerun"
	"github.com/pzhenzhou/lakeflow-controller/pkg/status"
	"github.com/pzhenzhou/lakeflow-controller/pkg/validation"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	logger = common.GetSharedLogger().WithName("workflow-controller")
)

const (
	// DefaultRequeueInterval is the steady-state requeue delay used when the
	// loop completes without error but wants to re-check later (e.g. after a
	// validation failure). Error paths intentionally do not set a requeue
	// interval and instead rely on the controller's rate limiter for backoff.
	DefaultRequeueInterval = 30 * time.Second
)

// LakeFlowController reconciles a LakeFlow object
// It handles the Kubernetes controller lifecycle, error handling, status management, and requeuing
type LakeFlowController struct {
	client.Client
	Scheme     *runtime.Scheme
	reconciler reconciler.LakeFlowResourceProcessor
	Config     *rest.Config

	ResourceManager         argoclient.ArgoResourceClient
	CloudProfile            cloudprofile.CloudProfile
	CloudProvider           cloud.Provider
	WorkflowConverter       adapter.WorkflowConverter
	ValidationManager       validation.Manager
	RerunManager            rerun.WorkflowRerunManager // handles both retry and resubmit
	SparkApplicationManager *sparkmanager.SparkApplicationManager
	EventRecorder           record.EventRecorder

	// EnableRerunTaskLock gates the per-task rerun lock (opt-in, default off).
	EnableRerunTaskLock bool
}

// +kubebuilder:rbac:groups=lakeflow.io,resources=lakeflows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=lakeflow.io,resources=lakeflows/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=lakeflow.io,resources=lakeflows/finalizers,verbs=update
// +kubebuilder:rbac:groups=argoproj.io,resources=workflows;cronworkflows;workflowtemplates;workflowartifactgctasks;workflowtasksets;workflowtaskresults,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=argoproj.io,resources=workflows/status;cronworkflows/status;workflowtemplates/status;workflowartifactgctasks/status;workflowtasksets/status;workflowtaskresults/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=argoproj.io,resources=workflows/finalizers;cronworkflows/finalizers;workflowtemplates/finalizers;workflowartifactgctasks/finalizers;workflowtasksets/finalizers;workflowtaskresults/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;update;patch
// +kubebuilder:rbac:groups="",resources=resourcequotas,verbs=get;list;watch
// +kubebuilder:rbac:groups=extensions,resources=ingresses,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications/finalizers,verbs=update
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=scheduledsparkapplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=scheduledsparkapplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=scheduledsparkapplications/finalizers,verbs=update
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;create;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;create;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets/scale,verbs=get;watch;update
// +kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets/finalizers,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups=batch,resources=jobs/finalizers,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *LakeFlowController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logger.WithValues("Namespace", req.NamespacedName, "ReconcileId", shortuuid.New())
	logger.Info("Starting reconciliation", "lakeflow", req.NamespacedName)

	// Fetch the LakeFlow instance
	var lakeWorkflow v1alpha1.LakeFlow
	if err := r.Get(ctx, req.NamespacedName, &lakeWorkflow); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("LakeFlow resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get LakeFlow")
		// Return the bare error so controller-runtime's rate limiter drives the
		// requeue; RequeueAfter is ignored when a non-nil error is returned.
		return ctrl.Result{}, err
	}

	if !lakeWorkflow.DeletionTimestamp.IsZero() {
		logger.Info("LakeFlow has been deleted", "Namespace", lakeWorkflow.Namespace, "Name", lakeWorkflow.Name)
		metrics.GetMetricsManager().ResetMetrics(&lakeWorkflow)
		return ctrl.Result{}, nil
	}

	statusManager := status.NewStatusManager(r.Client, r.ResourceManager, &lakeWorkflow, logger, r.SparkApplicationManager)
	finish := func(result ctrl.Result, err error) (ctrl.Result, error) {
		if persistErr := statusManager.Persist(ctx, &lakeWorkflow); persistErr != nil {
			logger.Error(persistErr, "Failed to persist status changes at the end of reconciliation")
			if err == nil {
				return ctrl.Result{}, persistErr
			}
		}
		return result, err
	}

	// Observe execution status from underlying Argo resources.
	if err := statusManager.ObserveExecution(ctx, &lakeWorkflow); err != nil {
		logger.Error(err, "Failed to observe execution status")
		return finish(ctrl.Result{}, err)
	}

	if validationErr := r.ValidationManager.Validate(ctx, &lakeWorkflow); validationErr != nil {
		logger.Error(validationErr, "Workflow validation failed")
		statusManager.UpdateValidation(&lakeWorkflow, validationErr)
		return finish(ctrl.Result{RequeueAfter: DefaultRequeueInterval}, nil)
	}
	statusManager.UpdateValidation(&lakeWorkflow, nil) // Mark validation as successful in the status.

	result, recErr := r.reconciler.Process(ctx, &lakeWorkflow)
	if recErr != nil {
		logger.Error(recErr, "Resource reconciliation failed")
		result = reconciler.NewErrorResult(recErr)
	}

	// Apply the reconciliation outcome to the status.
	if applyErr := statusManager.ApplyReconcileOutcome(ctx, &lakeWorkflow, result); applyErr != nil {
		logger.Error(applyErr, "Failed to apply reconcile outcome to status")
		return finish(ctrl.Result{}, applyErr)
	}
	if recErr != nil {
		return finish(ctrl.Result{}, recErr)
	}

	if rerunResult, handled, err := r.handleRerun(ctx, &lakeWorkflow, statusManager, logger); handled {
		return finish(rerunResult, err)
	}

	if ctrlErr := statusManager.ApplyControlState(ctx, &lakeWorkflow); ctrlErr != nil {
		logger.Error(ctrlErr, "Failed to apply control state")
		return finish(ctrl.Result{}, ctrlErr)
	}

	logger.Info("Reconciliation completed successfully", "lakeflow", lakeWorkflow.Name)
	return finish(ctrl.Result{}, nil)
}
