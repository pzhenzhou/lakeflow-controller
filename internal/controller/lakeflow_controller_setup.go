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
	"fmt"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/sparkmanager"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"github.com/pzhenzhou/lakeflow-controller/pkg/reconciler"
	"github.com/pzhenzhou/lakeflow-controller/pkg/rerun"
	"github.com/pzhenzhou/lakeflow-controller/pkg/validation"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// SetupWithManager sets up the LakeFlow controller with the Manager.
func (r *LakeFlowController) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.ensureDependencies(mgr); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter[reconcile.Request](
				workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](5*time.Millisecond, 10*time.Second),
			),
		}).
		For(&v1alpha1.LakeFlow{}, builder.WithPredicates(lakeFlowChangePredicate())).
		Owns(&argowfv1.WorkflowTemplate{}).
		Watches(&argowfv1.Workflow{}, lakeFlowHandlerFor[*argowfv1.Workflow](), builder.WithPredicates(argoWorkflowStatePredicate())).
		Watches(&argowfv1.CronWorkflow{}, lakeFlowHandlerFor[*argowfv1.CronWorkflow](), builder.WithPredicates(argoCronWorkflowStatePredicate())).
		Named(v1alpha1.LakeFlowControllerName).
		Complete(r)
}

func (r *LakeFlowController) ensureDependencies(mgr ctrl.Manager) error {
	if r.CloudProfile.Name == "" {
		profile, err := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
		if err != nil {
			return err
		}
		r.CloudProfile = profile
	}
	if r.CloudProvider == nil {
		r.CloudProvider = cloud.NewProvider(r.CloudProfile)
	}
	if r.WorkflowConverter == nil {
		r.WorkflowConverter = adapter.NewWorkflowConverter(r.CloudProfile)
	}

	if r.ResourceManager == nil {
		resourceManager, err := argoclient.NewArgoResourceClient(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("failed to create ArgoResourceClient: %w", err)
		}
		r.ResourceManager = resourceManager
	}

	if r.ValidationManager == nil {
		r.ValidationManager = validation.NewManager()
	}

	if r.EventRecorder == nil {
		r.EventRecorder = mgr.GetEventRecorderFor(v1alpha1.LakeFlowControllerName)
	}

	if r.RerunManager == nil {
		r.RerunManager = rerun.NewWorkflowRerunManager(mgr.GetClient(), mgr.GetConfig(), r.ResourceManager, r.WorkflowConverter, r.EnableRerunTaskLock)
	}

	if r.SparkApplicationManager == nil {
		sparkMgr, err := sparkmanager.NewSparkApplicationManager(
			mgr.GetConfig(),
			mgr.GetClient(),
			common.GetSharedLogger().WithName("spark-application-manager"),
		)
		if err != nil {
			return fmt.Errorf("failed to create SparkApplicationManager: %w", err)
		}
		r.SparkApplicationManager = sparkMgr
	}

	if r.reconciler == nil {
		config := r.Config
		if config == nil {
			config = mgr.GetConfig()
		}
		r.reconciler = reconciler.NewLakeFlowReconciler(
			r.ResourceManager,
			r.WorkflowConverter,
			r.CloudProvider,
			r.Client,
			config,
		)
	}

	return nil
}
