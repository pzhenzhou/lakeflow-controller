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
	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

func lakeFlowChangePredicate() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		annotationSelectPredicate(),
		sparkPVCLabelPredicate(),
	)
}

// typedUpdate adapts a strongly-typed update comparison into the untyped
// UpdateFunc shape expected by predicate.TypedFuncs[client.Object]. It centralizes
// the "assert both objects to T, bail if either fails" boilerplate so each
// predicate only expresses the change it actually cares about.
func typedUpdate[T client.Object](changed func(oldObj, newObj T) bool) func(event.TypedUpdateEvent[client.Object]) bool {
	return func(e event.TypedUpdateEvent[client.Object]) bool {
		oldObj, ok1 := e.ObjectOld.(T)
		newObj, ok2 := e.ObjectNew.(T)
		if !ok1 || !ok2 {
			return false
		}
		return changed(oldObj, newObj)
	}
}

func argoWorkflowStatePredicate() predicate.TypedFuncs[client.Object] {
	return predicate.TypedFuncs[client.Object]{
		CreateFunc: func(e event.CreateEvent) bool {
			wf, ok := e.Object.(*argowfv1.Workflow)
			if !ok {
				return false
			}
			if isManualInterventionWorkflow(wf) {
				logger.Info("Manual intervention workflow created, triggering LakeFlow reconciliation",
					"workflow", wf.Name,
					"namespace", wf.Namespace,
					"isLakeResubmit", wf.Labels[v1alpha1.IsResubmitWorkflowLabel] == "true",
					"isLakeRetry", wf.Labels[v1alpha1.IsRetryWorkflowLabel] == "true")
				return true
			}
			return false
		},
		UpdateFunc: typedUpdate(func(oldWF, newWF *argowfv1.Workflow) bool {
			phaseChanged := oldWF.Status.Phase != newWF.Status.Phase
			if phaseChanged {
				logger.Info("Argo Workflow phase changed, triggering LakeFlow reconciliation",
					"workflow", newWF.Name,
					"namespace", newWF.Namespace,
					"oldPhase", oldWF.Status.Phase,
					"newPhase", newWF.Status.Phase)
			}
			return phaseChanged
		}),
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
	}
}

func isManualInterventionWorkflow(wf *argowfv1.Workflow) bool {
	labels := wf.GetLabels()
	return labels[v1alpha1.IsResubmitWorkflowLabel] == "true" ||
		labels[v1alpha1.IsRetryWorkflowLabel] == "true"
}

func argoCronWorkflowStatePredicate() predicate.TypedFuncs[client.Object] {
	return predicate.TypedFuncs[client.Object]{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: typedUpdate(func(oldCWF, newCWF *argowfv1.CronWorkflow) bool {
			phaseChanged := oldCWF.Status.Phase != newCWF.Status.Phase
			activeJobsChanged := len(oldCWF.Status.Active) != len(newCWF.Status.Active)
			countersChanged := oldCWF.Status.Succeeded != newCWF.Status.Succeeded ||
				oldCWF.Status.Failed != newCWF.Status.Failed

			shouldTrigger := phaseChanged || activeJobsChanged || countersChanged
			if shouldTrigger {
				logger.Info("Argo CronWorkflow status changed, triggering LakeFlow reconciliation",
					"cronworkflow", newCWF.Name,
					"namespace", newCWF.Namespace,
					"oldPhase", oldCWF.Status.Phase,
					"newPhase", newCWF.Status.Phase,
					"oldActive", len(oldCWF.Status.Active),
					"newActive", len(newCWF.Status.Active),
					"oldSucceeded", oldCWF.Status.Succeeded,
					"newSucceeded", newCWF.Status.Succeeded,
					"oldFailed", oldCWF.Status.Failed,
					"newFailed", newCWF.Status.Failed)
			}
			return shouldTrigger
		}),
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
	}
}

func annotationSelectPredicate() predicate.TypedFuncs[client.Object] {
	ignored := map[string]struct{}{
		"kubectl.kubernetes.io/last-applied-configuration": {},
		"deployment.kubernetes.io/revision":                {},
		"control-plane.alpha.kubernetes.io/leader":         {},
	}
	return predicate.TypedFuncs[client.Object]{
		UpdateFunc: func(e event.TypedUpdateEvent[client.Object]) bool {
			return !annotationsEqualIgnoring(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations(), ignored)
		},
	}
}

func annotationsEqualIgnoring(oldAnnotations, newAnnotations map[string]string, ignored map[string]struct{}) bool {
	oldFiltered := filterAnnotations(oldAnnotations, ignored)
	newFiltered := filterAnnotations(newAnnotations, ignored)
	if len(oldFiltered) != len(newFiltered) {
		return false
	}
	for k, v := range oldFiltered {
		if newFiltered[k] != v {
			return false
		}
	}
	return true
}

func filterAnnotations(annotations map[string]string, ignored map[string]struct{}) map[string]string {
	filtered := make(map[string]string)
	for k, v := range annotations {
		if _, skip := ignored[k]; !skip {
			filtered[k] = v
		}
	}
	return filtered
}

func sparkPVCLabelPredicate() predicate.TypedFuncs[client.Object] {
	return predicate.TypedFuncs[client.Object]{
		UpdateFunc: func(e event.TypedUpdateEvent[client.Object]) bool {
			return adapter.SparkPVCLabelsChanged(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels())
		},
	}
}
