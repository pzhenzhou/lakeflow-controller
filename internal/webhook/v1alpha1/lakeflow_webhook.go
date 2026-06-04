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

package v1alpha1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/validation"
)

// lakeflowlog is for logging in this package.
var lakeflowlog = logf.Log.WithName("lakeflow-webhook")

// lakeFlowGroupKind identifies the LakeFlow kind for structured admission
// errors (apierrors.NewInvalid surfaces per-field causes to kubectl).
var lakeFlowGroupKind = schema.GroupKind{Group: v1alpha1.WorkflowGroupName, Kind: "LakeFlow"}

// SetupLakeFlowWebhookWithManager registers the LakeFlow validating webhook with
// the manager's webhook server.
func SetupLakeFlowWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&v1alpha1.LakeFlow{}).
		WithValidator(&LakeFlowCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-lakeflow-io-v1alpha1-lakeflow,mutating=false,failurePolicy=fail,sideEffects=None,groups=lakeflow.io,resources=lakeflows,verbs=create;update,versions=v1alpha1,name=vlakeflow.kb.io,admissionReviewVersions=v1

// LakeFlowCustomValidator validates LakeFlow objects at admission time. The rule
// logic lives in pkg/validation so it is shared with the in-reconcile safety net.
type LakeFlowCustomValidator struct{}

var _ admission.CustomValidator = &LakeFlowCustomValidator{}

// ValidateCreate validates a LakeFlow on creation.
func (v *LakeFlowCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	lw, err := asLakeFlow(obj)
	if err != nil {
		return nil, err
	}
	lakeflowlog.V(1).Info("validate create", "name", lw.Name, "namespace", lw.Namespace)
	return nil, asInvalidError(lw, validation.ValidateCreate(lw))
}

// ValidateUpdate validates a LakeFlow on update, including immutability rules.
func (v *LakeFlowCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	newLw, err := asLakeFlow(newObj)
	if err != nil {
		return nil, err
	}
	oldLw, err := asLakeFlow(oldObj)
	if err != nil {
		return nil, err
	}
	lakeflowlog.V(1).Info("validate update", "name", newLw.Name, "namespace", newLw.Namespace)
	return nil, asInvalidError(newLw, validation.ValidateUpdate(oldLw, newLw))
}

// ValidateDelete is a no-op; deletions are always allowed.
func (v *LakeFlowCustomValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func asLakeFlow(obj runtime.Object) (*v1alpha1.LakeFlow, error) {
	lw, ok := obj.(*v1alpha1.LakeFlow)
	if !ok {
		return nil, fmt.Errorf("expected a LakeFlow object but got %T", obj)
	}
	return lw, nil
}

// asInvalidError wraps a non-empty field.ErrorList into an Invalid API status
// error so the apiserver reports path-aware causes; returns nil when valid.
func asInvalidError(lw *v1alpha1.LakeFlow, errs field.ErrorList) error {
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(lakeFlowGroupKind, lw.Name, errs)
}
