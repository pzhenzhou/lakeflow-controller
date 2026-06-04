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

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// lakeFlowHandlerFor returns an event handler that maps watched objects of type
// T back to their owning LakeFlow. The type parameter guards against spurious
// events for other kinds delivered through a shared watch.
func lakeFlowHandlerFor[T client.Object]() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
		if _, ok := obj.(T); !ok {
			return nil
		}
		return lakeFlowRequestsForObject(obj)
	})
}

func lakeFlowRequestsForObject(obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}

	for _, ownerRef := range obj.GetOwnerReferences() {
		if ownerRef.APIVersion == v1alpha1.GroupVersion.String() && ownerRef.Kind == "LakeFlow" {
			return lakeFlowRequest(ownerRef.Name, obj.GetNamespace())
		}
	}

	if lwName := obj.GetLabels()[v1alpha1.WorkflowNameLabel]; lwName != "" {
		return lakeFlowRequest(lwName, obj.GetNamespace())
	}

	return nil
}

func lakeFlowRequest(name, namespace string) []reconcile.Request {
	return []reconcile.Request{
		{
			NamespacedName: client.ObjectKey{
				Name:      name,
				Namespace: namespace,
			},
		},
	}
}
