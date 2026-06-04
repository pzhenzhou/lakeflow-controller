package reconciler

import (
	"fmt"
	"strings"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// resourceComparator handles change detection for Argo resources
type resourceComparator struct{}

// newResourceComparator creates a new resourceComparator
func newResourceComparator() *resourceComparator {
	return &resourceComparator{}
}

// hasResourceChanged compares the existing resource with the desired resource to determine if an update is needed
// It focuses on comparing the spec and relevant metadata fields while ignoring fields managed by Kubernetes
func (c *resourceComparator) hasResourceChanged(existing, desired client.Object) (bool, error) {
	switch existingRes := existing.(type) {
	case *argowfv1.WorkflowTemplate:
		desiredRes := desired.(*argowfv1.WorkflowTemplate)
		return c.hasWorkflowTemplateChanged(existingRes, desiredRes), nil
	case *argowfv1.Workflow:
		desiredRes := desired.(*argowfv1.Workflow)
		return c.hasWorkflowChanged(existingRes, desiredRes), nil
	case *argowfv1.CronWorkflow:
		desiredRes := desired.(*argowfv1.CronWorkflow)
		return c.hasCronWorkflowChanged(existingRes, desiredRes), nil
	default:
		return false, fmt.Errorf("unsupported resource type for comparison: %T", existing)
	}
}

// metadataChanged compares the user-managed metadata (labels + owner references)
// shared by every Argo resource type. System-managed labels are filtered out by
// labelsEqual. The per-type wrappers below add the spec comparison on top.
func (c *resourceComparator) metadataChanged(existing, desired client.Object) bool {
	if !c.labelsEqual(existing.GetLabels(), desired.GetLabels()) {
		return true
	}
	return !c.ownerReferencesEqual(existing.GetOwnerReferences(), desired.GetOwnerReferences())
}

// hasWorkflowChanged compares Workflow specs and relevant metadata
func (c *resourceComparator) hasWorkflowChanged(existing, desired *argowfv1.Workflow) bool {
	return c.metadataChanged(existing, desired) || !equality.Semantic.DeepEqual(existing.Spec, desired.Spec)
}

// hasWorkflowTemplateChanged compares WorkflowTemplate specs and relevant metadata
func (c *resourceComparator) hasWorkflowTemplateChanged(existing, desired *argowfv1.WorkflowTemplate) bool {
	return c.metadataChanged(existing, desired) || !equality.Semantic.DeepEqual(existing.Spec, desired.Spec)
}

// hasCronWorkflowChanged compares CronWorkflow specs and relevant metadata
func (c *resourceComparator) hasCronWorkflowChanged(existing, desired *argowfv1.CronWorkflow) bool {
	return c.metadataChanged(existing, desired) || !equality.Semantic.DeepEqual(existing.Spec, desired.Spec)
}

// labelsEqual compares two label maps, filtering out system-managed labels
func (c *resourceComparator) labelsEqual(existing, desired map[string]string) bool {
	// Filter out system-managed labels for comparison
	existingFiltered := c.filterUserManagedLabels(existing)
	desiredFiltered := c.filterUserManagedLabels(desired)

	if len(existingFiltered) != len(desiredFiltered) {
		return false
	}

	for k, v := range desiredFiltered {
		if existingFiltered[k] != v {
			return false
		}
	}
	return true
}

// filterUserManagedLabels filters out system-managed labels from a label map
func (c *resourceComparator) filterUserManagedLabels(labels map[string]string) map[string]string {
	filtered := make(map[string]string)

	for k, v := range labels {
		isSystemManaged := false
		for _, prefix := range systemManagedPrefixes {
			if strings.HasPrefix(k, prefix) {
				isSystemManaged = true
				break
			}
		}
		if !isSystemManaged {
			filtered[k] = v
		}
	}
	return filtered
}

// ownerReferencesEqual compares two slices of OwnerReferences
func (c *resourceComparator) ownerReferencesEqual(existing, desired []metav1.OwnerReference) bool {
	if len(existing) != len(desired) {
		return false
	}

	// Create maps for easier comparison
	existingMap := make(map[string]metav1.OwnerReference)
	for _, ref := range existing {
		key := fmt.Sprintf("%s/%s/%s", ref.APIVersion, ref.Kind, ref.Name)
		existingMap[key] = ref
	}

	for _, ref := range desired {
		key := fmt.Sprintf("%s/%s/%s", ref.APIVersion, ref.Kind, ref.Name)
		if existingRef, exists := existingMap[key]; !exists {
			return false
		} else if existingRef.UID != ref.UID {
			return false
		}
	}
	return true
}
