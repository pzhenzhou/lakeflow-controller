// Package argoclient provides a thin CRUD client over Argo Workflows resources
// plus a create-with-retry helper that absorbs Argo informer cache lag. It is
// intentionally decoupled from the conversion/rendering code in pkg/adapter so
// reconcile, status, and rerun can depend on just the client.
package argoclient

import (
	"context"
	"fmt"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var logger = common.GetSharedLogger().WithName("argo-resource-client")

// ArgoResourceClient handles CRUD operations for Argo Workflow resources.
type ArgoResourceClient interface {
	Create(ctx context.Context, obj client.Object) error
	Get(ctx context.Context, name, namespace string, obj client.Object) error
	List(ctx context.Context, namespace string, obj client.ObjectList, opts ...client.ListOption) error
	Update(ctx context.Context, obj client.Object) error
	Delete(ctx context.Context, name, namespace string, obj client.Object) error
	// CreateOrUpdate creates a resource if it doesn't exist, or updates it if it does and has changed
	CreateOrUpdate(ctx context.Context, obj client.Object) error
	// Patch patches a resource with the given patch data using the specified patch type
	Patch(ctx context.Context, name, namespace string, obj client.Object, patchData []byte, patchType types.PatchType) error
}

type argoResourceClient struct {
	client client.Client
}

// NewArgoResourceClient creates a new Argo resource manager using controller-runtime client
func NewArgoResourceClient(config *rest.Config) (ArgoResourceClient, error) {
	scheme, err := createScheme()
	if err != nil {
		logger.Error(err, "Failed to create scheme")
		return nil, fmt.Errorf("failed to create scheme: %w", err)
	}

	c, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		logger.Error(err, "Failed to create controller-runtime client")
		return nil, fmt.Errorf("failed to create controller-runtime client: %w", err)
	}

	return &argoResourceClient{
		client: c,
	}, nil
}

func (m *argoResourceClient) Create(ctx context.Context, obj client.Object) error {
	// Validate supported resource types
	if err := m.validateResourceType(obj); err != nil {
		return err
	}

	return m.client.Create(ctx, obj)
}

func (m *argoResourceClient) Get(ctx context.Context, name, namespace string, obj client.Object) error {
	// Validate supported resource types
	if err := m.validateResourceType(obj); err != nil {
		return err
	}

	key := client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}
	return m.client.Get(ctx, key, obj)
}

func (m *argoResourceClient) List(ctx context.Context, namespace string, obj client.ObjectList, opts ...client.ListOption) error {
	// Add namespace to the list options
	listOpts := append(opts, client.InNamespace(namespace))
	return m.client.List(ctx, obj, listOpts...)
}

func (m *argoResourceClient) Update(ctx context.Context, obj client.Object) error {
	// Validate supported resource types
	if err := m.validateResourceType(obj); err != nil {
		return err
	}

	return m.client.Update(ctx, obj)
}

func (m *argoResourceClient) Delete(ctx context.Context, name, namespace string, obj client.Object) error {
	// Validate supported resource types
	if err := m.validateResourceType(obj); err != nil {
		return err
	}

	key := client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}

	// We need to get the object first to delete it
	if err := m.client.Get(ctx, key, obj); err != nil {
		return err
	}

	return m.client.Delete(ctx, obj)
}

func (m *argoResourceClient) CreateOrUpdate(ctx context.Context, obj client.Object) error {
	// Validate supported resource types
	if err := m.validateResourceType(obj); err != nil {
		return err
	}

	// For Workflows, always create new ones (they're one-time executions)
	if _, ok := obj.(*argowfv1.Workflow); ok {
		return m.client.Create(ctx, obj)
	}

	// For other resources, check if they exist and update only if changed
	key := client.ObjectKeyFromObject(obj)

	// Create a new object of the same type to check existence
	var existing client.Object
	switch obj.(type) {
	case *argowfv1.WorkflowTemplate:
		existing = &argowfv1.WorkflowTemplate{}
	case *argowfv1.CronWorkflow:
		existing = &argowfv1.CronWorkflow{}
	default:
		err := fmt.Errorf("unsupported resource type: %T", obj)
		logger.Error(err, "Unsupported resource type in CreateOrUpdate", "resourceType", fmt.Sprintf("%T", obj))
		return err
	}

	err := m.client.Get(ctx, key, existing)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Resource doesn't exist, create it
			return m.client.Create(ctx, obj)
		}
		return err
	}

	// Resource exists, update it (basic implementation without change detection)
	// Note: For more sophisticated change detection, use the controller's createOrUpdateResource method
	obj.SetResourceVersion(existing.GetResourceVersion())
	return m.client.Update(ctx, obj)
}

func (m *argoResourceClient) Patch(ctx context.Context, name, namespace string, obj client.Object, patchData []byte, patchType types.PatchType) error {
	// Validate supported resource types
	if err := m.validateResourceType(obj); err != nil {
		return err
	}

	// Set the name and namespace on the object for patching
	obj.SetName(name)
	obj.SetNamespace(namespace)

	// Create patch and apply it
	patch := client.RawPatch(patchType, patchData)
	return m.client.Patch(ctx, obj, patch)
}

// validateResourceType ensures we only work with supported Argo resources
func (m *argoResourceClient) validateResourceType(obj client.Object) error {
	switch obj.(type) {
	case *argowfv1.Workflow, *argowfv1.WorkflowTemplate, *argowfv1.CronWorkflow:
		return nil // Argo Workflows resources
	default:
		err := fmt.Errorf("unsupported resource type: %T", obj)
		logger.Error(err, "Unsupported resource type in validateResourceType", "resourceType", fmt.Sprintf("%T", obj))
		return err
	}
}

// createScheme creates a scheme with all required resource types
func createScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()

	// Add Argo Workflows resources
	if err := argowfv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add Argo Workflows to scheme: %w", err)
	}

	return scheme, nil
}
