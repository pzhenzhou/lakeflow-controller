package rerun

import (
	"context"
	"errors"
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestResubmitWorkflowUsesInjectedConverter(t *testing.T) {
	sentinel := errors.New("injected converter used")
	converter := &sentinelConverter{err: sentinel}
	manager := &workflowRerunManager{
		resourceManager:   emptyArgoResourceClient{},
		workflowConverter: converter,
		config:            &rest.Config{},
	}

	_, err := manager.ResubmitWorkflow(context.Background(), &v1alpha1.LakeFlow{}, "test")

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.True(t, converter.called)
}

type sentinelConverter struct {
	called bool
	err    error
}

func (c *sentinelConverter) Convert(*v1alpha1.LakeFlow, *rest.Config) (*adapter.ArgoWorkflowCR, error) {
	c.called = true
	return nil, c.err
}

type emptyArgoResourceClient struct{}

func (emptyArgoResourceClient) Create(context.Context, client.Object) error { return nil }
func (emptyArgoResourceClient) Get(context.Context, string, string, client.Object) error {
	return nil
}
func (emptyArgoResourceClient) List(context.Context, string, client.ObjectList, ...client.ListOption) error {
	return nil
}
func (emptyArgoResourceClient) Update(context.Context, client.Object) error { return nil }
func (emptyArgoResourceClient) Delete(context.Context, string, string, client.Object) error {
	return nil
}
func (emptyArgoResourceClient) CreateOrUpdate(context.Context, client.Object) error { return nil }
func (emptyArgoResourceClient) Patch(context.Context, string, string, client.Object, []byte, types.PatchType) error {
	return nil
}

// labeledGetClient stubs Get to stamp configured labels (or return an error) on
// the fetched object, for verifyExistingRerunResource tests.
type labeledGetClient struct {
	emptyArgoResourceClient
	labels map[string]string
	getErr error
}

func (c labeledGetClient) Get(_ context.Context, _, _ string, obj client.Object) error {
	if c.getErr != nil {
		return c.getErr
	}
	obj.SetLabels(c.labels)
	return nil
}

func TestVerifyExistingRerunResource(t *testing.T) {
	const (
		name    = "flow-a-resubmit-abc123"
		ns      = "ns1"
		lwName  = "flow-a"
		token   = "tok-123"
		marker  = v1alpha1.IsResubmitWorkflowLabel
		otherTk = "tok-999"
	)

	matching := map[string]string{
		marker:                        "true",
		v1alpha1.WorkflowNameLabel:    lwName,
		v1alpha1.RerunTokenAnnotation: token,
	}

	tests := []struct {
		name    string
		client  labeledGetClient
		wantErr bool
	}{
		{
			name:   "matching object is idempotent",
			client: labeledGetClient{labels: matching},
		},
		{
			name:    "get error surfaces",
			client:  labeledGetClient{getErr: errors.New("boom")},
			wantErr: true,
		},
		{
			name: "missing marker label",
			client: labeledGetClient{labels: map[string]string{
				v1alpha1.WorkflowNameLabel:    lwName,
				v1alpha1.RerunTokenAnnotation: token,
			}},
			wantErr: true,
		},
		{
			name: "different lakeflow owner",
			client: labeledGetClient{labels: map[string]string{
				marker:                        "true",
				v1alpha1.WorkflowNameLabel:    "other-flow",
				v1alpha1.RerunTokenAnnotation: token,
			}},
			wantErr: true,
		},
		{
			name: "stale token mismatch",
			client: labeledGetClient{labels: map[string]string{
				marker:                        "true",
				v1alpha1.WorkflowNameLabel:    lwName,
				v1alpha1.RerunTokenAnnotation: otherTk,
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &workflowRerunManager{resourceManager: tt.client}
			err := m.verifyExistingRerunResource(context.Background(),
				&v1alpha1.LakeFlow{}, name, ns, lwName, token, marker)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
