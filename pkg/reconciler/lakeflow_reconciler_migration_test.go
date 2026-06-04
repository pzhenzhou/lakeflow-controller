package reconciler

import (
	"context"
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type patchCall struct {
	name      string
	namespace string
	patchData []byte
}

type mockMigrationArgoResourceClient struct {
	patchCalls  []patchCall
	deleteCalls []struct{ name, namespace string }
}

func (m *mockMigrationArgoResourceClient) Create(ctx context.Context, obj client.Object) error {
	return nil
}

func (m *mockMigrationArgoResourceClient) Get(ctx context.Context, name, namespace string, obj client.Object) error {
	return nil
}

func (m *mockMigrationArgoResourceClient) List(ctx context.Context, namespace string, obj client.ObjectList, opts ...client.ListOption) error {
	return nil
}

func (m *mockMigrationArgoResourceClient) Update(ctx context.Context, obj client.Object) error {
	return nil
}

func (m *mockMigrationArgoResourceClient) Delete(ctx context.Context, name, namespace string, obj client.Object) error {
	m.deleteCalls = append(m.deleteCalls, struct{ name, namespace string }{name, namespace})
	return nil
}

func (m *mockMigrationArgoResourceClient) CreateOrUpdate(ctx context.Context, obj client.Object) error {
	return nil
}

func (m *mockMigrationArgoResourceClient) Patch(ctx context.Context, name, namespace string, obj client.Object, patchData []byte, patchType types.PatchType) error {
	m.patchCalls = append(m.patchCalls, patchCall{name: name, namespace: namespace, patchData: patchData})
	return nil
}

// TestCleanupScheduleToDependencyDeletesCronWorkflow verifies that when a previously-scheduled
// workflow (status.cronWorkflow is set) transitions to dependency trigger, the old CronWorkflow
// is deleted to avoid stale scheduled executions.
//
// In the real reconciliation flow, both lw and existingLW have the NEW spec (both fetched from
// API server after the user's update). The previous trigger type is inferred from status fields.
func TestCleanupScheduleToDependencyDeletesCronWorkflow(t *testing.T) {
	mockRM := &mockMigrationArgoResourceClient{}
	r := &reconcilerImpl{resourceManager: mockRM}

	// existingLW: spec already updated to dependency (API server has new spec),
	// but status still reflects the old scheduled state.
	existingLW := &v1alpha1.LakeFlow{}
	existingLW.Spec.WorkflowTrigger.Depend.Workflows = []v1alpha1.Upstream{
		{Name: "upstream-a", Namespace: "default"},
	}
	existingLW.Status.CronWorkflow = "test-workflow" // <-- evidence it was previously scheduled

	// desiredLW has the same new spec (dependency trigger).
	desiredLW := &v1alpha1.LakeFlow{}
	desiredLW.Name = "test-workflow"
	desiredLW.Namespace = "default"
	desiredLW.Spec.WorkflowTrigger.Depend.Workflows = []v1alpha1.Upstream{
		{Name: "upstream-a", Namespace: "default"},
	}

	err := r.cleanupObsoleteResources(context.Background(), desiredLW, existingLW)
	assert.NoError(t, err)

	// Should delete, not patch
	assert.Empty(t, mockRM.patchCalls, "Should NOT patch CronWorkflow")
	assert.Len(t, mockRM.deleteCalls, 1)
	assert.Equal(t, "test-workflow", mockRM.deleteCalls[0].name)
	assert.Equal(t, "default", mockRM.deleteCalls[0].namespace)
}

// TestCleanupScheduleToImmediateDeletesCronWorkflow verifies schedule -> immediate also deletes.
func TestCleanupScheduleToImmediateDeletesCronWorkflow(t *testing.T) {
	mockRM := &mockMigrationArgoResourceClient{}
	r := &reconcilerImpl{resourceManager: mockRM}

	existingLW := &v1alpha1.LakeFlow{}
	existingLW.Status.CronWorkflow = "example-cron"

	desiredLW := &v1alpha1.LakeFlow{}
	desiredLW.Name = "example"
	desiredLW.Namespace = "default"
	// No schedule, no dependency → immediate

	err := r.cleanupObsoleteResources(context.Background(), desiredLW, existingLW)
	assert.NoError(t, err)
	assert.Empty(t, mockRM.patchCalls, "Should NOT patch CronWorkflow")
	assert.Len(t, mockRM.deleteCalls, 1)
	assert.Equal(t, "example-cron", mockRM.deleteCalls[0].name)
	assert.Equal(t, "default", mockRM.deleteCalls[0].namespace)
}

// TestCleanupDependencyToScheduleSkipsSuspend verifies that when entering schedule mode
// (status.cronWorkflow is empty), no suspend happens.
func TestCleanupDependencyToScheduleSkipsSuspend(t *testing.T) {
	mockRM := &mockMigrationArgoResourceClient{}
	r := &reconcilerImpl{resourceManager: mockRM}

	existingLW := &v1alpha1.LakeFlow{}
	existingLW.Spec.WorkflowTrigger.Schedule.Cron = "0 * * * *"
	existingLW.Status.CronWorkflow = "" // was NOT previously scheduled

	desiredLW := &v1alpha1.LakeFlow{}
	desiredLW.Name = "example"
	desiredLW.Namespace = "default"
	desiredLW.Spec.WorkflowTrigger.Schedule.Cron = "0 * * * *"

	err := r.cleanupObsoleteResources(context.Background(), desiredLW, existingLW)
	assert.NoError(t, err)
	assert.Empty(t, mockRM.patchCalls)
	assert.Empty(t, mockRM.deleteCalls)
}

// TestCleanupScheduleToScheduleSkipsSuspend verifies that updating schedule params
// (still scheduled, status.cronWorkflow set) does NOT suspend the CronWorkflow.
func TestCleanupScheduleToScheduleSkipsSuspend(t *testing.T) {
	mockRM := &mockMigrationArgoResourceClient{}
	r := &reconcilerImpl{resourceManager: mockRM}

	existingLW := &v1alpha1.LakeFlow{}
	existingLW.Spec.WorkflowTrigger.Schedule.Cron = "0 12 * * *"
	existingLW.Status.CronWorkflow = "example-cron"

	desiredLW := &v1alpha1.LakeFlow{}
	desiredLW.Name = "example"
	desiredLW.Namespace = "default"
	desiredLW.Spec.WorkflowTrigger.Schedule.Cron = "0 12 * * *"

	err := r.cleanupObsoleteResources(context.Background(), desiredLW, existingLW)
	assert.NoError(t, err)
	assert.Empty(t, mockRM.patchCalls, "Should not suspend CronWorkflow when staying in schedule mode")
	assert.Empty(t, mockRM.deleteCalls, "Should not delete CronWorkflow when staying in schedule mode")
}

// TestCleanupNoCronStatusNeverSuspends verifies that when status.cronWorkflow is empty,
// no suspend is attempted regardless of desired trigger type.
func TestCleanupNoCronStatusNeverSuspends(t *testing.T) {
	mockRM := &mockMigrationArgoResourceClient{}
	r := &reconcilerImpl{resourceManager: mockRM}

	existingLW := &v1alpha1.LakeFlow{}
	existingLW.Status.CronWorkflow = ""

	desiredLW := &v1alpha1.LakeFlow{}
	desiredLW.Name = "example"
	desiredLW.Namespace = "default"

	err := r.cleanupObsoleteResources(context.Background(), desiredLW, existingLW)
	assert.NoError(t, err)
	assert.Empty(t, mockRM.patchCalls)
	assert.Empty(t, mockRM.deleteCalls)
}

func TestDetermineTriggerClass(t *testing.T) {
	var trigger v1alpha1.Trigger
	assert.Equal(t, triggerClassImmediate, determineTriggerClass(trigger))

	trigger.Schedule.Cron = "0 0 * * *"
	assert.Equal(t, triggerClassSchedule, determineTriggerClass(trigger))

	trigger.Schedule.Cron = ""
	trigger.Depend.Workflows = []v1alpha1.Upstream{{Name: "upstream", Namespace: "default"}}
	assert.Equal(t, triggerClassDependency, determineTriggerClass(trigger))
}

func TestPreProcessForUpdateIncludesCronWorkflowWhenEnteringSchedule(t *testing.T) {
	profile, _ := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	r := &reconcilerImpl{
		workflowConverter: adapter.NewWorkflowConverter(profile),
	}

	existingLW := &v1alpha1.LakeFlow{}
	existingLW.Name = "example"
	existingLW.Namespace = "default"
	existingLW.Spec.WorkflowTrigger.Depend.Workflows = []v1alpha1.Upstream{
		{Name: "upstream-a", Namespace: "default"},
	}
	existingLW.Status.WorkflowTemplate = "example-template-old"
	existingLW.Status.CronWorkflow = "" // previously non-scheduled

	desiredLW := existingLW.DeepCopy()
	desiredLW.Spec.WorkflowTrigger.Depend.Workflows = nil
	desiredLW.Spec.WorkflowTrigger.Schedule.Cron = "*/5 * * * *"

	argoResources, err := r.preProcessForUpdate(context.Background(), desiredLW, existingLW)
	assert.NoError(t, err)
	assert.NotNil(t, argoResources.WorkflowTemplate)
	assert.NotNil(t, argoResources.CronWorkflow, "CronWorkflow must be included when entering schedule mode")
	assert.Equal(t, "example", argoResources.CronWorkflow.Name)
}

// Compile-time interface check.
var _ interface {
	Create(context.Context, client.Object) error
	Get(context.Context, string, string, client.Object) error
	List(context.Context, string, client.ObjectList, ...client.ListOption) error
	Update(context.Context, client.Object) error
	Delete(context.Context, string, string, client.Object) error
	CreateOrUpdate(context.Context, client.Object) error
	Patch(context.Context, string, string, client.Object, []byte, types.PatchType) error
} = (*mockMigrationArgoResourceClient)(nil)

// Silence unused import check for Argo type in this test file.
var _ = &argowfv1.CronWorkflow{}
