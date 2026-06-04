package status

import (
	"context"
	"fmt"
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// mockArgoResourceClient is a mock implementation of ArgoResourceClient for testing
type mockArgoResourceClient struct {
	workflows      map[string]*argowfv1.Workflow
	cronWorkflows  map[string]*argowfv1.CronWorkflow
	listWorkflows  []argowfv1.Workflow
	patchCallCount int
	lastPatchData  []byte
}

func newMockArgoResourceClient() *mockArgoResourceClient {
	return &mockArgoResourceClient{
		workflows:     make(map[string]*argowfv1.Workflow),
		cronWorkflows: make(map[string]*argowfv1.CronWorkflow),
	}
}

func (m *mockArgoResourceClient) Create(ctx context.Context, obj client.Object) error {
	return nil
}

func (m *mockArgoResourceClient) Get(ctx context.Context, name, namespace string, obj client.Object) error {
	if wf, ok := obj.(*argowfv1.Workflow); ok {
		if stored, exists := m.workflows[name]; exists {
			*wf = *stored
			return nil
		}
	}
	if cwf, ok := obj.(*argowfv1.CronWorkflow); ok {
		if stored, exists := m.cronWorkflows[name]; exists {
			*cwf = *stored
			return nil
		}
	}
	return nil
}

func (m *mockArgoResourceClient) List(ctx context.Context, namespace string, obj client.ObjectList, opts ...client.ListOption) error {
	if wfList, ok := obj.(*argowfv1.WorkflowList); ok {
		wfList.Items = m.listWorkflows
		return nil
	}
	return nil
}

func (m *mockArgoResourceClient) Update(ctx context.Context, obj client.Object) error {
	return nil
}

func (m *mockArgoResourceClient) Delete(ctx context.Context, name, namespace string, obj client.Object) error {
	return nil
}

func (m *mockArgoResourceClient) CreateOrUpdate(ctx context.Context, obj client.Object) error {
	return nil
}

func (m *mockArgoResourceClient) Patch(ctx context.Context, name, namespace string, obj client.Object, patchData []byte, patchType types.PatchType) error {
	m.patchCallCount++
	m.lastPatchData = patchData
	return nil
}

func TestStopOrTerminateChildWorkflows(t *testing.T) {
	tests := []struct {
		name             string
		cronWorkflowName string
		namespace        string
		shutdownStrategy string
		childWorkflows   []argowfv1.Workflow
		expectedPatches  int
		description      string
	}{
		{
			name:             "Stop multiple running child workflows",
			cronWorkflowName: "test-cron",
			namespace:        "test-ns",
			shutdownStrategy: "Stop",
			childWorkflows: []argowfv1.Workflow{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "child-1", Namespace: "test-ns"},
					Status:     argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowRunning},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "child-2", Namespace: "test-ns"},
					Status:     argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowPending},
				},
			},
			expectedPatches: 2,
			description:     "Should patch both running and pending workflows",
		},
		{
			name:             "No running children - no-op",
			cronWorkflowName: "test-cron",
			namespace:        "test-ns",
			shutdownStrategy: "Stop",
			childWorkflows: []argowfv1.Workflow{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "child-1", Namespace: "test-ns"},
					Status:     argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowSucceeded},
				},
			},
			expectedPatches: 0,
			description:     "Should not patch completed workflows",
		},
		{
			name:             "Terminate child workflows",
			cronWorkflowName: "test-cron",
			namespace:        "test-ns",
			shutdownStrategy: "Terminate",
			childWorkflows: []argowfv1.Workflow{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "child-1", Namespace: "test-ns"},
					Status:     argowfv1.WorkflowStatus{Phase: argowfv1.WorkflowRunning},
				},
			},
			expectedPatches: 1,
			description:     "Should use Terminate strategy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockArgoResourceClient()
			mockClient.listWorkflows = tt.childWorkflows

			handler := &controlStateHandler{
				resourceManager: mockClient,
				logger:          logr.Discard(),
			}

			err := handler.stopOrTerminateChildWorkflows(context.Background(), tt.cronWorkflowName, tt.namespace, tt.shutdownStrategy)
			assert.NoError(t, err, tt.description)
			assert.Equal(t, tt.expectedPatches, mockClient.patchCallCount, "Expected %d patches, got %d", tt.expectedPatches, mockClient.patchCallCount)

			if tt.expectedPatches > 0 {
				assert.Contains(t, string(mockClient.lastPatchData), tt.shutdownStrategy, "Patch data should contain shutdown strategy")
				// Note: Exact match may vary due to JSON formatting, so we check for content
			}
		})
	}
}

func TestHandleActiveState_ReactivateCronWorkflow(t *testing.T) {
	tests := []struct {
		name              string
		executionState    v1alpha1.WorkflowControlState
		cronWorkflowName  string
		currentPhase      v1alpha1.WorkflowPhase
		expectedUnsuspend bool
		description       string
	}{
		{
			name:              "Reactivate CronWorkflow from Stopped state",
			executionState:    v1alpha1.ControlStateStopped,
			cronWorkflowName:  "test-cron",
			currentPhase:      v1alpha1.WorkflowPhaseFailed,
			expectedUnsuspend: true,
			description:       "Should unsuspend CronWorkflow when reactivating from Stopped",
		},
		{
			name:              "Reactivate CronWorkflow from Terminated state",
			executionState:    v1alpha1.ControlStateTerminated,
			cronWorkflowName:  "test-cron",
			currentPhase:      v1alpha1.WorkflowPhaseFailed,
			expectedUnsuspend: true,
			description:       "Should unsuspend CronWorkflow when reactivating from Terminated",
		},
		{
			name:              "Reactivate regular Workflow from Stopped state",
			executionState:    v1alpha1.ControlStateStopped,
			cronWorkflowName:  "", // No CronWorkflow
			currentPhase:      v1alpha1.WorkflowPhaseFailed,
			expectedUnsuspend: false,
			description:       "Should not unsuspend regular Workflow, only update executionState",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockArgoResourceClient()
			if tt.cronWorkflowName != "" {
				mockClient.cronWorkflows[tt.cronWorkflowName] = &argowfv1.CronWorkflow{
					ObjectMeta: metav1.ObjectMeta{Name: tt.cronWorkflowName},
				}
			}

			handler := &controlStateHandler{
				resourceManager: mockClient,
				logger:          logr.Discard(),
			}

			lw := &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-lw",
					Namespace: "test-ns",
				},
				Spec: v1alpha1.LakeFlowSpec{
					State: v1alpha1.ControlStateActive,
				},
				Status: v1alpha1.LakeFlowStatus{
					ExecutionState: tt.executionState,
					CronWorkflow:   tt.cronWorkflowName,
					Phase:          tt.currentPhase,
				},
			}

			err := handler.handleActiveState(context.Background(), lw, tt.currentPhase)
			assert.NoError(t, err, tt.description)

			if tt.expectedUnsuspend {
				assert.Equal(t, v1alpha1.ControlStateActive, lw.Status.ExecutionState, "ExecutionState should be Active")
				assert.Greater(t, mockClient.patchCallCount, 0, "Should have patched CronWorkflow to unsuspend")
			} else {
				// For regular workflows, executionState should still be updated but no unsuspend
				assert.Equal(t, v1alpha1.ControlStateActive, lw.Status.ExecutionState, "ExecutionState should be Active")
			}
		})
	}
}

// controlStateMethod is a method expression over the four control-state handlers so the
// characterization table below can drive each of them uniformly.
type controlStateMethod func(*controlStateHandler, context.Context, *v1alpha1.LakeFlow, v1alpha1.WorkflowPhase) error

// TestControlStateHandlers_LockBehavior is a characterization safety net that pins the
// observable outputs (status.ExecutionState, status.Phase, and the ControlStateTransition
// condition Reason/Message) of every handle*State method across all phase branches. It exists
// to guarantee the upcoming controlStatePlan/applyControlState refactor stays behavior-preserving.
func TestControlStateHandlers_LockBehavior(t *testing.T) {
	const phaseFmt = "unknown time" // FinishedTime is left nil in every case below
	finishedMsg := func(verb, phase string) string {
		return fmt.Sprintf("Workflow finished with phase '%s' at %s. %s state acknowledged but has no effect on completed workflow. Use %s or %s annotation to re-run.",
			phase, phaseFmt, verb, v1alpha1.RetryRequestAnnotation, v1alpha1.ResubmitRequestAnnotation)
	}

	tests := []struct {
		name          string
		fn            controlStateMethod
		phase         v1alpha1.WorkflowPhase
		execState     v1alpha1.WorkflowControlState
		workflowName  string
		cronWorkflow  string
		wantExecState v1alpha1.WorkflowControlState
		wantPhase     v1alpha1.WorkflowPhase
		wantReason    string // "" means expect no ControlStateTransition condition
		wantMessage   string
	}{
		// ---- Suspend ----
		{"suspend/running", (*controlStateHandler).handleSuspendedState, v1alpha1.WorkflowPhaseRunning, "", "", "cron", v1alpha1.ControlStateSuspend, v1alpha1.WorkflowPhaseRunning, "WorkflowSuspended", "Workflow suspended successfully. Current phase: Running"},
		{"suspend/pending-no-refs", (*controlStateHandler).handleSuspendedState, v1alpha1.WorkflowPhasePending, "", "", "", v1alpha1.ControlStateSuspend, v1alpha1.WorkflowPhasePending, "WorkflowSuspendAcknowledged", "Workflow suspend state acknowledged but pending - no backend operations until resources exist"},
		{"suspend/pending-with-refs", (*controlStateHandler).handleSuspendedState, v1alpha1.WorkflowPhasePending, "", "", "cron", v1alpha1.ControlStateSuspend, v1alpha1.WorkflowPhasePending, "WorkflowSuspended", "Workflow suspended successfully. Current phase: Pending"},
		{"suspend/finished", (*controlStateHandler).handleSuspendedState, v1alpha1.WorkflowPhaseFailed, "", "", "", v1alpha1.ControlStateSuspend, v1alpha1.WorkflowPhaseFailed, "WorkflowAlreadyFinished", finishedMsg("Suspend", "Failed")},
		{"suspend/empty-no-refs", (*controlStateHandler).handleSuspendedState, "", "", "", "", v1alpha1.ControlStateSuspend, v1alpha1.WorkflowPhasePending, "WorkflowSuspendAcknowledged", "Workflow suspend state acknowledged during initialization - no backend operations until resources exist"},
		{"suspend/empty-with-refs", (*controlStateHandler).handleSuspendedState, "", "", "", "cron", v1alpha1.ControlStateSuspend, v1alpha1.WorkflowPhasePending, "WorkflowSuspended", "Workflow suspended during initialization"},
		{"suspend/already", (*controlStateHandler).handleSuspendedState, v1alpha1.WorkflowPhaseRunning, v1alpha1.ControlStateSuspend, "", "cron", v1alpha1.ControlStateSuspend, v1alpha1.WorkflowPhaseRunning, "", ""},

		// ---- Stopped ----
		{"stop/running", (*controlStateHandler).handleStoppedState, v1alpha1.WorkflowPhaseRunning, "", "", "cron", v1alpha1.ControlStateStopped, v1alpha1.WorkflowPhaseRunning, "WorkflowStopped", "Workflow stopped gracefully. Current phase: Running"},
		{"stop/pending-no-refs", (*controlStateHandler).handleStoppedState, v1alpha1.WorkflowPhasePending, "", "", "", v1alpha1.ControlStateStopped, v1alpha1.WorkflowPhasePending, "WorkflowStopAcknowledged", "Workflow stop state acknowledged but pending - no backend operations until resources exist"},
		{"stop/pending-with-refs", (*controlStateHandler).handleStoppedState, v1alpha1.WorkflowPhasePending, "", "", "cron", v1alpha1.ControlStateStopped, v1alpha1.WorkflowPhasePending, "WorkflowStopped", "Workflow stopped gracefully. Current phase: Pending"},
		{"stop/finished", (*controlStateHandler).handleStoppedState, v1alpha1.WorkflowPhaseSucceeded, "", "", "", v1alpha1.ControlStateStopped, v1alpha1.WorkflowPhaseSucceeded, "WorkflowAlreadyFinished", finishedMsg("Stop", "Succeeded")},
		{"stop/empty-no-refs", (*controlStateHandler).handleStoppedState, "", "", "", "", v1alpha1.ControlStateStopped, v1alpha1.WorkflowPhasePending, "WorkflowStopAcknowledged", "Workflow stop state acknowledged during initialization - no backend operations until resources exist"},
		{"stop/empty-with-refs", (*controlStateHandler).handleStoppedState, "", "", "", "cron", v1alpha1.ControlStateStopped, v1alpha1.WorkflowPhasePending, "WorkflowStopped", "Workflow stopped during initialization"},
		{"stop/already", (*controlStateHandler).handleStoppedState, v1alpha1.WorkflowPhaseRunning, v1alpha1.ControlStateStopped, "", "cron", v1alpha1.ControlStateStopped, v1alpha1.WorkflowPhaseRunning, "", ""},

		// ---- Terminated ----
		{"terminate/running", (*controlStateHandler).handleTerminatedState, v1alpha1.WorkflowPhaseRunning, "", "", "cron", v1alpha1.ControlStateTerminated, v1alpha1.WorkflowPhaseRunning, "WorkflowTerminated", "Workflow terminated forcefully. Current phase: Running"},
		{"terminate/pending-no-refs", (*controlStateHandler).handleTerminatedState, v1alpha1.WorkflowPhasePending, "", "", "", v1alpha1.ControlStateTerminated, v1alpha1.WorkflowPhasePending, "WorkflowTerminateAcknowledged", "Workflow terminate state acknowledged but pending - no backend operations until resources exist"},
		{"terminate/pending-with-refs", (*controlStateHandler).handleTerminatedState, v1alpha1.WorkflowPhasePending, "", "", "cron", v1alpha1.ControlStateTerminated, v1alpha1.WorkflowPhasePending, "WorkflowTerminated", "Workflow terminated forcefully. Current phase: Pending"},
		{"terminate/finished", (*controlStateHandler).handleTerminatedState, v1alpha1.WorkflowPhaseCompleted, "", "", "", v1alpha1.ControlStateTerminated, v1alpha1.WorkflowPhaseCompleted, "WorkflowAlreadyFinished", finishedMsg("Terminate", "Completed")},
		{"terminate/empty-no-refs", (*controlStateHandler).handleTerminatedState, "", "", "", "", v1alpha1.ControlStateTerminated, v1alpha1.WorkflowPhasePending, "WorkflowTerminateAcknowledged", "Workflow terminate state acknowledged during initialization - no backend operations until resources exist"},
		{"terminate/empty-with-refs", (*controlStateHandler).handleTerminatedState, "", "", "", "cron", v1alpha1.ControlStateTerminated, v1alpha1.WorkflowPhasePending, "WorkflowTerminated", "Workflow terminated during initialization"},
		{"terminate/already", (*controlStateHandler).handleTerminatedState, v1alpha1.WorkflowPhaseRunning, v1alpha1.ControlStateTerminated, "", "cron", v1alpha1.ControlStateTerminated, v1alpha1.WorkflowPhaseRunning, "", ""},

		// ---- Active (ExecutionState empty so the resume/reactivate short-circuits are skipped) ----
		{"active/idle", (*controlStateHandler).handleActiveState, v1alpha1.WorkflowPhaseIdle, "", "", "cron", v1alpha1.ControlStateActive, v1alpha1.WorkflowPhaseIdle, "WorkflowActive", "Workflow is active and idle, waiting for next scheduled execution"},
		{"active/running", (*controlStateHandler).handleActiveState, v1alpha1.WorkflowPhaseRunning, "", "", "cron", v1alpha1.ControlStateActive, v1alpha1.WorkflowPhaseRunning, "WorkflowActive", "Workflow is actively running"},
		{"active/pending-no-refs", (*controlStateHandler).handleActiveState, v1alpha1.WorkflowPhasePending, "", "", "", v1alpha1.ControlStateActive, v1alpha1.WorkflowPhasePending, "WorkflowPendingDependencies", "Workflow is active but pending - waiting for dependencies to be satisfied"},
		{"active/pending-with-refs", (*controlStateHandler).handleActiveState, v1alpha1.WorkflowPhasePending, "", "", "cron", v1alpha1.ControlStateActive, v1alpha1.WorkflowPhasePending, "WorkflowActive", "Workflow activated from pending state"},
		{"active/finished", (*controlStateHandler).handleActiveState, v1alpha1.WorkflowPhaseFailed, "", "", "", v1alpha1.ControlStateActive, v1alpha1.WorkflowPhaseFailed, "WorkflowFinishedButActive", fmt.Sprintf("Workflow finished with phase '%s' at %s. Control state is active. Use %s or %s annotation to re-run.", "Failed", phaseFmt, v1alpha1.RetryRequestAnnotation, v1alpha1.ResubmitRequestAnnotation)},
		{"active/empty", (*controlStateHandler).handleActiveState, "", "", "", "", v1alpha1.ControlStateActive, v1alpha1.WorkflowPhasePending, "WorkflowActive", "Workflow initialized in active state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockArgoResourceClient()
			if tt.cronWorkflow != "" {
				mockClient.cronWorkflows[tt.cronWorkflow] = &argowfv1.CronWorkflow{
					ObjectMeta: metav1.ObjectMeta{Name: tt.cronWorkflow},
				}
			}
			handler := &controlStateHandler{resourceManager: mockClient, logger: logr.Discard()}

			lw := &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{Name: "test-lw", Namespace: "test-ns"},
				Status: v1alpha1.LakeFlowStatus{
					ExecutionState: tt.execState,
					Phase:          tt.phase,
					WorkflowName:   tt.workflowName,
					CronWorkflow:   tt.cronWorkflow,
				},
			}

			err := tt.fn(handler, context.Background(), lw, tt.phase)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantExecState, lw.Status.ExecutionState, "ExecutionState")
			assert.Equal(t, tt.wantPhase, lw.Status.Phase, "Phase")

			cond := findLakeFlowCondition(lw.Status.Conditions, "ControlStateTransition")
			if tt.wantReason == "" {
				assert.Nil(t, cond, "expected no ControlStateTransition condition")
				return
			}
			assert.NotNil(t, cond, "expected a ControlStateTransition condition")
			if cond != nil {
				assert.Equal(t, tt.wantReason, cond.Reason, "condition Reason")
				assert.Equal(t, tt.wantMessage, cond.Message, "condition Message")
			}
		})
	}
}
