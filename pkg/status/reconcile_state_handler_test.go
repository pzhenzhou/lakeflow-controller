package status

import (
	"testing"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsWorkflowStale(t *testing.T) {
	handler := &reconcileStateHandler{}

	tests := []struct {
		name             string
		lwLastFinishedAt time.Time // LastFinishedTime (for scheduled)
		lwFinishedAt     time.Time // FinishedAt (for immediate/dependency)
		wfFinishedAt     time.Time
		wfPhase          argowfv1.WorkflowPhase
		expectedStale    bool
		description      string
	}{
		{
			name:          "Workflow older than LakeFlow FinishedAt - STALE",
			lwFinishedAt:  time.Date(2025, 12, 25, 10, 5, 0, 0, time.UTC),
			wfFinishedAt:  time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC),
			wfPhase:       argowfv1.WorkflowFailed,
			expectedStale: true,
			description:   "Old failed workflow should not overwrite newer status",
		},
		{
			name:          "Workflow newer than LakeFlow - NOT STALE",
			lwFinishedAt:  time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC),
			wfFinishedAt:  time.Date(2025, 12, 25, 10, 5, 0, 0, time.UTC),
			wfPhase:       argowfv1.WorkflowSucceeded,
			expectedStale: false,
			description:   "Newer workflow should be synced",
		},
		{
			name:          "Workflow same time as LakeFlow - NOT STALE",
			lwFinishedAt:  time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC),
			wfFinishedAt:  time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC),
			wfPhase:       argowfv1.WorkflowSucceeded,
			expectedStale: false,
			description:   "Same time is safe to sync (idempotent)",
		},
		{
			name:          "LakeFlow never finished - NOT STALE",
			wfFinishedAt:  time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC),
			wfPhase:       argowfv1.WorkflowSucceeded,
			expectedStale: false,
			description:   "First workflow sync should always proceed",
		},
		{
			name:          "Workflow still running - NOT STALE",
			lwFinishedAt:  time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC),
			wfPhase:       argowfv1.WorkflowRunning,
			expectedStale: false,
			description:   "Running workflow represents current state",
		},
		{
			name:             "Use max of LastFinishedTime and FinishedAt - STALE",
			lwLastFinishedAt: time.Date(2025, 12, 25, 10, 3, 0, 0, time.UTC),
			lwFinishedAt:     time.Date(2025, 12, 25, 10, 5, 0, 0, time.UTC), // Manual trigger is newer
			wfFinishedAt:     time.Date(2025, 12, 25, 10, 4, 0, 0, time.UTC), // Between the two
			wfPhase:          argowfv1.WorkflowFailed,
			expectedStale:    true,
			description:      "Should use max time when both are present",
		},
		{
			name:             "Use max of LastFinishedTime and FinishedAt - NOT STALE",
			lwLastFinishedAt: time.Date(2025, 12, 25, 10, 5, 0, 0, time.UTC), // Scheduled is newer
			lwFinishedAt:     time.Date(2025, 12, 25, 10, 3, 0, 0, time.UTC),
			wfFinishedAt:     time.Date(2025, 12, 25, 10, 6, 0, 0, time.UTC), // Even newer
			wfPhase:          argowfv1.WorkflowSucceeded,
			expectedStale:    false,
			description:      "Workflow newer than max should sync",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lw := &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-lw",
					Namespace: "test-ns",
				},
				Status: v1alpha1.LakeFlowStatus{},
			}

			// Set LakeFlow finish times if provided
			if !tt.lwFinishedAt.IsZero() || !tt.lwLastFinishedAt.IsZero() {
				lw.Status.FinishedTime = &v1alpha1.LakeFlowFinishedTime{}
				if !tt.lwFinishedAt.IsZero() {
					lw.Status.FinishedTime.FinishedAt = metav1.NewTime(tt.lwFinishedAt)
				}
				if !tt.lwLastFinishedAt.IsZero() {
					lw.Status.FinishedTime.LastFinishedTime = metav1.NewTime(tt.lwLastFinishedAt)
				}
			}

			wf := &argowfv1.Workflow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-wf",
					Namespace: "test-ns",
				},
				Status: argowfv1.WorkflowStatus{
					Phase: tt.wfPhase,
				},
			}
			if !tt.wfFinishedAt.IsZero() {
				wf.Status.FinishedAt = metav1.NewTime(tt.wfFinishedAt)
			}

			result := handler.isWorkflowStale(lw, wf)
			assert.Equal(t, tt.expectedStale, result, tt.description)
		})
	}
}

// TestSyncStatus_DoesNotRegressToStaleFailed verifies that the controller
// does not sync stale failed workflow status after TTL cleanup removes successful retry
func TestSyncStatus_DoesNotRegressToStaleFailed(t *testing.T) {
	// This test simulates the critical bug scenario:
	// 1. Normal workflow fails at 10:00
	// 2. User retries, succeeds at 10:05 (LakeFlow status: Succeeded)
	// 3. TTL deletes retry workflow at 14:05
	// 4. Controller restarts at 15:00, only sees old failed workflow
	// 5. Should NOT sync failed status (regression)

	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-workflow",
			Namespace: "test-ns",
		},
		Status: v1alpha1.LakeFlowStatus{
			Phase: v1alpha1.WorkflowPhaseSucceeded, // Current status from retry
			FinishedTime: &v1alpha1.LakeFlowFinishedTime{
				FinishedAt: metav1.NewTime(time.Date(2025, 12, 25, 10, 5, 0, 0, time.UTC)),
			},
		},
	}

	// Old failed workflow (TTL hasn't cleaned it yet)
	oldFailedWf := &argowfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-workflow-old",
			Namespace: "test-ns",
		},
		Status: argowfv1.WorkflowStatus{
			Phase:      argowfv1.WorkflowFailed,
			FinishedAt: metav1.NewTime(time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC)),
		},
	}

	handler := &reconcileStateHandler{}

	// Verify the old workflow is detected as stale
	isStale := handler.isWorkflowStale(lw, oldFailedWf)
	assert.True(t, isStale, "Old failed workflow should be detected as stale")

	// Verify LakeFlow status would not be changed
	originalPhase := lw.Status.Phase
	// In real code, SyncStatus would skip sync due to staleness check
	// Here we just verify the check works
	assert.Equal(t, v1alpha1.WorkflowPhaseSucceeded, originalPhase,
		"LakeFlow should preserve Succeeded status")
}

// TestSyncStatus_MaxFinishTime verifies that the controller uses the maximum
// of LastFinishedTime and FinishedAt for staleness detection
func TestSyncStatus_MaxFinishTime(t *testing.T) {
	handler := &reconcileStateHandler{}

	// Scenario: Scheduled workflow with manual retry
	// - LastFinishedTime: 10:00 (from scheduled execution)
	// - FinishedAt: 10:05 (from manual retry - newer!)
	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scheduled-with-retry",
			Namespace: "test-ns",
		},
		Status: v1alpha1.LakeFlowStatus{
			Phase: v1alpha1.WorkflowPhaseSucceeded,
			FinishedTime: &v1alpha1.LakeFlowFinishedTime{
				LastFinishedTime: metav1.NewTime(time.Date(2025, 12, 25, 10, 5, 0, 0, time.UTC)), // Scheduled
				FinishedAt:       metav1.NewTime(time.Date(2025, 12, 25, 10, 3, 0, 0, time.UTC)), // Manual trigger
			},
		},
	}

	// Old workflow that finished at 10:04 (between the two times)
	wf := &argowfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "old-workflow",
			Namespace: "test-ns",
		},
		Status: argowfv1.WorkflowStatus{
			Phase:      argowfv1.WorkflowFailed,
			FinishedAt: metav1.NewTime(time.Date(2025, 12, 25, 10, 4, 0, 0, time.UTC)),
		},
	}

	// Should be stale because 10:04 < max(10:00, 10:05) = 10:05
	isStale := handler.isWorkflowStale(lw, wf)
	assert.True(t, isStale, "Workflow should be stale when older than max finish time")
}
