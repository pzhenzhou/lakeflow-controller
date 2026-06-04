package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/rerun"
	"github.com/pzhenzhou/lakeflow-controller/pkg/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fakeRerunManager is a controllable rerun.WorkflowRerunManager for testing
// handleRerun's outcome handling without touching the cluster.
type fakeRerunManager struct {
	resubmit     bool
	retry        bool
	resubmitRes  rerun.RerunResult
	resubmitErr  error
	retryRes     rerun.RerunResult
	retryErr     error
	clearCalled  bool
	tokenAtRun   string
	resubmitRan  bool
	retryRan     bool
	clearReturns error
}

func (f *fakeRerunManager) ShouldResubmit(lw *v1alpha1.LakeFlow) (bool, string) {
	return f.resubmit, "resubmit-reason"
}

func (f *fakeRerunManager) ShouldRetry(lw *v1alpha1.LakeFlow) (bool, string) {
	return f.retry, "retry-reason"
}

func (f *fakeRerunManager) ResubmitWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow, reason string) (rerun.RerunResult, error) {
	f.resubmitRan = true
	f.tokenAtRun = lw.GetAnnotations()[v1alpha1.RerunTokenAnnotation]
	return f.resubmitRes, f.resubmitErr
}

func (f *fakeRerunManager) RetryWorkflow(ctx context.Context, lw *v1alpha1.LakeFlow, reason string) (rerun.RerunResult, error) {
	f.retryRan = true
	f.tokenAtRun = lw.GetAnnotations()[v1alpha1.RerunTokenAnnotation]
	return f.retryRes, f.retryErr
}

func (f *fakeRerunManager) ClearRerunAnnotations(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	f.clearCalled = true
	return f.clearReturns
}

func (f *fakeRerunManager) HasActiveWorkflowInstance(ctx context.Context, lw *v1alpha1.LakeFlow) (bool, string, error) {
	return false, "", nil
}

func rerunTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	return scheme
}

func newRerunTestHarness(t *testing.T, mgr *fakeRerunManager, ann map[string]string) (*LakeFlowController, *v1alpha1.LakeFlow, *record.FakeRecorder, *status.WorkflowStatusManager) {
	t.Helper()
	scheme := rerunTestScheme(t)
	lw := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-a", Namespace: "ns1", Annotations: ann},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(lw).Build()
	rec := record.NewFakeRecorder(16)
	r := &LakeFlowController{Client: cl, EventRecorder: rec, RerunManager: mgr}
	sm := status.NewStatusManager(cl, nil, lw, logr.Discard(), nil)
	return r, lw, rec, sm
}

func drainEvents(rec *record.FakeRecorder) []string {
	close(rec.Events)
	var out []string
	for e := range rec.Events {
		out = append(out, e)
	}
	return out
}

func eventsContain(events []string, substr string) bool {
	for _, e := range events {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func TestHandleRerunResubmitCreatedClearsAndStampsToken(t *testing.T) {
	mgr := &fakeRerunManager{
		resubmit:    true,
		resubmitRes: rerun.RerunResult{Outcome: rerun.RerunCreated, Message: "created"},
	}
	r, lw, rec, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.ResubmitRequestAnnotation: "true",
	})

	res, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, ResubmitRequeueInterval, res.RequeueAfter)
	assert.True(t, mgr.resubmitRan)
	assert.NotEmpty(t, mgr.tokenAtRun, "token must be stamped before run")
	assert.True(t, mgr.clearCalled, "Created is terminal and must clear annotations")

	events := drainEvents(rec)
	assert.True(t, eventsContain(events, "RerunStarted"))
}

func TestHandleRerunBusyLockedPreservesAnnotations(t *testing.T) {
	mgr := &fakeRerunManager{
		resubmit:    true,
		resubmitRes: rerun.RerunResult{Outcome: rerun.RerunBusyLocked, Message: "busy"},
	}
	r, lw, rec, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.ResubmitRequestAnnotation: "true",
	})

	res, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.GreaterOrEqual(t, res.RequeueAfter, RerunBusyRequeueInterval)
	assert.False(t, mgr.clearCalled, "BusyLocked must NOT clear annotations so the request survives")

	events := drainEvents(rec)
	assert.True(t, eventsContain(events, "RerunBusy"))
}

func TestHandleRerunErrorPreservesAnnotations(t *testing.T) {
	mgr := &fakeRerunManager{
		resubmit:    true,
		resubmitErr: errors.New("boom"),
	}
	r, lw, rec, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.ResubmitRequestAnnotation: "true",
	})

	_, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.Error(t, err)
	assert.True(t, handled)
	assert.False(t, mgr.clearCalled, "errors must NOT clear annotations so the request is retried")

	events := drainEvents(rec)
	assert.True(t, eventsContain(events, "RerunFailed"))
}

func TestHandleRerunUnknownOutcomeIsError(t *testing.T) {
	mgr := &fakeRerunManager{
		resubmit:    true,
		resubmitRes: rerun.RerunResult{Outcome: rerun.RerunUnknown},
	}
	r, lw, _, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.ResubmitRequestAnnotation: "true",
	})

	_, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.Error(t, err, "the zero-value outcome must be treated as an error")
	assert.True(t, handled)
	assert.False(t, mgr.clearCalled)
}

func TestHandleRerunIgnoredRunningClearsAndRecordsCondition(t *testing.T) {
	mgr := &fakeRerunManager{
		resubmit:    true,
		resubmitRes: rerun.RerunResult{Outcome: rerun.RerunIgnoredRunning, Message: "spark task \"t1\" already running"},
	}
	r, lw, rec, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.ResubmitRequestAnnotation: "true",
	})

	res, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, ResubmitRequeueInterval, res.RequeueAfter)
	assert.True(t, mgr.clearCalled, "IgnoredRunning is a terminal cluster-state decision and clears annotations")

	// A user-visible condition must be recorded.
	found := false
	for _, c := range lw.Status.Conditions {
		if c.Type == status.ConditionTypeRerunIgnored {
			found = true
		}
	}
	assert.True(t, found, "a RerunIgnored condition must be recorded")

	events := drainEvents(rec)
	assert.True(t, eventsContain(events, "RerunIgnoredRunning"))
}

func TestHandleRerunRetryNoopDoesNotIncrementAttempts(t *testing.T) {
	mgr := &fakeRerunManager{
		retry:    true,
		retryRes: rerun.RerunResult{Outcome: rerun.RerunNoop, Message: "no failed tasks"},
	}
	r, lw, rec, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.RetryRequestAnnotation: "true",
	})

	_, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.True(t, mgr.retryRan)
	assert.True(t, mgr.clearCalled)
	assert.Nil(t, lw.Status.RetryStatus, "a no-op retry must not increment retry attempts")

	events := drainEvents(rec)
	assert.True(t, eventsContain(events, "RerunNoop"))
}

func TestHandleRerunRetryCreatedIncrementsAttempts(t *testing.T) {
	mgr := &fakeRerunManager{
		retry:    true,
		retryRes: rerun.RerunResult{Outcome: rerun.RerunCreated, Message: "retry started"},
	}
	r, lw, rec, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.RetryRequestAnnotation: "true",
	})

	res, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, RetryRequeueInterval, res.RequeueAfter)
	require.NotNil(t, lw.Status.RetryStatus, "a created retry must increment retry attempts")
	assert.Equal(t, int32(1), lw.Status.RetryStatus.RetryAttempts)

	events := drainEvents(rec)
	assert.True(t, eventsContain(events, "RerunStarted"))
}

func TestHandleRerunRetryCreatedClearFailureDoesNotIncrement(t *testing.T) {
	mgr := &fakeRerunManager{
		retry:        true,
		retryRes:     rerun.RerunResult{Outcome: rerun.RerunCreated, Message: "retry started"},
		clearReturns: errors.New("clear failed"),
	}
	r, lw, rec, sm := newRerunTestHarness(t, mgr, map[string]string{
		v1alpha1.RetryRequestAnnotation: "true",
	})

	_, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.Error(t, err, "a clear failure must abort with an error so the request is retried")
	assert.True(t, handled)
	assert.True(t, mgr.clearCalled)
	assert.Nil(t, lw.Status.RetryStatus, "retry attempts must NOT be incremented when clearing fails")

	events := drainEvents(rec)
	assert.True(t, eventsContain(events, "RerunFailed"))
}

func TestHandleRerunNoAnnotationIsNotHandled(t *testing.T) {
	mgr := &fakeRerunManager{}
	r, lw, _, sm := newRerunTestHarness(t, mgr, nil)

	_, handled, err := r.handleRerun(context.Background(), lw, sm, logr.Discard())
	require.NoError(t, err)
	assert.False(t, handled)
	assert.False(t, mgr.resubmitRan)
	assert.False(t, mgr.retryRan)
}
