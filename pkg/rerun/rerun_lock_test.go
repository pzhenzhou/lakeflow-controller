package rerun

import (
	"context"
	"fmt"
	"testing"
	"time"

	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
)

func lockTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, sparkv1beta2.AddToScheme(scheme))
	return scheme
}

func testLakeFlow() *v1alpha1.LakeFlow {
	return &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-a", Namespace: "ns1"},
	}
}

func getLease(t *testing.T, c client.Client, ns, name string) (*coordinationv1.Lease, error) {
	t.Helper()
	lease := &coordinationv1.Lease{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, lease)
	return lease, err
}

func TestGuardRerunDisabledShortCircuits(t *testing.T) {
	// Disabled feature must not touch any client (directClient is nil).
	m := &workflowRerunManager{taskLockEnabled: false}
	lw := testLakeFlow()

	proceed, lock, outcome, reason, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.NoError(t, err)
	assert.True(t, proceed)
	assert.Equal(t, RerunCreated, outcome)
	assert.Empty(t, reason)
	require.NotNil(t, lock)
	// release must be safe to call.
	lock.release(false)
}

func TestGuardRerunNoSparkTasksShortCircuits(t *testing.T) {
	m := &workflowRerunManager{taskLockEnabled: true}
	lw := testLakeFlow()

	proceed, lock, _, _, err := m.guardRerun(context.Background(), lw, nil)
	require.NoError(t, err)
	assert.True(t, proceed)
	lock.release(false)
}

func TestGuardRerunAcquiresLeaseAndFailureReleaseDeletes(t *testing.T) {
	scheme := lockTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}
	lw := testLakeFlow()

	proceed, lock, outcome, _, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.NoError(t, err)
	require.True(t, proceed)
	assert.Equal(t, RerunCreated, outcome)

	// Lease should exist while held.
	name := leaseName(lw.Name, "task1")
	_, getErr := getLease(t, c, lw.Namespace, name)
	require.NoError(t, getErr)

	// A failure release (success=false) deletes the lease immediately so a
	// legitimate retry is not blocked.
	lock.release(false)
	_, getErr = getLease(t, c, lw.Namespace, name)
	assert.True(t, apierrors.IsNotFound(getErr), "lease should be deleted after a failure release, got %v", getErr)
}

func TestGuardRerunSuccessReleaseKeepsLeaseUntilTTL(t *testing.T) {
	scheme := lockTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}
	lw := testLakeFlow()

	proceed, lock, _, _, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.NoError(t, err)
	require.True(t, proceed)

	name := leaseName(lw.Name, "task1")

	// A success release (success=true) stops renewal but LEAVES the lease in
	// place to expire by TTL, bridging the SparkApplication visibility gap so a
	// concurrent rerun is serialized rather than racing through.
	lock.release(true)
	lease, getErr := getLease(t, c, lw.Namespace, name)
	require.NoError(t, getErr, "lease must be retained after a success release to bridge the gap")
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.True(t, isLeaseValid(lease, time.Now()), "retained lease should still be within its TTL window")
}

func TestGuardRerunForeignValidLeaseIsBusy(t *testing.T) {
	scheme := lockTestScheme(t)
	lw := testLakeFlow()
	name := leaseName(lw.Name, "task1")

	foreignHolder := "other-host/other-id"
	now := metav1.NewMicroTime(time.Now())
	duration := int32(rerunLockLeaseDuration.Seconds())
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: lw.Namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &foreignHolder,
			LeaseDurationSeconds: &duration,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}

	proceed, lock, outcome, reason, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.NoError(t, err)
	assert.False(t, proceed)
	assert.Equal(t, RerunBusyLocked, outcome)
	assert.NotEmpty(t, reason)
	lock.release(false)

	// The foreign lease must be untouched.
	lease, getErr := getLease(t, c, lw.Namespace, name)
	require.NoError(t, getErr)
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, foreignHolder, *lease.Spec.HolderIdentity)
}

func TestGuardRerunExpiredLeaseIsTakenOver(t *testing.T) {
	scheme := lockTestScheme(t)
	lw := testLakeFlow()
	name := leaseName(lw.Name, "task1")

	foreignHolder := "other-host/other-id"
	stale := metav1.NewMicroTime(time.Now().Add(-2 * rerunLockLeaseDuration))
	duration := int32(rerunLockLeaseDuration.Seconds())
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: lw.Namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &foreignHolder,
			LeaseDurationSeconds: &duration,
			AcquireTime:          &stale,
			RenewTime:            &stale,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}

	proceed, lock, outcome, _, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.NoError(t, err)
	assert.True(t, proceed)
	assert.Equal(t, RerunCreated, outcome)

	// Lease is now held by a new (non-foreign) holder.
	lease, getErr := getLease(t, c, lw.Namespace, name)
	require.NoError(t, getErr)
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.NotEqual(t, foreignHolder, *lease.Spec.HolderIdentity)

	lock.release(false)
}

func TestGuardRerunRunningSparkTaskIsIgnored(t *testing.T) {
	scheme := lockTestScheme(t)
	lw := testLakeFlow()

	runningApp := &sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-1",
			Namespace: lw.Namespace,
			Labels: map[string]string{
				v1alpha1.WorkflowNameLabel:     lw.Name,
				v1alpha1.WorkflowTaskNameLabel: "task1",
			},
		},
		Status: sparkv1beta2.SparkApplicationStatus{
			AppState: sparkv1beta2.ApplicationState{State: sparkv1beta2.ApplicationStateRunning},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runningApp).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}

	proceed, lock, outcome, reason, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.NoError(t, err)
	assert.False(t, proceed)
	assert.Equal(t, RerunIgnoredRunning, outcome)
	assert.Contains(t, reason, "task1")
	lock.release(false)

	// The lease acquired for the check must have been released.
	_, getErr := getLease(t, c, lw.Namespace, leaseName(lw.Name, "task1"))
	assert.True(t, apierrors.IsNotFound(getErr), "lease should be released after ignore, got %v", getErr)
}

func TestGuardRerunCompletedSparkTaskAllowsProceed(t *testing.T) {
	scheme := lockTestScheme(t)
	lw := testLakeFlow()

	doneApp := &sparkv1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-done",
			Namespace: lw.Namespace,
			Labels: map[string]string{
				v1alpha1.WorkflowNameLabel:     lw.Name,
				v1alpha1.WorkflowTaskNameLabel: "task1",
			},
		},
		Status: sparkv1beta2.SparkApplicationStatus{
			AppState: sparkv1beta2.ApplicationState{State: sparkv1beta2.ApplicationStateCompleted},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(doneApp).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}

	proceed, lock, outcome, _, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.NoError(t, err)
	assert.True(t, proceed)
	assert.Equal(t, RerunCreated, outcome)
	lock.release(false)
}

func TestGuardRerunSparkListErrorIsErrorNotIgnored(t *testing.T) {
	scheme := lockTestScheme(t)
	lw := testLakeFlow()

	c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*sparkv1beta2.SparkApplicationList); ok {
				return fmt.Errorf("simulated list failure")
			}
			return cl.List(ctx, list, opts...)
		},
	}).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}

	proceed, lock, outcome, _, err := m.guardRerun(context.Background(), lw, []string{"task1"})
	require.Error(t, err, "a failed observation must surface as an error, never IgnoredRunning")
	assert.False(t, proceed)
	assert.NotEqual(t, RerunIgnoredRunning, outcome)
	lock.release(false)

	// The lease acquired before the failed check must have been released.
	_, getErr := getLease(t, c, lw.Namespace, leaseName(lw.Name, "task1"))
	assert.True(t, apierrors.IsNotFound(getErr), "lease should be released after list error, got %v", getErr)
}

func TestReleaseLeaseDoesNotDeleteTakenOverLease(t *testing.T) {
	scheme := lockTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	m := &workflowRerunManager{taskLockEnabled: true, directClient: c}
	lw := testLakeFlow()

	holderA := "host-a/id-a"
	name := leaseName(lw.Name, "task1")
	leaseA := m.newLease(lw, name, "task1", holderA)
	require.NoError(t, c.Create(context.Background(), leaseA))

	// A different holder takes over the lease.
	holderB := "host-b/id-b"
	current, getErr := getLease(t, c, lw.Namespace, name)
	require.NoError(t, getErr)
	current.Spec.HolderIdentity = &holderB
	require.NoError(t, c.Update(context.Background(), current))

	// Holder A's release must be a no-op (identity mismatch).
	m.releaseLease(context.Background(), leaseA, holderA)

	after, getErr := getLease(t, c, lw.Namespace, name)
	require.NoError(t, getErr, "lease taken over by another holder must not be deleted")
	require.NotNil(t, after.Spec.HolderIdentity)
	assert.Equal(t, holderB, *after.Spec.HolderIdentity)
}

func TestLeaseNameDeterministicAndValid(t *testing.T) {
	a := leaseName("flow-a", "task1")
	b := leaseName("flow-a", "task1")
	assert.Equal(t, a, b, "lease name must be deterministic")
	assert.NotEqual(t, leaseName("flow-a", "task1"), leaseName("flow-a", "task2"))
	assert.LessOrEqual(t, len(a), maxLeaseNameLen)

	// Very long inputs stay within the RFC1123 limit.
	long := leaseName("a-very-long-lakeflow-name-that-exceeds-the-limit-significantly", "another-very-long-task-name")
	assert.LessOrEqual(t, len(long), maxLeaseNameLen)
}

func TestSparkTaskNamesFiltersSparkOnly(t *testing.T) {
	tasks := []v1alpha1.Task{
		{Name: "s1", Executor: v1alpha1.TaskExecutorSpark},
		{Name: "b1", Executor: v1alpha1.TaskExecutorBash},
		{Name: "s2", Executor: v1alpha1.TaskExecutorSpark},
		{Name: "p1", Executor: v1alpha1.TaskExecutorPython},
	}
	got := sparkTaskNames(tasks)
	assert.Equal(t, []string{"s1", "s2"}, got)
}
