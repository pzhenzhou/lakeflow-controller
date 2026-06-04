package rerun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/lithammer/shortuuid/v4"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
)

const (
	// rerunLockLeaseDuration is the validity window of a rerun lock Lease. A
	// lease whose renewTime is older than this is considered stale and may be
	// taken over by another holder.
	rerunLockLeaseDuration = 120 * time.Second
	// rerunLockRenewInterval is how often the renewal goroutine bumps renewTime
	// while the critical section (resource creation) is in progress.
	rerunLockRenewInterval = rerunLockLeaseDuration / 3

	// rerunLockNamePrefix prefixes all rerun-lock Lease names.
	rerunLockNamePrefix = "lf-rerun-"
	// maxLeaseNameLen keeps generated Lease names within the RFC1123 limit.
	maxLeaseNameLen = 63
)

// rerunLock is the handle returned by guardRerun. The caller must run all
// create steps under ctx (which is canceled if the lock is lost) and always
// call release when done, passing success=true only when the rerun resources
// were created.
//
// release always stops the renewal goroutine. On success it intentionally
// LEAVES the Lease in place to expire by its TTL: this bridges the window
// between Workflow creation and the SparkApplication becoming observable, so a
// second manual rerun arriving in that window is serialized (BusyLocked) rather
// than racing through anyTaskRunning. On failure (error / IgnoredRunning) it
// deletes the Lease immediately so a legitimate retry is not blocked.
type rerunLock struct {
	ctx     context.Context
	release func(success bool)
}

// noopLock returns a lock handle that does nothing, used when the feature is
// disabled or there are no Spark tasks to guard.
func noopLock(ctx context.Context) *rerunLock {
	return &rerunLock{ctx: ctx, release: func(bool) {}}
}

// sparkTaskNames returns the names of the Spark-executor tasks in the slice.
func sparkTaskNames(tasks []v1alpha1.Task) []string {
	names := make([]string, 0, len(tasks))
	for i := range tasks {
		if tasks[i].Executor == v1alpha1.TaskExecutorSpark {
			names = append(names, tasks[i].Name)
		}
	}
	return names
}

// rerunUniqueSuffix builds a deterministic naming suffix from the rerun token.
// When the token is empty (defensive; the controller normally stamps it) it
// falls back to a time-based value so names never collide unexpectedly.
func rerunUniqueSuffix(operationType, token string) string {
	t := token
	if t == "" {
		t = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if len(t) > 12 {
		t = t[:12]
	}
	return fmt.Sprintf("%s-%s", operationType, strings.ToLower(t))
}

// rerunWorkflowName builds a deterministic, RFC1123-safe Workflow name from the
// LakeFlow name and the naming suffix.
func rerunWorkflowName(lwName, suffix string) string {
	base := lwName
	if len(base) > 40 {
		base = base[:40]
	}
	base = strings.TrimRight(base, "-")
	return fmt.Sprintf("%s-%s", base, suffix)
}

// leaseName builds a deterministic, RFC1123-safe Lease name from the workflow
// and task. The namespace is implicit (the Lease lives in the LakeFlow
// namespace). An 8-char hash guarantees uniqueness even when the human-readable
// prefix is truncated.
func leaseName(workflow, task string) string {
	sum := sha256.Sum256([]byte(workflow + "/" + task))
	hash := hex.EncodeToString(sum[:])[:8]
	readable := sanitizeDNS(workflow + "-" + task)
	// Reserve room for prefix, hash, and the two separators.
	budget := maxLeaseNameLen - len(rerunLockNamePrefix) - len(hash) - 1
	if budget < 0 {
		budget = 0
	}
	if len(readable) > budget {
		readable = readable[:budget]
	}
	readable = strings.Trim(readable, "-")
	if readable == "" {
		return fmt.Sprintf("%s%s", rerunLockNamePrefix, hash)
	}
	return fmt.Sprintf("%s%s-%s", rerunLockNamePrefix, readable, hash)
}

// sanitizeDNS lowercases and replaces characters that are invalid in a RFC1123
// label with '-'.
func sanitizeDNS(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	return b.String()
}

// guardRerun acquires the per-task rerun lock for the given Spark tasks and
// verifies that none of them already has a running SparkApplication instance.
//
// Returns:
//   - proceed: true when the caller may create the rerun workflow.
//   - lock: a handle whose ctx is canceled if the lock is lost; release must
//     always be called (it is safe even on the !proceed path).
//   - outcome/reason: the RerunResult details when proceed is false.
//   - err: a real error (e.g. List/RBAC failure). Never returns IgnoredRunning
//     based on a failed observation.
func (m *workflowRerunManager) guardRerun(ctx context.Context, lw *v1alpha1.LakeFlow, taskNames []string) (proceed bool, lock *rerunLock, outcome RerunOutcome, reason string, err error) {
	// Feature disabled or nothing to guard: proceed with a no-op lock and no
	// client calls. The caller can still use lock.ctx uniformly.
	if !m.taskLockEnabled || len(taskNames) == 0 {
		return true, noopLock(ctx), RerunCreated, "", nil
	}

	holderID := holderIdentity()
	taskSet := make(map[string]struct{}, len(taskNames))
	for _, t := range taskNames {
		taskSet[t] = struct{}{}
	}

	acquired := make([]*coordinationv1.Lease, 0, len(taskNames))
	releaseAcquired := func() {
		for _, lease := range acquired {
			m.releaseLease(context.Background(), lease, holderID)
		}
	}

	for _, task := range taskNames {
		lease, ok, acqErr := m.acquireLease(ctx, lw, task, holderID)
		if acqErr != nil {
			releaseAcquired()
			return false, noopLock(ctx), RerunUnknown, "", acqErr
		}
		if !ok {
			releaseAcquired()
			msg := fmt.Sprintf("another rerun is in progress for task %q", task)
			return false, noopLock(ctx), RerunBusyLocked, msg, nil
		}
		acquired = append(acquired, lease)
	}

	// Verify, via a strongly-consistent (uncached) read, that no targeted Spark
	// task is currently running. A List error is propagated as an error so we
	// never drop a rerun based on a failed or stale observation.
	running, runningTask, runErr := m.anyTaskRunning(ctx, lw, taskSet)
	if runErr != nil {
		releaseAcquired()
		return false, noopLock(ctx), RerunUnknown, "", fmt.Errorf("failed to check running Spark tasks: %w", runErr)
	}
	if running {
		releaseAcquired()
		msg := fmt.Sprintf("spark task %q already running", runningTask)
		return false, noopLock(ctx), RerunIgnoredRunning, msg, nil
	}

	// Start renewing the leases for the duration of the create. If renewal fails
	// or the lease is taken over, lockCancel fires so the caller aborts.
	lockCtx, lockCancel := context.WithCancel(ctx)
	stopRenew := make(chan struct{})
	go m.renewLeases(lockCtx, lockCancel, stopRenew, acquired, holderID)

	released := false
	release := func(success bool) {
		if released {
			return
		}
		released = true
		// Stop renewing regardless of outcome.
		close(stopRenew)
		lockCancel()
		if success {
			// Leave the Lease to expire by its TTL so it keeps guarding the task
			// until the freshly created SparkApplication is observable. The
			// deterministic lease name means a later rerun reuses (takes over)
			// this same Lease after expiry — no accumulation.
			return
		}
		for _, lease := range acquired {
			m.releaseLease(context.Background(), lease, holderID)
		}
	}

	return true, &rerunLock{ctx: lockCtx, release: release}, RerunCreated, "", nil
}

// acquireLease attempts to acquire (or take over a stale) Lease for one task.
// Returns the held Lease (with up-to-date UID/ResourceVersion), whether it was
// acquired, and any hard error.
func (m *workflowRerunManager) acquireLease(ctx context.Context, lw *v1alpha1.LakeFlow, task, holderID string) (*coordinationv1.Lease, bool, error) {
	name := leaseName(lw.Name, task)
	key := client.ObjectKey{Namespace: lw.Namespace, Name: name}

	existing := &coordinationv1.Lease{}
	getErr := m.lockClient().Get(ctx, key, existing)
	if apierrors.IsNotFound(getErr) {
		fresh := m.newLease(lw, name, task, holderID)
		if createErr := m.lockClient().Create(ctx, fresh); createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				// Lost the create race; re-read and evaluate below.
				if reErr := m.lockClient().Get(ctx, key, existing); reErr != nil {
					return nil, false, reErr
				}
				return m.evaluateExisting(ctx, existing, holderID)
			}
			return nil, false, createErr
		}
		return fresh, true, nil
	}
	if getErr != nil {
		return nil, false, getErr
	}
	return m.evaluateExisting(ctx, existing, holderID)
}

// evaluateExisting decides whether an already-present Lease can be acquired:
// a still-valid lease held by someone else cannot; a stale lease is taken over.
func (m *workflowRerunManager) evaluateExisting(ctx context.Context, lease *coordinationv1.Lease, holderID string) (*coordinationv1.Lease, bool, error) {
	heldBy := ""
	if lease.Spec.HolderIdentity != nil {
		heldBy = *lease.Spec.HolderIdentity
	}
	if isLeaseValid(lease, time.Now()) && heldBy != holderID {
		return nil, false, nil
	}

	// Stale (or already ours): take it over with optimistic concurrency.
	now := metav1.NewMicroTime(time.Now())
	duration := int32(rerunLockLeaseDuration.Seconds())
	lease.Spec.HolderIdentity = &holderID
	lease.Spec.LeaseDurationSeconds = &duration
	lease.Spec.AcquireTime = &now
	lease.Spec.RenewTime = &now
	if updErr := m.lockClient().Update(ctx, lease); updErr != nil {
		if apierrors.IsConflict(updErr) || apierrors.IsAlreadyExists(updErr) {
			// Someone else won the takeover race.
			return nil, false, nil
		}
		return nil, false, updErr
	}
	return lease, true, nil
}

// renewLeases periodically bumps renewTime on the held leases. On any renewal
// failure or holder-identity mismatch it cancels the lock context so the caller
// aborts the create instead of acting without the lock.
func (m *workflowRerunManager) renewLeases(ctx context.Context, cancel context.CancelFunc, stop <-chan struct{}, leases []*coordinationv1.Lease, holderID string) {
	ticker := time.NewTicker(rerunLockRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, lease := range leases {
				if !m.renewOne(ctx, lease, holderID) {
					logger.Info("Lost rerun lock during renewal, canceling create",
						"lease", lease.Name)
					cancel()
					return
				}
			}
		}
	}
}

// renewOne refreshes a single lease's renewTime. It returns false (lock lost)
// when the lease no longer exists, is held by someone else, or cannot be
// updated.
func (m *workflowRerunManager) renewOne(ctx context.Context, lease *coordinationv1.Lease, holderID string) bool {
	key := client.ObjectKey{Namespace: lease.Namespace, Name: lease.Name}
	current := &coordinationv1.Lease{}
	if err := m.lockClient().Get(ctx, key, current); err != nil {
		return false
	}
	if current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != holderID {
		return false
	}
	now := metav1.NewMicroTime(time.Now())
	current.Spec.RenewTime = &now
	if err := m.lockClient().Update(ctx, current); err != nil {
		return false
	}
	// Keep the cached object's identity (UID/ResourceVersion) current for release.
	lease.ResourceVersion = current.ResourceVersion
	lease.UID = current.UID
	return true
}

// releaseLease deletes a lease only if it is still held by holderID, using
// UID + ResourceVersion preconditions so we never delete a lease another holder
// has taken over.
func (m *workflowRerunManager) releaseLease(ctx context.Context, lease *coordinationv1.Lease, holderID string) {
	key := client.ObjectKey{Namespace: lease.Namespace, Name: lease.Name}
	current := &coordinationv1.Lease{}
	if err := m.lockClient().Get(ctx, key, current); err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to get lease for release", "lease", lease.Name)
		}
		return
	}
	if current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != holderID {
		// Taken over by someone else; do not touch it.
		return
	}
	uid := current.UID
	rv := current.ResourceVersion
	delOpts := &client.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
	}
	if err := m.lockClient().Delete(ctx, current, delOpts); err != nil {
		if !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			logger.Error(err, "Failed to release lease", "lease", lease.Name)
		}
	}
}

// newLease builds a fresh Lease for a task, owner-referenced to the LakeFlow so
// it is garbage-collected when the LakeFlow is deleted (when a UID is known).
func (m *workflowRerunManager) newLease(lw *v1alpha1.LakeFlow, name, task, holderID string) *coordinationv1.Lease {
	now := metav1.NewMicroTime(time.Now())
	duration := int32(rerunLockLeaseDuration.Seconds())
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: lw.Namespace,
			Labels: map[string]string{
				v1alpha1.RerunLockLabel:        "true",
				v1alpha1.WorkflowNameLabel:     lw.Name,
				v1alpha1.WorkflowTaskNameLabel: task,
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holderID,
			LeaseDurationSeconds: &duration,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}
	if lw.UID != "" {
		controller := true
		blockOwnerDeletion := true
		lease.OwnerReferences = []metav1.OwnerReference{{
			APIVersion:         v1alpha1.GroupVersion.String(),
			Kind:               "LakeFlow",
			Name:               lw.Name,
			UID:                lw.UID,
			Controller:         &controller,
			BlockOwnerDeletion: &blockOwnerDeletion,
		}}
	}
	return lease
}

// anyTaskRunning reports whether any of the given Spark tasks currently has a
// non-terminal SparkApplication. It reads through the direct (uncached) client
// and returns an error on List failure so a terminal IgnoredRunning drop is
// never made from stale or missing data.
func (m *workflowRerunManager) anyTaskRunning(ctx context.Context, lw *v1alpha1.LakeFlow, taskSet map[string]struct{}) (bool, string, error) {
	sparkAppList := &sparkv1beta2.SparkApplicationList{}
	listOpts := []client.ListOption{
		client.InNamespace(lw.Namespace),
		client.MatchingLabels{v1alpha1.WorkflowNameLabel: lw.Name},
	}
	if err := m.lockClient().List(ctx, sparkAppList, listOpts...); err != nil {
		return false, "", err
	}
	for i := range sparkAppList.Items {
		app := &sparkAppList.Items[i]
		task := app.GetLabels()[v1alpha1.WorkflowTaskNameLabel]
		if _, ok := taskSet[task]; !ok {
			continue
		}
		state := app.Status.AppState.State
		if state == sparkv1beta2.ApplicationStateCompleted || state == sparkv1beta2.ApplicationStateFailed {
			continue
		}
		return true, task, nil
	}
	return false, "", nil
}

// isLeaseValid reports whether the lease is still within its validity window.
func isLeaseValid(lease *coordinationv1.Lease, now time.Time) bool {
	if lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil {
		return false
	}
	expiry := lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	return now.Before(expiry)
}

// holderIdentity returns a per-call lock holder identity (hostname + uuid).
func holderIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "/" + shortuuid.New()
}
