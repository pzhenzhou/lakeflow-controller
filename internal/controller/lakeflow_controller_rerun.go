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
	"fmt"
	"math/rand"
	"time"

	"github.com/go-logr/logr"
	"github.com/lithammer/shortuuid/v4"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/rerun"
	"github.com/pzhenzhou/lakeflow-controller/pkg/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// ResubmitRequeueInterval is the short delay after a successful resubmit,
	// giving the freshly created workflow time to register before re-reconciling.
	ResubmitRequeueInterval = 5 * time.Second
	// RetryRequeueInterval is the short delay after a successful retry trigger.
	RetryRequeueInterval = 2 * time.Second
	// RerunBusyRequeueInterval is the base delay before retrying a rerun that was
	// blocked by a competing reconciler holding the per-task lock. It is sized to
	// the lock lease duration so the request survives until the lock frees.
	RerunBusyRequeueInterval = 120 * time.Second
	// RerunBusyRequeueJitter is the maximum random jitter added to the busy
	// requeue delay to avoid a thundering herd across reconcilers.
	RerunBusyRequeueJitter = 10 * time.Second
)

// rerunAction describes a single annotation-driven rerun strategy. The order in
// which actions are evaluated in handleRerun defines their priority, so resubmit
// (a full fresh run) is checked before retry (failed nodes only).
type rerunAction struct {
	name string
	// should reports whether the action's annotation is present, plus a reason.
	should func(*v1alpha1.LakeFlow) (bool, string)
	// run performs the action against the cluster and returns a structured outcome.
	run func(context.Context, *v1alpha1.LakeFlow, string) (rerun.RerunResult, error)
	// onSuccess runs only after a Created outcome, before requeueing. May be nil.
	onSuccess func(*v1alpha1.LakeFlow, string)
	// requeueAfter is the delay used to re-reconcile after a terminal outcome.
	requeueAfter time.Duration
}

// handleRerun evaluates each rerun strategy in priority order. For the first
// action whose annotation is present, it stamps a durable idempotency token,
// runs the action, and reacts to the structured outcome:
//   - Created/Noop/IgnoredRunning are terminal: clear the rerun annotations.
//   - BusyLocked preserves the annotations and requeues after the lock TTL so the
//     request survives a competing reconciler.
//   - An error (or the defensive Unknown outcome) preserves the annotations and
//     backs off via the rate limiter.
//
// It reports handled=true so the caller can short-circuit the rest of the
// reconcile loop, or handled=false when no rerun was requested.
func (r *LakeFlowController) handleRerun(
	ctx context.Context,
	lw *v1alpha1.LakeFlow,
	statusManager *status.WorkflowStatusManager,
	logger logr.Logger,
) (ctrl.Result, bool, error) {
	actions := []rerunAction{
		{
			name:         "resubmit",
			should:       r.RerunManager.ShouldResubmit,
			run:          r.RerunManager.ResubmitWorkflow,
			requeueAfter: ResubmitRequeueInterval,
		},
		{
			name:         "retry",
			should:       r.RerunManager.ShouldRetry,
			run:          r.RerunManager.RetryWorkflow,
			requeueAfter: RetryRequeueInterval,
			onSuccess: func(lw *v1alpha1.LakeFlow, reason string) {
				statusManager.UpdateRetryStatus(lw, reason)
				retryAttempts := int32(0)
				if lw.Status.RetryStatus != nil {
					retryAttempts = lw.Status.RetryStatus.RetryAttempts
				}
				logger.Info("Workflow retry triggered successfully",
					"lakeflow", lw.Name,
					"retryAttempts", retryAttempts)
			},
		},
	}

	for _, action := range actions {
		trigger, reason := action.should(lw)
		if !trigger {
			continue
		}

		logger.Info("Rerun annotation detected",
			"action", action.name,
			"lakeflow", lw.Name,
			"currentPhase", lw.Status.Phase,
			"reason", reason)

		// Stamp a durable, per-request idempotency token before running so rerun
		// resource names are deterministic and survive a controller restart. On
		// failure, preserve annotations and back off.
		if err := r.ensureRerunToken(ctx, lw); err != nil {
			logger.Error(err, "Failed to stamp rerun token", "action", action.name)
			return ctrl.Result{}, true, err
		}

		result, runErr := action.run(ctx, lw, reason)
		if runErr != nil {
			r.emitRerunEvent(lw, corev1.EventTypeWarning, "RerunFailed",
				fmt.Sprintf("%s failed: %v", action.name, runErr))
			logger.Error(runErr, "Failed to execute rerun", "action", action.name)
			// Preserve annotations; return the bare error so the rate limiter
			// drives the requeue (RequeueAfter is ignored for non-nil errors).
			return ctrl.Result{}, true, runErr
		}

		switch result.Outcome {
		case rerun.RerunBusyLocked:
			r.emitRerunEvent(lw, corev1.EventTypeNormal, "RerunBusy", result.Message)
			logger.Info("Rerun blocked by an in-progress rerun, will retry",
				"action", action.name, "lakeflow", lw.Name, "message", result.Message)
			// Preserve annotations so the request survives; requeue after the lock TTL.
			return ctrl.Result{RequeueAfter: rerunBusyRequeueAfter()}, true, nil

		case rerun.RerunIgnoredRunning:
			r.emitRerunEvent(lw, corev1.EventTypeWarning, "RerunIgnoredRunning", result.Message)
			statusManager.RecordRerunIgnored(lw, result.Message)
			logger.Info("Rerun ignored because a targeted task is already running",
				"action", action.name, "lakeflow", lw.Name, "message", result.Message)
			if err := r.clearRerunAnnotations(ctx, lw, action.name, logger); err != nil {
				// Preserve annotations and back off; the condition still persists
				// via the deferred status write so the user has feedback.
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{RequeueAfter: action.requeueAfter}, true, nil

		case rerun.RerunNoop:
			r.emitRerunEvent(lw, corev1.EventTypeNormal, "RerunNoop", result.Message)
			logger.Info("Rerun was a no-op", "action", action.name,
				"lakeflow", lw.Name, "message", result.Message)
			if err := r.clearRerunAnnotations(ctx, lw, action.name, logger); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{RequeueAfter: action.requeueAfter}, true, nil

		case rerun.RerunCreated:
			r.emitRerunEvent(lw, corev1.EventTypeNormal, "RerunStarted", result.Message)
			// Clear annotations BEFORE counting the retry. If clearing fails we
			// must not run onSuccess: the durable token keeps the create
			// idempotent, so the next reconcile re-observes Created and we count
			// exactly once only after the annotations (and token) are gone.
			if err := r.clearRerunAnnotations(ctx, lw, action.name, logger); err != nil {
				r.emitRerunEvent(lw, corev1.EventTypeWarning, "RerunFailed",
					fmt.Sprintf("%s created but clearing the rerun request failed: %v", action.name, err))
				return ctrl.Result{}, true, err
			}
			if action.onSuccess != nil {
				action.onSuccess(lw, reason)
			}
			logger.Info("Rerun executed successfully, requeuing",
				"action", action.name, "lakeflow", lw.Name)
			return ctrl.Result{RequeueAfter: action.requeueAfter}, true, nil

		default:
			// RerunUnknown (zero value): a code path forgot to set an outcome.
			// Treat as an error so the request is preserved and retried.
			err := fmt.Errorf("rerun %s returned an unknown outcome", action.name)
			r.emitRerunEvent(lw, corev1.EventTypeWarning, "RerunFailed", err.Error())
			logger.Error(err, "Unexpected rerun outcome", "action", action.name)
			return ctrl.Result{}, true, err
		}
	}

	return ctrl.Result{}, false, nil
}

// ensureRerunToken stamps a durable per-request idempotency token annotation on
// the LakeFlow if one is not already present, persisting it so a controller
// restart mid-flight reuses the same token. The in-memory object is updated so
// the rerun manager reads the same token.
func (r *LakeFlowController) ensureRerunToken(ctx context.Context, lw *v1alpha1.LakeFlow) error {
	if lw.GetAnnotations()[v1alpha1.RerunTokenAnnotation] != "" {
		return nil
	}

	token := shortuuid.New()
	key := client.ObjectKey{Namespace: lw.Namespace, Name: lw.Name}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1alpha1.LakeFlow{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		ann := latest.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		if existing := ann[v1alpha1.RerunTokenAnnotation]; existing != "" {
			token = existing
			return nil
		}
		ann[v1alpha1.RerunTokenAnnotation] = token
		latest.SetAnnotations(ann)
		return r.Update(ctx, latest)
	}); err != nil {
		return fmt.Errorf("failed to persist rerun token: %w", err)
	}

	ann := lw.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[v1alpha1.RerunTokenAnnotation] = token
	lw.SetAnnotations(ann)
	return nil
}

// clearRerunAnnotations clears all rerun annotations (including the token) on a
// terminal outcome. It returns the error so callers can preserve annotations and
// back off rather than silently proceeding (which would let retry accounting and
// metrics repeat once the idempotent create re-reports Created).
func (r *LakeFlowController) clearRerunAnnotations(ctx context.Context, lw *v1alpha1.LakeFlow, action string, logger logr.Logger) error {
	if clearErr := r.RerunManager.ClearRerunAnnotations(ctx, lw); clearErr != nil {
		logger.Error(clearErr, "Failed to clear rerun annotation", "action", action)
		return clearErr
	}
	return nil
}

// emitRerunEvent records a Kubernetes event for a rerun outcome when an event
// recorder is configured.
func (r *LakeFlowController) emitRerunEvent(lw *v1alpha1.LakeFlow, eventType, reason, message string) {
	if r.EventRecorder == nil {
		return
	}
	r.EventRecorder.Event(lw, eventType, reason, message)
}

// rerunBusyRequeueAfter returns the busy requeue delay with random jitter.
func rerunBusyRequeueAfter() time.Duration {
	return RerunBusyRequeueInterval + time.Duration(rand.Int63n(int64(RerunBusyRequeueJitter)+1))
}
