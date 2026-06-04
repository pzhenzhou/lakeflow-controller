package metrics

import (
	"fmt"
	"sync"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"k8s.io/apimachinery/pkg/api/resource"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// TaskMetricsRetentionPeriod is the time to keep task state in memory after completion
	// to prevent memory leaks in long-running workflows while allowing for late observations.
	TaskMetricsRetentionPeriod = 10 * time.Minute

	// CleanupTickerInterval is the interval for the background cleanup routine
	CleanupTickerInterval = 1 * time.Minute
)

// TriggerType represents the type of workflow trigger
type TriggerType string

const (
	TriggerTypeScheduled  TriggerType = "scheduled"
	TriggerTypeDependency TriggerType = "dependency"
	TriggerTypeImmediate  TriggerType = "immediate"
	TriggerTypeResubmit   TriggerType = "resubmit"
	TriggerTypeRetry      TriggerType = "retry"
)

// triggerTypes for cleanup
var triggerTypes = []TriggerType{
	TriggerTypeScheduled,
	TriggerTypeDependency,
	TriggerTypeImmediate,
	TriggerTypeResubmit,
	TriggerTypeRetry,
}

var (
	metricsOnce = sync.Once{}
	manager     *LakeFlowMetricsManager
	logger      = common.GetSharedLogger().WithName("metrics")

	//  workflowTransitionTime State Transition Timestamp
	//	Reflects success/failure by the time of state transition
	workflowTransitionTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lake_workflow_transition_timestamp_seconds",
			Help: "Timestamp of the last state transition for the LakeFlow",
		},
		[]string{"namespace", "workflow", "phase", "trigger_type"},
	)

	workflowDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "lake_workflow_duration_seconds",
			Help: "Runtime of a lakeflow in seconds, from start to completion",
			// Starts at 10s, doubles 14 times (covers up to ~45 hours)
			Buckets: prometheus.ExponentialBuckets(10, 2, 14),
		},
		[]string{"namespace", "workflow", "phase", "trigger_type"},
	)

	// taskDuration Task Runtime (Duration)
	// Histogram for the runtime distribution
	taskDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lake_workflow_task_duration_seconds",
			Help:    "Runtime of a lakeflow task in seconds, from start to completion",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15), // Starts at 1s, doubles 15 times (covers up to ~9 hours)
		},
		[]string{"namespace", "workflow_name", "task_name", "phase"},
	)

	// 3. Task Ready (Start) Timestamp
	taskReadyTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lake_workflow_task_ready_timestamp_seconds",
			Help: "Timestamp when the task became ready (started)",
		},
		[]string{"namespace", "workflow_name", "task_name"},
	)

	// TaskCompleteTime. Task Completion Timestamp
	taskCompletionTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lake_workflow_task_completion_timestamp_seconds",
			Help: "Timestamp when the task completed",
		},
		[]string{"namespace", "workflow_name", "task_name", "phase"},
	)

	// Task retry counter
	taskRetryCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lake_workflow_task_retry_total",
			Help: "Total number of task retry attempts",
		},
		[]string{"namespace", "workflow_name", "task_name"},
	)

	// Workflow retry counter
	workflowRetryCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lake_workflow_retry_total",
			Help: "Total number of workflow retry attempts",
		},
		[]string{"namespace", "workflow", "trigger_type"},
	)

	// Task Resource Requests
	taskResourceRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lake_workflow_task_resource_requests",
			Help: "Resource requests for the task (cpu in cores, memory in MB)",
		},
		[]string{"namespace", "workflow_name", "task_name", "queue", "resource"},
	)

	// Task Resource Limits
	taskResourceLimits = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lake_workflow_task_resource_limits",
			Help: "Resource limits for the task (cpu in cores, memory in MB)",
		},
		[]string{"namespace", "workflow_name", "task_name", "queue", "resource"},
	)

	argoWfNodePhase = []argowfv1.NodePhase{
		argowfv1.NodePending,
		argowfv1.NodeRunning,
		argowfv1.NodeSucceeded,
		argowfv1.NodeFailed,
		argowfv1.NodeError,
		argowfv1.NodeSkipped,
	}

	lakeWorkflowPhase = []v1alpha1.WorkflowPhase{
		v1alpha1.WorkflowPhasePending,
		v1alpha1.WorkflowPhaseIdle,
		v1alpha1.WorkflowPhaseRunning,
		v1alpha1.WorkflowPhaseSucceeded,
		v1alpha1.WorkflowPhaseFailed,
		v1alpha1.WorkflowPhaseCompleted,
	}
)

type TaskLifecycleState struct {
	StartTime  time.Time
	EndTime    time.Time
	Phase      argowfv1.NodePhase
	RetryCount int32
	QueueName  string
}

type LakeFlowMetricsManager struct {
	// key is namespace + workflow_name
	workflowStateTrack *xsync.Map[string, v1alpha1.WorkflowPhase]
	// task lifecycle tracking
	// key is namespace + workflow_name + task_name
	workflowTaskTrack *xsync.Map[string, TaskLifecycleState]
	// Background cleanup
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

func GetMetricsManager() *LakeFlowMetricsManager {
	metricsOnce.Do(func() {
		manager = &LakeFlowMetricsManager{
			workflowStateTrack: xsync.NewMap[string, v1alpha1.WorkflowPhase](),
			workflowTaskTrack:  xsync.NewMap[string, TaskLifecycleState](),
			stopCleanup:        make(chan struct{}),
		}
		// Start background cleanup routine
		manager.startCleanupRoutine()
	})
	return manager
}

func buildTaskKey(lw *v1alpha1.LakeFlow, taskName string) string {
	return fmt.Sprintf("%s/%s/%s", lw.Namespace, lw.Name, taskName)
}

func buildWorkflowKey(lw *v1alpha1.LakeFlow) string {
	return fmt.Sprintf("%s/%s", lw.Namespace, lw.Name)
}

// determineTriggerType determines the trigger type from LakeFlow spec
func determineTriggerType(lw *v1alpha1.LakeFlow) TriggerType {
	if lw == nil {
		return TriggerTypeImmediate
	}

	// Check if this is a scheduled workflow (has cron expression)
	if lw.Spec.WorkflowTrigger.Schedule.Cron != "" {
		return TriggerTypeScheduled
	}

	// Check if this is a dependency-triggered workflow
	if len(lw.Spec.WorkflowTrigger.Depend.Workflows) > 0 {
		return TriggerTypeDependency
	}

	// Default to immediate execution
	return TriggerTypeImmediate
}

// determineTriggerTypeWithOverride determines trigger type, allowing override for resubmit/retry
func determineTriggerTypeWithOverride(lw *v1alpha1.LakeFlow, isResubmit, isRetry bool) TriggerType {
	if isResubmit {
		return TriggerTypeResubmit
	}
	if isRetry {
		return TriggerTypeRetry
	}
	return determineTriggerType(lw)
}

// ObserveWorkflowState updates the workflow transition timestamp metric
// This is a convenience wrapper that auto-detects trigger type from the LakeFlow spec
func (m *LakeFlowMetricsManager) ObserveWorkflowState(lw *v1alpha1.LakeFlow, eventTime time.Time) {
	m.ObserveWorkflowStateWithTrigger(lw, eventTime, false, false)
}

// ObserveWorkflowStateWithTrigger updates the workflow transition timestamp metric with trigger type override
func (m *LakeFlowMetricsManager) ObserveWorkflowStateWithTrigger(lw *v1alpha1.LakeFlow, eventTime time.Time, isResubmit, isRetry bool) {
	// Safety check
	if lw == nil || lw.Status.Phase == "" {
		logger.Info("ObserveWorkflowState: skipping due to safety check", "lw_nil", lw == nil, "phase_empty", lw == nil || lw.Status.Phase == "")
		return
	}

	workflowKey := buildWorkflowKey(lw)
	triggerType := determineTriggerTypeWithOverride(lw, isResubmit, isRetry)
	logger.Info("ObserveWorkflowState called", "workflow", lw.Name, "namespace", lw.Namespace, "phase", lw.Status.Phase, "triggerType", triggerType)

	// Get previous phase from tracking map
	previousPhase, hadPreviousPhase := m.workflowStateTrack.Load(workflowKey)
	currentPhase := lw.Status.Phase

	// Only record metrics if phase changed or this is the first observation
	if !hadPreviousPhase || previousPhase != currentPhase {
		logger.Info("Recording workflow state metrics", "workflow", lw.Name, "hadPreviousPhase", hadPreviousPhase, "previousPhase", previousPhase, "currentPhase", currentPhase, "triggerType", triggerType)
		// Record state transition timestamp
		// If eventTime is provided (non-zero), use it; otherwise use current time
		var timestamp float64
		if !eventTime.IsZero() {
			timestamp = float64(eventTime.Unix())
		} else {
			timestamp = float64(time.Now().Unix())
		}

		workflowTransitionTime.WithLabelValues(
			lw.Namespace,
			lw.Name,
			string(currentPhase),
			string(triggerType),
		).Set(timestamp)

		// Update tracking map with new phase
		m.workflowStateTrack.Store(workflowKey, currentPhase)
		logger.Info("Workflow state metric recorded", "workflow", lw.Name, "phase", currentPhase, "timestamp", timestamp, "triggerType", triggerType)

		// Record workflow duration if workflow completed
		if currentPhase == v1alpha1.WorkflowPhaseSucceeded || currentPhase == v1alpha1.WorkflowPhaseFailed {
			logger.Info("Recording workflow duration metric", "workflow", lw.Name, "phase", currentPhase, "triggerType", triggerType)
			// Calculate duration from creation to completion
			if lw.Status.FinishedTime != nil && !lw.Status.FinishedTime.FinishedAt.IsZero() {
				duration := lw.Status.FinishedTime.FinishedAt.Sub(lw.CreationTimestamp.Time).Seconds()
				workflowDuration.WithLabelValues(
					lw.Namespace,
					lw.Name,
					string(currentPhase),
					string(triggerType),
				).Observe(duration)
				logger.Info("Workflow duration metric recorded", "workflow", lw.Name, "duration", duration, "triggerType", triggerType)
			}
		}
	} else {
		logger.V(1).Info("ObserveWorkflowState: no change", "workflow", lw.Name, "phase", currentPhase)
	}
}

// ObserveWorkflowRetry increments the workflow retry counter
func (m *LakeFlowMetricsManager) ObserveWorkflowRetry(lw *v1alpha1.LakeFlow) {
	if lw == nil {
		return
	}
	triggerType := determineTriggerType(lw)
	workflowRetryCount.WithLabelValues(lw.Namespace, lw.Name, string(triggerType)).Inc()
	logger.Info("Workflow retry count incremented", "workflow", lw.Name, "namespace", lw.Namespace, "triggerType", triggerType)
}

// ObserveTaskExecution tracks task start/end times in memory and updates metrics
func (m *LakeFlowMetricsManager) ObserveTaskExecution(lw *v1alpha1.LakeFlow, argoWf *argowfv1.Workflow) {
	// Safety checks
	if lw == nil {
		logger.V(1).Info("ObserveTaskExecution: lw is nil")
		return
	}
	if argoWf == nil || argoWf.Status.Nodes == nil {
		logger.Info("ObserveTaskExecution: skipping", "workflow", lw.Name, "argoWf_nil", argoWf == nil, "nodes_nil", argoWf == nil || argoWf.Status.Nodes == nil)
		return
	}

	logger.Info("ObserveTaskExecution called", "workflow", lw.Name, "argoWorkflow", argoWf.Name, "nodeCount", len(argoWf.Status.Nodes))

	// Build a task-name -> spec index once so per-node resource recording is O(1).
	// Previously recordTaskResources re-scanned lw.Spec.Tasks for every Argo node,
	// making this O(nodes x tasks).
	taskIndex := make(map[string]*v1alpha1.TaskSpec, len(lw.Spec.Tasks))
	for i := range lw.Spec.Tasks {
		taskIndex[lw.Spec.Tasks[i].Name] = &lw.Spec.Tasks[i].TaskSpec
	}

	// Iterate through all nodes in the Argo Workflow
	for nodeID, node := range argoWf.Status.Nodes {
		// Only process task nodes (Pod type nodes represent actual tasks)
		if node.Type != argowfv1.NodeTypePod {
			continue
		}

		// Skip tasks that finished too long ago to prevent re-observation after cleanup
		// We use the exact retention period to ensure we don't process tasks that have been cleaned up
		if !node.FinishedAt.IsZero() {
			if time.Since(node.FinishedAt.Time) > TaskMetricsRetentionPeriod {
				continue
			}
		}

		// Extract task name from node
		// Priority: TemplateName (matches Task.Name) > DisplayName > Name
		taskName := node.TemplateName
		if taskName == "" {
			taskName = node.DisplayName
		}
		if taskName == "" {
			taskName = node.Name
		}
		taskKey := buildTaskKey(lw, taskName)

		// Always update resource metrics to reflect current spec (handles retries/updates)
		// We do this before tracking logic to ensure even transient states have resources recorded
		queueName := m.recordTaskResources(lw, taskName, taskIndex)

		// Get existing task state from tracking map
		existingState, exists := m.workflowTaskTrack.Load(taskKey)

		// Case 1: Task not yet tracked (taskKey doesn't exist - no start time)
		if !exists {
			// Task must have started to be tracked
			if node.StartedAt.IsZero() {
				continue
			}
			// Record task ready timestamp (first time we see it started)
			taskReadyTime.WithLabelValues(
				lw.Namespace,
				lw.Name,
				taskName,
			).Set(float64(node.StartedAt.Unix()))

			// Create new tracking state with start time
			newState := TaskLifecycleState{
				StartTime:  node.StartedAt.Time,
				Phase:      node.Phase,
				RetryCount: 0,
				QueueName:  queueName,
				// EndTime remains zero until task completes
			}
			m.workflowTaskTrack.Store(taskKey, newState)

			// If task already finished (rare but possible in fast tasks)
			if !node.FinishedAt.IsZero() {
				// Record completion immediately
				taskCompletionTime.WithLabelValues(
					lw.Namespace,
					lw.Name,
					taskName,
					string(node.Phase),
				).Set(float64(node.FinishedAt.Unix()))

				// Calculate and record task duration
				duration := node.FinishedAt.Sub(node.StartedAt.Time).Seconds()
				taskDuration.WithLabelValues(
					lw.Namespace,
					lw.Name,
					taskName,
					string(node.Phase),
				).Observe(duration)

				// Update state with end time
				newState.EndTime = node.FinishedAt.Time
				m.workflowTaskTrack.Store(taskKey, newState)
			}
			continue
		}

		// Case 2: Task exists in tracking (has start time, may or may not have end time)
		// existingState.StartTime is guaranteed to be non-zero here

		// Check if task completed or retried
		if !node.FinishedAt.IsZero() {
			if existingState.EndTime.IsZero() {
				// First completion - record it
				taskCompletionTime.WithLabelValues(
					lw.Namespace,
					lw.Name,
					taskName,
					string(node.Phase),
				).Set(float64(node.FinishedAt.Unix()))

				// Calculate and record task duration
				duration := node.FinishedAt.Sub(existingState.StartTime).Seconds()
				taskDuration.WithLabelValues(
					lw.Namespace,
					lw.Name,
					taskName,
					string(node.Phase),
				).Observe(duration)

				// Update tracking state with completion
				completedState := TaskLifecycleState{
					StartTime:  existingState.StartTime,
					EndTime:    node.FinishedAt.Time,
					Phase:      node.Phase,
					RetryCount: existingState.RetryCount,
					QueueName:  queueName,
				}
				m.workflowTaskTrack.Store(taskKey, completedState)
			} else if !node.FinishedAt.Time.Equal(existingState.EndTime) {
				// Task was retried - FinishedAt changed
				// Update retry counter
				taskRetryCount.WithLabelValues(
					lw.Namespace,
					lw.Name,
					taskName,
				).Inc()

				// Record new completion time
				taskCompletionTime.WithLabelValues(
					lw.Namespace,
					lw.Name,
					taskName,
					string(node.Phase),
				).Set(float64(node.FinishedAt.Unix()))

				// Calculate and record new task duration
				duration := node.FinishedAt.Sub(existingState.StartTime).Seconds()
				taskDuration.WithLabelValues(
					lw.Namespace,
					lw.Name,
					taskName,
					string(node.Phase),
				).Observe(duration)

				// Update tracking state with new completion
				retriedState := TaskLifecycleState{
					StartTime:  existingState.StartTime,
					EndTime:    node.FinishedAt.Time,
					Phase:      node.Phase,
					RetryCount: existingState.RetryCount + 1,
					QueueName:  queueName,
				}
				m.workflowTaskTrack.Store(taskKey, retriedState)
			}
			// If task already has EndTime and it hasn't changed, it's already fully recorded - no action needed
		} else if existingState.EndTime.IsZero() {
			// Task still running - update phase if changed
			if existingState.Phase != node.Phase {
				updatedState := existingState
				updatedState.Phase = node.Phase
				m.workflowTaskTrack.Store(taskKey, updatedState)
			}
		}

		// Store node ID for reference (helps with debugging)
		_ = nodeID
	}
}

// recordTaskResources extracts and records resource metrics for a task
// Returns the queue name for state tracking. taskIndex is the per-call task-name ->
// spec index built by the caller so the lookup is O(1).
func (m *LakeFlowMetricsManager) recordTaskResources(lw *v1alpha1.LakeFlow, taskName string, taskIndex map[string]*v1alpha1.TaskSpec) string {
	taskSpec := taskIndex[taskName]
	if taskSpec == nil {
		logger.V(1).Info("recordTaskResources: task spec not found", "taskName", taskName, "workflow", lw.Name)
		return ""
	}

	queueName := taskSpec.QueueName
	var cpuReq, memReq, cpuLim, memLim float64

	if taskSpec.SparkExecutorSpec != nil {
		// Spark: Aggregate Driver + Executor resources
		// Note: This assumes 1 executor instance for simplicity as Replicas can be dynamic/missing
		// Ideally we multiply by Replicas if specified, but SparkExecutorSpec.ExecutorResource.Replicas
		// is often just initial instances.
		driver := taskSpec.SparkExecutorSpec.DriverResource.Resources
		executor := taskSpec.SparkExecutorSpec.ExecutorResource.Resources
		replicas := float64(taskSpec.SparkExecutorSpec.ExecutorResource.Replicas)

		// Requests
		if q := driver.Requests.Cpu(); q != nil {
			cpuReq += parseQty(*q)
		}
		if q := driver.Requests.Memory(); q != nil {
			memReq += parseMem(*q)
		}
		if q := executor.Requests.Cpu(); q != nil {
			cpuReq += parseQty(*q) * replicas
		}
		if q := executor.Requests.Memory(); q != nil {
			memReq += parseMem(*q) * replicas
		}

		// Limits
		if q := driver.Limits.Cpu(); q != nil {
			cpuLim += parseQty(*q)
		}
		if q := driver.Limits.Memory(); q != nil {
			memLim += parseMem(*q)
		}
		if q := executor.Limits.Cpu(); q != nil {
			cpuLim += parseQty(*q) * replicas
		}
		if q := executor.Limits.Memory(); q != nil {
			memLim += parseMem(*q) * replicas
		}

		// Override queue name if present in Spark spec (though TaskSpec has it too)
		if taskSpec.SparkExecutorSpec.QueueName != "" {
			queueName = taskSpec.SparkExecutorSpec.QueueName
		}

	} else if taskSpec.CommandExecutorSpec != nil {
		if taskSpec.CommandExecutorSpec.Resources != nil {
			res := taskSpec.CommandExecutorSpec.Resources
			// Requests
			if q := res.Requests.Cpu(); q != nil {
				cpuReq = parseQty(*q)
			}
			if q := res.Requests.Memory(); q != nil {
				memReq = parseMem(*q)
			}
			// Limits
			if q := res.Limits.Cpu(); q != nil {
				cpuLim = parseQty(*q)
			}
			if q := res.Limits.Memory(); q != nil {
				memLim = parseMem(*q)
			}
		}
	}

	// Record Metrics
	taskResourceRequests.WithLabelValues(lw.Namespace, lw.Name, taskName, queueName, "cpu").Set(cpuReq)
	taskResourceRequests.WithLabelValues(lw.Namespace, lw.Name, taskName, queueName, "memory").Set(memReq)
	taskResourceLimits.WithLabelValues(lw.Namespace, lw.Name, taskName, queueName, "cpu").Set(cpuLim)
	taskResourceLimits.WithLabelValues(lw.Namespace, lw.Name, taskName, queueName, "memory").Set(memLim)

	return queueName
}

// Helper to parse quantity to cores
func parseQty(q resource.Quantity) float64 {
	return float64(q.MilliValue()) / 1000.0 // Convert to cores
}

// Helper to parse quantity to MB
func parseMem(q resource.Quantity) float64 {
	return float64(q.Value()) / (1024 * 1024) // Convert Bytes to MB
}

// startCleanupRoutine starts a background goroutine for periodic cleanup
func (m *LakeFlowMetricsManager) startCleanupRoutine() {
	m.cleanupTicker = time.NewTicker(CleanupTickerInterval)

	go func() {
		for {
			select {
			case <-m.cleanupTicker.C:
				m.cleanupExpiredTasks()
			case <-m.stopCleanup:
				m.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// cleanupExpiredTasks removes task states that have been completed for longer than retention period
func (m *LakeFlowMetricsManager) cleanupExpiredTasks() {
	now := time.Now()
	m.workflowTaskTrack.Range(func(key string, state TaskLifecycleState) bool {
		if !state.EndTime.IsZero() && now.Sub(state.EndTime) > TaskMetricsRetentionPeriod {
			m.workflowTaskTrack.Delete(key)
		}
		return true
	})
}

// ResetMetrics cleans up in-memory state and deletes Prometheus metrics
func (m *LakeFlowMetricsManager) ResetMetrics(lw *v1alpha1.LakeFlow) {
	workflowKey := buildWorkflowKey(lw)
	taskKeyPrefix := workflowKey + "/"

	// Remove workflow state from tracking
	m.workflowStateTrack.Delete(workflowKey)

	// Delete workflow-level Prometheus metrics
	// Note: We delete all phase and trigger_type combinations for this workflow
	for _, phase := range lakeWorkflowPhase {
		for _, triggerType := range triggerTypes {
			workflowTransitionTime.DeleteLabelValues(lw.Namespace, lw.Name, string(phase), string(triggerType))
			workflowDuration.DeleteLabelValues(lw.Namespace, lw.Name, string(phase), string(triggerType))
		}
	}
	for _, triggerType := range triggerTypes {
		workflowRetryCount.DeleteLabelValues(lw.Namespace, lw.Name, string(triggerType))
	}

	// Collect all task keys for this workflow
	tasksToDelete := make([]string, 0)
	m.workflowTaskTrack.Range(func(key string, value TaskLifecycleState) bool {
		if len(key) > len(taskKeyPrefix) && key[:len(taskKeyPrefix)] == taskKeyPrefix {
			tasksToDelete = append(tasksToDelete, key)
		}
		return true
	})

	// Delete task-level metrics and tracking state
	for _, taskKey := range tasksToDelete {
		// Extract task name from key (format: namespace/workflow/taskname)
		taskName := taskKey[len(taskKeyPrefix):]

		// Retrieve state to get queue name for cleaning up resource metrics
		if state, ok := m.workflowTaskTrack.Load(taskKey); ok {
			taskResourceRequests.DeleteLabelValues(lw.Namespace, lw.Name, taskName, state.QueueName, "cpu")
			taskResourceRequests.DeleteLabelValues(lw.Namespace, lw.Name, taskName, state.QueueName, "memory")
			taskResourceLimits.DeleteLabelValues(lw.Namespace, lw.Name, taskName, state.QueueName, "cpu")
			taskResourceLimits.DeleteLabelValues(lw.Namespace, lw.Name, taskName, state.QueueName, "memory")
		}

		// Delete task metrics for all possible phases
		for _, phase := range argoWfNodePhase {
			taskDuration.DeleteLabelValues(lw.Namespace, lw.Name, taskName, string(phase))
			taskCompletionTime.DeleteLabelValues(lw.Namespace, lw.Name, taskName, string(phase))
		}

		// Delete task ready time (no phase label)
		taskReadyTime.DeleteLabelValues(lw.Namespace, lw.Name, taskName)
		taskRetryCount.DeleteLabelValues(lw.Namespace, lw.Name, taskName)

		// Remove from tracking map
		m.workflowTaskTrack.Delete(taskKey)
	}
}

func init() {
	ctrlmetrics.Registry.MustRegister(workflowTransitionTime)
	ctrlmetrics.Registry.MustRegister(workflowDuration)
	ctrlmetrics.Registry.MustRegister(taskDuration)
	ctrlmetrics.Registry.MustRegister(taskReadyTime)
	ctrlmetrics.Registry.MustRegister(taskCompletionTime)
	ctrlmetrics.Registry.MustRegister(taskRetryCount)
	ctrlmetrics.Registry.MustRegister(workflowRetryCount)
	ctrlmetrics.Registry.MustRegister(taskResourceRequests)
	ctrlmetrics.Registry.MustRegister(taskResourceLimits)
}
