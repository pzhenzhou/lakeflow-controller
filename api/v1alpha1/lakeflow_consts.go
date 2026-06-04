package v1alpha1

type DependTriggerMode string
type TaskExecutor string

type WorkflowConditionType string

type WorkflowControlState string
type WorkflowPhase string

type SparkApplicationType string

const (
	SparkAppTypeJava   SparkApplicationType = "Java"
	SparkAppTypePython SparkApplicationType = "Python"
)

// Unified control state enum used in both Spec (desired) and Status (observed)
// Spec.State: User declares what control state they want
// Status.ExecutionState: System reports what control state actually is
const (
	ControlStateActive     WorkflowControlState = "Active"     // Actively running/scheduling (spec: desired, status: observed)
	ControlStateSuspend    WorkflowControlState = "Suspend"    // Paused, can be resumed (spec: desired, status: observed)
	ControlStateStopped    WorkflowControlState = "Stopped"    // Gracefully stopped, cannot resume (spec: desired, status: observed)
	ControlStateTerminated WorkflowControlState = "Terminated" // Force terminated, cannot resume (spec: desired, status: observed)
)

// System-reported resource reconciliation outcome - goes in Status
const (
	WorkflowPhasePending   WorkflowPhase = "Pending"   // Resources being coordinated, cannot execute yet
	WorkflowPhaseIdle      WorkflowPhase = "Idle"      // Resources ready, no active executions (CronWorkflow waiting/paused)
	WorkflowPhaseRunning   WorkflowPhase = "Running"   // Resources ready, actively executing tasks
	WorkflowPhaseSucceeded WorkflowPhase = "Succeeded" // Individual workflow execution completed successfully
	WorkflowPhaseFailed    WorkflowPhase = "Failed"    // Individual workflow execution failed
	WorkflowPhaseCompleted WorkflowPhase = "Completed" // Scheduled workflow has finished its scheduling lifecycle

	DependTriggerModeAll DependTriggerMode = "all"

	TaskExecutorSpark  TaskExecutor = "spark"
	TaskExecutorBash   TaskExecutor = "bash"
	TaskExecutorPython TaskExecutor = "python"

	ConditionTypeTriggerEvaluated      WorkflowConditionType = "TriggerEvaluated"
	ConditionTypeDependenciesEvaluated WorkflowConditionType = "DependenciesEvaluated"
	ConditionTypeConverted             WorkflowConditionType = "Converted"
	ConditionTypeArgoResourceCreated   WorkflowConditionType = "ArgoResourceCreated"
	ConditionTypeParallelismEvaluated  WorkflowConditionType = "ParallelismEvaluated"
	ConditionTypePolicyActive          WorkflowConditionType = "ParallelismPolicyActive"
	ConditionTypeUsageUpdated          WorkflowConditionType = "UsageUpdated"
)

const (
	WorkflowGroupName                  = "lakeflow.io"
	LakeFlowControllerName             = "lake-workflow-controller"
	WorkflowNameLabel                  = WorkflowGroupName + "/name"
	WorkflowBizLabel                   = WorkflowGroupName + "/biz"
	WorkflowTaskNameLabel              = WorkflowGroupName + "/task"
	WorkflowNameSpaceLabel             = WorkflowGroupName + "/namespace"
	WorkflowTenantLabel                = WorkflowGroupName + "/tenant"
	WorkflowDeploymentOriginalReplicas = WorkflowGroupName + "/original-replicas"
	WorkflowDependNamespaceLabel       = WorkflowGroupName + "/depend-workflow-namespace"
	WorkflowDependNameLabel            = WorkflowGroupName + "/depend-workflow-name"
	SparkLocalDirPVCLabel              = WorkflowGroupName + "/spark-pvc"
	SparkLocalDirPVCSizeLabel          = WorkflowGroupName + "/spark-pvc-size"
	// SharedObjectStoragePVCLabel marks operator-managed, namespace-shared OSS
	// StorageClasses/PVCs that are content-addressed from the storage identity.
	SharedObjectStoragePVCLabel = WorkflowGroupName + "/shared-oss-pvc"

	IsRetryWorkflowLabel    = WorkflowGroupName + "/is-retry"
	IsResubmitWorkflowLabel = WorkflowGroupName + "/is-resubmit"
	IsBackfillWorkflowLabel = WorkflowGroupName + "/backfill"
	IsBarrierWorkflowLabel  = WorkflowGroupName + "/backfill-barrier"

	WorkflowControlStateLabel = WorkflowGroupName + "/state" // Tracks control state (Active/Suspend/Stopped/Terminated) for sensor lifecycle management

	// WorkflowTemplateTimestampLabel tracks the Unix timestamp when a versioned WorkflowTemplate was created
	WorkflowTemplateTimestampLabel = WorkflowGroupName + "/template-timestamp"

	ManagedByLabel = "app.kubernetes.io/managed-by"
	ManagerByValue = "lakeflow-controller"

	// ArgoCronWorkflowLabel is the label Argo stamps on child Workflows spawned by a
	// CronWorkflow, holding the owning CronWorkflow's name. Owned by Argo, not lakeflow.io.
	ArgoCronWorkflowLabel = "workflows.argoproj.io/cron-workflow"

	ResubmitRequestAnnotation     = WorkflowGroupName + "/resubmit-request"
	RetryRequestAnnotation        = WorkflowGroupName + "/rerun-request"
	RetryMaxAttemptsAnnotation    = WorkflowGroupName + "/rerun-max-attempts"
	RetrySkipSuccessfulAnnotation = WorkflowGroupName + "/rerun-skip-successful"
	// RerunTokenAnnotation holds a durable, per-request idempotency token stamped
	// by the controller before a rerun action runs. It drives deterministic rerun
	// resource names so creation is atomic, and is cleared together with the other
	// rerun annotations on any terminal outcome.
	RerunTokenAnnotation = WorkflowGroupName + "/rerun-token"

	// RerunLockLabel marks Lease objects used as the per-task rerun lock.
	RerunLockLabel = WorkflowGroupName + "/rerun-lock"

	VolcanoSchedulerName      = "volcano"
	VolcanoQueueAnnotationKey = "scheduling.volcano.sh/queue-name"

	SparkLocalDirPVCTypeEBS = "ebs"

	BackFillTemplateLabel    = WorkflowGroupName + "/backfill-template"
	BackFillDateLabel        = WorkflowGroupName + "/backfill-date"
	BackFillSessionIDLabel   = WorkflowGroupName + "/backfill-session"
	BackFillSessionNameLabel = WorkflowGroupName + "/backfill-name"
)
