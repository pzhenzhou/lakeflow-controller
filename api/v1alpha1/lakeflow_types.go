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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// Trigger defines the trigger spec.
type Trigger struct {
	// DependTrigger defines the upstream workflow that the current workflow depends on.
	// +optional
	Depend DependTrigger `json:"depend,omitempty"`
	// ScheduleTrigger defines the schedule trigger for the LakeFlow
	// +optional
	Schedule ScheduleTrigger `json:"schedule,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// ScheduleTrigger defines the schedule trigger for the LakeFlow
type ScheduleTrigger struct {
	// Cron expression for the schedule trigger
	// +kubebuilder:validation:Required
	Cron string `json:"cron"`
	// Timezone for the schedule trigger
	// +optional
	Timezone string `json:"timezone,omitempty"`
	// Jitter is a random offset to the scheduled time
	// +optional
	// +kubebuilder:validation:Pattern:="^([0-9]+(s|m|h))$"
	Jitter string `json:"jitter,omitempty"`
	// ConcurrencyPolicy defines how to handle concurrent executions of the scheduled workflow
	// - "Allow": allows multiple workflows to run concurrently (default Argo behavior)
	// - "Forbid": prevents new workflows from starting if previous instance is still running
	// - "Replace": terminates currently running workflow before starting new one
	// +optional
	// +kubebuilder:default="Forbid"
	// +kubebuilder:validation:Enum=Allow;Forbid;Replace
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty"`
	// StopStrategy defines when to automatically stop scheduling new workflows
	// +optional
	StopStrategy *StopStrategy `json:"stopStrategy,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// StopStrategy defines when to automatically stop scheduling new workflows for a ScheduleTrigger
type StopStrategy struct {
	// Phase defines the workflow execution phase to count for stopping condition
	// - "success": count successful workflow executions
	// - "failure": count failed workflow executions
	// - "complete": count all completed workflow executions (successful + failed)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=success;failure;complete
	Phase string `json:"phase"`

	// Count is the number of workflow executions in the specified phase after which scheduling will stop
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Count int32 `json:"count"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// DependTrigger defines the upstream workflow that the current workflow depends on.
type DependTrigger struct {
	// Mode defines the trigger mode for the depend trigger
	// +optional
	// +kubebuilder:default=all
	// +kubebuilder:validation:Enum=all
	Mode DependTriggerMode `json:"mode,omitempty"`
	// Workflows define the upstream workflows that the current workflow depends on
	// +optional
	Workflows []Upstream `json:"workflows,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// Upstream defines the upstream workflow that the current workflow depends on.
type Upstream struct {
	// Name of the upstream LakeFlow
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// Namespace of the upstream LakeFlow
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`
	// Phase of the upstream LakeFlow
	// +kubebuilder:validation:Enum=Succeeded;Failed;Completed
	Phase string `json:"phase,omitempty"`
	// Minimum offset between the completion time of the upstream LakeFlow and the start time of the current LakeFlow
	// +optional
	MinOffset metav1.Duration `json:"minOffset,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// Task is minimum unit of execution in a LakeFlow.
type Task struct {
	// Name of the task. Must be a valid RFC1123 DNS label, at most 40 characters,
	// and unique within the workflow (the validating webhook enforces uniqueness).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
	// Executor of the task
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=spark;bash;python
	Executor TaskExecutor `json:"executor"`
	// Tasks that the current task depends on
	// +optional
	DependsOn []string `json:"dependsOn,omitempty"`
	// RetryPolicy defines the retry policy for the task
	// +optional
	RetryPolicy *TaskRetryPolicy `json:"retryPolicy,omitempty"`
	// TaskSpec is the specification for the task
	TaskSpec `json:",inline"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// TaskRetryPolicy defines the retry policy for a task
type TaskRetryPolicy struct {
	// MaxRetries is the maximum number of times a task can be retried upon failure.
	// +optional
	MaxRetries int32 `json:"maxRetries,omitempty"`
	// BackOff is the initial duration to wait before retrying a task.
	// This duration is multiplied by the BackoffFactor for subsequent retries.
	// +optional
	BackOff metav1.Duration `json:"backOff,omitempty"`
	// BackoffFactor is the factor by which the backoff duration is multiplied for each subsequent retry.
	// For example, if BackOff is 1s and BackoffFactor is 2, retries will occur after 1s, 2s, 4s, etc.
	// Must be greater than or equal to 1.0.
	// +optional
	BackoffFactor float64 `json:"backoffFactor,omitempty"`
	// MaxDuration is the maximum duration for which a task can be retried.
	// +optional
	MaxDuration metav1.Duration `json:"maxDuration,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// TaskSpec defines the specification for a task
// +kubebuilder:validation:XValidation:rule="has(self.sparkExecutor) || has(self.commandExecutor)",message="either sparkExecutor or commandExecutor must be specified"
type TaskSpec struct {
	// ServiceAccountName is the name of the service account to use for this task
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
	// QueueName is the name of the Volcano queue where the task is running.
	// +optional
	QueueName string `json:"queueName,omitempty"`
	// PodScheduling configures native Kubernetes scheduling for the pods this task creates.
	// +optional
	PodScheduling *TaskPodSchedulingSpec `json:"podScheduling,omitempty"`
	// SparkExecutorSpec is the specification for the Spark executor
	// +optional
	SparkExecutorSpec *SparkExecutorSpec `json:"sparkExecutor,omitempty"`
	// CommandExecutorSpec is the specification for the command executor
	// +optional
	CommandExecutorSpec *CommandExecutorSpec `json:"commandExecutor,omitempty"`
}

// TaskPodSchedulingSpec defines native Kubernetes scheduling for the pods a task
// creates. It is applied to Spark driver and executor pods and to command/Argo
// template pods. Business-oriented placement intent is mapped onto these native
// fields by lake-foundry; the operator only passes them through.
type TaskPodSchedulingSpec struct {
	// NodeSelector constrains task pods to nodes matching all of these labels.
	// Merged on top of the operator's default node selectors.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Affinity sets node/pod (anti-)affinity for task pods.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// Tolerations sets pod tolerations for task pods.
	// +optional
	// +listType=atomic
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// TopologySpreadConstraints describes how task pods spread across topology
	// domains. List semantics mirror corev1.PodSpec for SSA/partial-patch parity.
	// +optional
	// +listType=map
	// +listMapKey=topologyKey
	// +listMapKey=whenUnsatisfiable
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty" patchStrategy:"merge" patchMergeKey:"topologyKey"`
	// SchedulerName overrides the scheduler for this task's pods.
	// Ignored when QueueName is set, since QueueName implies the Volcano batch scheduler.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`
	// PriorityClassName sets the PriorityClass for this task's pods.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// SparkExecutorSpec defines the specification for the Spark executor
type SparkExecutorSpec struct {
	// Image is the container image for the Spark driver and executor pods.
	// It must be provided by the user; the operator does not derive or default it.
	// +kubebuilder:validation:Required
	Image string `json:"image"`
	// SparkVersion is the version of the Spark application, if not specified,by default, it is 3.5.6
	// +optional
	// +kubebuilder:default:="3.5.6"
	SparkVersion string `json:"sparkVersion,omitempty"`
	// SparkAppType defines the Spark application type (Java or Python)
	// Defaults to Java for backward compatibility with existing Spark jobs
	// +optional
	// +kubebuilder:default:="Java"
	// +kubebuilder:validation:Enum=Java;Python
	SparkAppType SparkApplicationType `json:"sparkAppType,omitempty"`
	// MainClass is the main class of the Spark application
	// Required for Java/Scala applications, must be empty for Python applications
	// +optional
	MainClass string `json:"mainClass,omitempty"`
	// MainApplicationFile is the main application file of the Spark application
	// For Java: path to JAR file (e.g., local:///mnt/spark/app.jar)
	// For Python: path to .py file (e.g., local:///mnt/spark/main.py)
	// Support local file, oss file
	// +kubebuilder:validation:Required
	MainApplicationFile string `json:"mainApplicationFile"`
	// Arguments are the arguments passed to the Spark application
	// +optional
	Arguments []string `json:"args,omitempty"`
	// PyFiles is the list of Python files or archives to be distributed to executors
	// Used for distributing user Python project code (e.g., mypyproject.zip)
	// Maps to SparkApplication.Spec.Deps.PyFiles
	// Only applicable when SparkAppType is Python
	// +optional
	PyFiles []string `json:"pyFiles,omitempty"`
	// PySitePkgsArchive is the path to the Python site-packages archive (tar.gz)
	// Contains third-party Python packages (pyarrow, polars, mysql-connector-python, starrocks, etc.)
	// If specified, will be distributed via spark.archives and added to PYTHONPATH
	// The archive should contain a top-level directory matching the archive name
	// Example: "local:///mnt/spark/pyspark-sitepkgs/pyspark_sitepkgs.tar.gz"
	// Only applicable when SparkAppType is Python
	// +optional
	// +kubebuilder:default:="local:///mnt/spark/pyspark-sitepkgs/pyspark_sitepkgs.tar.gz"
	PySitePkgsArchive string `json:"pySitePkgsArchive,omitempty"`
	// QueueName represents the name of the Volcano queue where the SparkApplication is running.
	// +kubebuilder:validation:Required
	QueueName string `json:"queueName"`
	// DriverResource is the resource requirements for the Spark driver
	// +kubebuilder:validation:Required
	DriverResource SparkResource `json:"driverResource"`
	// DriverOffHeapMemory is the off-heap memory for the Spark driver, for example, "1Gi"
	// +optional
	DriverMemoryOverhead string `json:"driverMemoryOverhead,omitempty"`
	// ExecutorResource is the resource requirements for the Spark executor
	// +kubebuilder:validation:Required
	ExecutorResource SparkResource `json:"executorResource"`
	// ExecutorOffHeapMemory is the off-heap memory for the Spark executor, for example, "1G"
	// if gluten is enabled, this value must be set and greater than 0
	// +optional
	ExecutorOffHeapMemory string `json:"executorOffHeapMemory,omitempty"`
	// ExecutorMemoryOverhead is the memory overhead for the Spark executor, for example, "1G"
	// +optional
	ExecutorMemoryOverhead string `json:"executorMemoryOverhead,omitempty"`
	// CredentialsRef references a Kubernetes Secret holding object-storage credentials.
	// When namespace is empty the LakeFlow's own namespace is used.
	// +optional
	CredentialsRef *corev1.SecretReference `json:"credentialsRef,omitempty"`
	// SparkConfig follows the configuration of the Kubeflow Spark application, for example,
	// +optional
	SparkConfig map[string]string `json:"sparkConfig,omitempty"`
	// UDFJars is an optional list of UDF (user-defined function) JAR file paths to append
	// to the Spark driver/executor extra classpath. Each entry should be an absolute path
	// available inside the Spark image (e.g. /mnt/spark/jars/udf/my-udf.jar).
	// When set, these paths replace the operator's default UDF JARs (configured in the
	// operator's cloud profile). When omitted, the operator falls back to those defaults.
	// +optional
	UDFJars []string `json:"udfJars,omitempty"`
	// ExtraJars is an optional list of additional JAR file paths to append to the Spark
	// driver/executor extra classpath, on top of the operator's runtime JARs and any
	// UDFJars. Use this for per-workflow dependencies not baked into the operator profile.
	// Each entry should be an absolute path available inside the Spark image.
	// +optional
	ExtraJars []string `json:"extraJars,omitempty"`
	// HiveMetastoreSpec is the configuration for the Spark Hive metastore
	// +optional
	HiveMetastoreSpec *HiveMetastoreSpec `json:"hiveMetastoreSpec,omitempty"`
	// IcebergWarehouse is the warehouse path for Iceberg tables
	// +optional
	IcebergWarehouse string `json:"icebergWarehouse,omitempty"`
	// ObjectStorage describes the bucket-backed object storage used by this task.
	// +optional
	ObjectStorage *ObjectStorage `json:"objectStorage,omitempty"`
	// BlockStorage describes the executor local-dir block storage for this task.
	// +optional
	BlockStorage *BlockStorage `json:"blockStorage,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// ObjectStorage describes a bucket-backed object storage location used by a task.
// It carries workload intent (where the data is, how it is consumed) plus optional
// overrides. Provider-specific provisioning details (CSI driver, default storage
// class, credential key conventions, etc.) are resolved from the operator's cloud
// profile rather than from this workload spec.
// +kubebuilder:validation:XValidation:rule="self.accessMode != 'mount' || has(self.mountSpec)",message="mountSpec is required when accessMode is 'mount'"
// +kubebuilder:validation:XValidation:rule="self.accessMode != 'uri' || !has(self.mountSpec)",message="mountSpec must not be set when accessMode is 'uri'"
type ObjectStorage struct {
	// Bucket is the object storage bucket name.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`
	// Path is the path/prefix within the bucket.
	// +kubebuilder:validation:Required
	Path string `json:"path"`
	// Endpoint is the object storage service endpoint (optional override).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// Region is the object storage region (optional override).
	// +optional
	Region string `json:"region,omitempty"`
	// AccessMode selects how the bucket is consumed: "mount" (CSI-backed PVC) or
	// "uri" (direct filesystem access via object-store URI).
	// +kubebuilder:validation:Enum=mount;uri
	// +kubebuilder:default:=mount
	// +optional
	AccessMode string `json:"accessMode,omitempty"`
	// MountSpec configures the CSI-backed PVC; required when AccessMode is "mount".
	// +optional
	MountSpec *ObjectStorageMount `json:"mountSpec,omitempty"`
	// StorageClass references the storage class used for the mounted PVC (optional override).
	// When empty, the operator derives a content-addressed StorageClass name from the
	// storage identity, guaranteeing a 1:1 mapping to the derived PVC. When set, the name
	// is user-managed: because StorageClass is cluster-scoped and created get-or-create,
	// the same explicit name MUST map to a single bucket/path/endpoint/region/credential
	// identity across the whole cluster (reuse for a different bucket binds to whichever
	// identity created it first). Reuse within one LakeFlow is rejected by validation.
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// ObjectStorageMount configures the PVC used to mount object storage into containers.
type ObjectStorageMount struct {
	// PVCName is the name of the PVC used to mount the bucket. When empty, the
	// operator derives a deterministic, namespace-shared name from the storage
	// identity (bucket/path/endpoint/region/credentials/size) so that identical
	// object-storage configs across tasks and workflows share a single PVC
	// instead of provisioning one PVC per task. When empty, the derived PVC is
	// always operator-managed and auto-created (AutoCreatePVC is ignored).
	// +optional
	PVCName string `json:"pvcName,omitempty"`
	// StorageSize is the requested size for the mount PVC.
	// +kubebuilder:default:="20Gi"
	// +optional
	StorageSize resource.Quantity `json:"storageSize,omitempty"`
	// AutoCreatePVC creates the PVC if it does not already exist. It only applies
	// when PVCName is set explicitly; for a derived (empty PVCName) shared PVC the
	// operator always creates it and this flag is ignored.
	// +optional
	AutoCreatePVC bool `json:"autoCreatePVC,omitempty"`
	// MountPath is the path where the bucket is mounted in the container.
	// +optional
	MountPath string `json:"mountPath,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// BlockStorage describes block storage backing a task's working/local directory.
// +kubebuilder:validation:XValidation:rule="self.mode != 'emptyDir' || !has(self.storageClass)",message="storageClass must not be set when mode is 'emptyDir'"
type BlockStorage struct {
	// Mode selects the backing storage: "pvc" (CSI PersistentVolumeClaim) or "emptyDir".
	// +kubebuilder:validation:Enum=pvc;emptyDir
	// +kubebuilder:validation:Required
	Mode string `json:"mode"`
	// Size is the requested size when Mode is "pvc".
	// +optional
	Size resource.Quantity `json:"size,omitempty"`
	// StorageClass is the storage class when Mode is "pvc".
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// PythonProjectMode defines execution of a Python project from a compressed archive
// The operator extracts the archive to /data and derives the project directory from the archive name
// Example: /mnt/oss/projects/simple-etl.zip -> extracted to /data/simple-etl
// Dependency management (pip/uv) is handled at the project level within the archive
// +kubebuilder:validation:XValidation:rule="has(self.entryPoint) || has(self.cmd)",message="either entryPoint or cmd must be specified"
// +kubebuilder:validation:XValidation:rule="!(has(self.entryPoint) && has(self.cmd))",message="entryPoint and cmd are mutually exclusive, specify only one"
// +kubebuilder:validation:XValidation:rule="!has(self.cmd) || !has(self.args) || size(self.args) == 0",message="args cannot be used with cmd; either use entryPoint with args, or include all arguments in the cmd string"
type PythonProjectMode struct {
	// ArchivePath is the path to the archive file (zip or tar.gz) in the mounted volume
	// The archive name (without extension) becomes the project directory under /data
	// Example: "/mnt/oss/projects/simple-etl.zip" -> project at "/data/simple-etl"
	// +kubebuilder:validation:Required
	ArchivePath string `json:"archivePath"`
	// EntryPoint is the Python script to run after extraction (relative to project directory)
	// Example: "main.py" or "src/app.py"
	// Either EntryPoint or Command must be specified
	// +optional
	EntryPoint string `json:"entryPoint,omitempty"`
	// Command specifies the full command to execute in the project directory
	// Example: "uv run main.py" or "./run.sh" or "python3 -m mymodule"
	// Either EntryPoint or Command must be specified
	// +optional
	Command string `json:"cmd,omitempty"`
	// Arguments are passed to the entry point script (only used with EntryPoint, not Command)
	// +optional
	Arguments []string `json:"args,omitempty"`
	// DependencyScript is the path to a script for installing dependencies (relative to project directory)
	// Example: "build-dep.bash" or "scripts/install.sh"
	// If specified, this script will be executed before the entry point
	// +optional
	DependencyScript string `json:"dependencyScript,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// CommandExecutorSpec defines the specification for the command executor
// +kubebuilder:validation:XValidation:rule="(has(self.scriptFileMode) ? 1 : 0) + (has(self.inlineMode) ? 1 : 0) + (has(self.pythonProjectMode) ? 1 : 0) == 1",message="exactly one execution mode must be specified: scriptFileMode, inlineMode, or pythonProjectMode"
type CommandExecutorSpec struct {
	// Image is the container image used to run this command (python/bash) task.
	// It must be provided by the user; the operator does not derive or default it.
	// +kubebuilder:validation:Required
	Image string `json:"image"`
	// ScriptFileMode: execute script files mounted via volumes (OSS PVC, ConfigMap, Secret, etc.)
	// +optional
	ScriptFileMode *ScriptFileMode `json:"scriptFileMode,omitempty"`
	// InlineMode: execute command directly with specified command and arguments
	// +optional
	InlineMode *InlineMode `json:"inlineMode,omitempty"`
	// PythonProjectMode: execute a Python project from a compressed archive (zip or tar.gz)
	// The operator extracts the archive and runs the specified entry point
	// Only valid when executor type is "python"
	// +optional
	PythonProjectMode *PythonProjectMode `json:"pythonProjectMode,omitempty"`
	// ObjectStorage describes the bucket-backed object storage shared by the command task.
	// +optional
	ObjectStorage *ObjectStorage `json:"objectStorage,omitempty"`
	// BlockStorage describes the per-task workflow working directory storage,
	// realized as an Argo volumeClaimTemplate (mode "pvc").
	// +optional
	BlockStorage *BlockStorage `json:"blockStorage,omitempty"`
	// VolumeMounts is the list of volume mounts for the command executor container
	// Used to mount ConfigMaps, Secrets, or other volumes containing scripts or data
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
	// Volumes is the list of volumes that can be mounted by the command executor container
	// Supports ConfigMap, Secret, EmptyDir, PVC, and other volume types
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`
	// Resources specifies the compute resource requirements for the command executor container
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// CredentialsRef references a Kubernetes Secret holding object-storage credentials.
	// When namespace is empty the LakeFlow's own namespace is used.
	// +optional
	CredentialsRef *corev1.SecretReference `json:"credentialsRef,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// ScriptFileMode defines execution of a script file from mounted volumes
// Recommended for OSS PVC-mounted scripts for easy updates without CRD changes
type ScriptFileMode struct {
	// ScriptPath is the full path to the script file in the mounted volume
	// Example: "/mnt/oss/scripts/process.py" or "/mnt/oss/scripts/transform.sh"
	// When using pvcSpec with OSS, this would typically be under the OSS mount path
	// +kubebuilder:validation:Required
	ScriptPath string `json:"scriptPath"`

	// Arguments are the arguments passed to the script
	// +optional
	Arguments []string `json:"args,omitempty"`
}

// InlineMode defines direct command execution
type InlineMode struct {
	// Command is the command to be executed, for example ["/bin/bash", "-c", "echo hello"]
	// +kubebuilder:validation:Required
	Command []string `json:"command"`
	// Arguments are the arguments passed to the command
	// +optional
	Arguments []string `json:"args,omitempty"`
}

type SparkResource struct {
	// Resources is the resource requirements for the Spark application
	// +kubebuilder:validation:Required
	Resources corev1.ResourceRequirements `json:"resources"`
	// +optional
	// +kubebuilder:default:=1
	Replicas int32 `json:"replicas,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// HiveMetastoreSpec is the configuration for the Spark Hive metastore
type HiveMetastoreSpec struct {
	// HiveMetastoreHost is the host of the Hive metastore
	// +optional
	HiveMetastoreHost string `json:"hiveMetastoreHost,omitempty"`
	// HiveHmsUri is the uri of the Hive metastore thrift service for example thrift://hms-mgr.hive-metastore.svc.cluster.local:9083
	// +optional
	// +kubebuilder:default:="thrift://hive3-metastore.lakeflow.io:9056"
	HiveHmsUri string `json:"hiveHmsUri,omitempty"`
	// HiveMetastorePort is the port of the Hive metastore
	// +optional
	HiveMetastorePort int `json:"hiveMetastorePort,omitempty"`
	// HiveMetastoreDB is the database name of the Hive metastore
	// +optional
	HiveMetastoreDB string `json:"hiveMetastoreDB,omitempty"`
	// ConnectionParams is the connection parameters for the Hive metastore
	// +optional
	ConnectionParams map[string]string `json:"connectionParams,omitempty"`
	// CredentialsRef references a Kubernetes Secret containing the Hive metastore
	// credentials (keys "userName"/"password"). When namespace is empty the LakeFlow's
	// own namespace is used. Mutually exclusive with the inline UserName/Password.
	// +optional
	CredentialsRef *corev1.SecretReference `json:"credentialsRef,omitempty"`
	// UserName is the username for the Hive metastore
	// +optional
	UserName string `json:"userName,omitempty"`
	// Password is the password for the Hive metastore
	// +optional
	Password string `json:"password,omitempty"`
	// LibPath is the path to the Hive metastore library
	// +kubebuilder:validation:Required
	LibPath string `json:"libPath"`
	// HikariCP is the configuration for the HikariCP connection pool
	// +optional
	HikariCP *HiveMetastoreHikariCP `json:"hikariCP,omitempty"`
}

type HiveMetastoreHikariCP struct {
	// MaxPoolSize is the configuration for the HikariCP connection pool
	// +optional
	MaxPoolSize int `json:"maxPoolSize,omitempty"`
	// MinPoolSize is the configuration for the HikariCP connection pool
	// +optional
	MinPoolSize int `json:"minPoolSize,omitempty"`
	// MaxLifetime is the configuration for the HikariCP connection pool
	// +optional
	MaxLifetime int64 `json:"maxLifetime,omitempty"`
	// ValidationTimeout is the configuration for the HikariCP connection pool
	ValidationTimeout int64 `json:"validationTimeout,omitempty"`
}

type LakeFlowTTLStrategy struct {
	// SecondsAfterCompletion is the number of seconds to wait after the workflow has completed before deleting it.
	// +optional
	SecondsAfterCompletion int64 `json:"secondsAfterCompletion,omitempty"`
	// SecondsAfterSuccess is the number of seconds to wait after the workflow has succeeded before deleting it.
	// +optional
	SecondsAfterSuccess int64 `json:"secondsAfterSuccess,omitempty"`
	// SecondsAfterFailure is the number of seconds to wait after the workflow has failed before deleting it.
	// +optional
	SecondsAfterFailure int64 `json:"secondsAfterFailure,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// PodGCStrategy defines the pod garbage collection strategy for workflow pods.
// This is separate from TTLStrategy which manages the workflow object lifecycle.
// PodGC controls when workflow pods are deleted to free cluster resources while preserving workflow history.
type PodGCStrategy struct {
	// Strategy defines when to delete pods
	// - "OnPodCompletion": delete pods immediately when they complete (frees resources fastest)
	// - "OnPodSuccess": delete only successful pods immediately
	// - "OnWorkflowCompletion": delete all pods when workflow completes (keeps pods for debugging during execution)
	// - "OnWorkflowSuccess": delete pods only when entire workflow succeeds
	// +optional
	// +kubebuilder:default="OnWorkflowSuccess"
	// +kubebuilder:validation:Enum=OnPodCompletion;OnPodSuccess;OnWorkflowCompletion;OnWorkflowSuccess
	Strategy string `json:"strategy,omitempty"`

	// DeleteDelayDuration specifies how long to wait before deleting pods after the trigger condition is met.
	// Useful for debugging - keeps pods available for log inspection.
	// Examples: "1h", "30m", "0s" (immediate)
	// +optional
	DeleteDelayDuration *metav1.Duration `json:"deleteDelayDuration,omitempty"`
}

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true

// LakeFlowSpec defines the desired state of LakeFlow.
type LakeFlowSpec struct {
	// PodGC defines pod garbage collection strategy for workflow pods.
	// Controls when workflow pods are deleted to free cluster resources.
	// If not specified, defaults to OnWorkflowCompletion (pods deleted after workflow finishes).
	// This is the RECOMMENDED way to manage resource cleanup while preserving workflow history.
	// +optional
	PodGC *PodGCStrategy `json:"podGC,omitempty"`

	// TTLStrategy defines the TTL strategy for the WORKFLOW OBJECT itself (not pods).
	// IMPORTANT: This controls deletion of the workflow metadata/execution history, NOT pods.
	// For workflows with cross-workflow dependencies, set all values to 0 to preserve history.
	// When nil or all zeros, workflows are retained indefinitely (recommended for dependency-based workflows).
	// Use PodGC instead for cleaning up pods to free resources.
	// +optional
	TTLStrategy *LakeFlowTTLStrategy `json:"ttlStrategy,omitempty"`

	// ServiceAccountName is the name of the service account to use for this workflow.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
	// WorkflowTrigger defines the trigger for the LakeFlow
	// +kubebuilder:validation:Required
	WorkflowTrigger Trigger `json:"trigger,omitempty"`
	// Tasks is the list of tasks to be executed. Capped at 30 because larger DAGs
	// impose significant load on the Argo controller and etcd (object size and
	// per-node pod/PodGroup overhead).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxItems=30
	Tasks []Task `json:"tasks"`
	// Parallelism is the number of tasks that can be executed in parallel, If not specified, all are parallel.
	// +optional
	Parallelism int `json:"parallelism,omitempty"`
	// State defines the desired control state of the workflow
	// - Active: Normal execution, actively running/scheduling
	// - Suspend: Pause execution, can be resumed
	// - Stopped: Graceful stop, cannot be resumed
	// - Terminated: Force stop, cannot be resumed
	// +kubebuilder:default=Active
	// +optional
	// +kubebuilder:validation:Enum=Active;Suspend;Stopped;Terminated
	State WorkflowControlState `json:"state,omitempty"`
	// TenantKey indicates which tenant the workflow belongs to, providing a more business-oriented context.
	// When shared resources exist within the workflow, specifying a TenantKey ensures better isolation.
	// +optional
	TenantKey string `json:"tenantKey,omitempty"`
}

type LakeFlowCondition struct {
	// Type is the type of the condition.
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// Status is the status of the condition.
	// +kubebuilder:validation:Required
	Status corev1.ConditionStatus `json:"status"`
	// Reason is the reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Message is a human-readable message indicating details about the transition.
	// +optional
	Message string `json:"message,omitempty"`
	// The time when the condition was updated.
	// +kubebuilder:validation:Required
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
}

// LakeFlowStatus defines the observed state of LakeFlow.
type LakeFlowStatus struct {
	// WorkflowName is the name of the argo workflow
	WorkflowName string `json:"workflowName"`
	// WorkflowTemplate is the name of the argo workflow template
	WorkflowTemplate string `json:"workflowTemplate"`
	// LakeFlowName is the name of  workflow.
	LakeFlowName string `json:"lakeWorkflowName"`
	// CronWorkflow is the name of the argo cron workflow
	CronWorkflow string `json:"cronWorkflow"`
	// ObservedGeneration is the generation of the LakeFlow that was last reconciled by the controller.
	// When this matches metadata.generation, it means the controller has fully reconciled the current spec
	// and all underlying Argo resources (WorkflowTemplate, Workflow, CronWorkflow, etc.) are up to date.
	// If metadata.generation > observedGeneration, there are unreconciled changes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Phase represents the current execution phase of the workflow
	// - Pending: Resources being coordinated, cannot execute yet
	// - Idle: Resources ready, no active executions (CronWorkflow waiting/paused)
	// - Running: Resources ready, actively executing tasks
	// - Succeeded/Failed: One-time workflow completed
	// - Completed: CronWorkflow lifecycle finished
	// +optional
	// +kubebuilder:validation:Enum=Pending;Idle;Running;Succeeded;Failed;Completed
	Phase WorkflowPhase `json:"phase,omitempty"`
	// ExecutionState represents the observed control/scheduling state
	// This is the system-observed state, which the controller reconciles toward spec.state
	// - Active: Workflow is actively running or scheduling
	// - Suspend: Workflow has been suspended (CronWorkflow.spec.suspend=true)
	// - Stopped: Workflow has been gracefully stopped
	// - Terminated: Workflow has been forcefully terminated
	// +optional
	// +kubebuilder:validation:Enum=Active;Suspend;Stopped;Terminated
	ExecutionState WorkflowControlState `json:"executionState,omitempty"`
	// RetryStatus records the state of LakeFlow retries.
	// +optional
	RetryStatus *WorkflowRetryStatus `json:"retryStatus,omitempty"`
	// Conditions is the list of conditions for the workflow
	// +listType=map
	// +listMapKey=type
	// +patchMergeKey=type
	// +patchStrategy=merge,retainKeys
	Conditions []LakeFlowCondition `json:"conditions"`
	// FinishedTime tracks workflow completion timestamps
	// +optional
	FinishedTime *LakeFlowFinishedTime `json:"finishedTime,omitempty"`
}

// WorkflowRetryStatus tracks the retry state of a workflow
type WorkflowRetryStatus struct {
	// RetryAttempts is the total number of retry attempts (excluding initial run)
	// +optional
	RetryAttempts int32 `json:"retryAttempts"`
	// LastRetryTime
	// +optional
	LastRetryTime *metav1.Time `json:"lastRetryTime,omitempty"`
	// +optional
	LastRetryReason string `json:"lastRetryReason,omitempty"`
}

// LakeFlowFinishedTime is the timestamp when the workflow finished
type LakeFlowFinishedTime struct {
	// FinishedAt is the timestamp when the workflow finished.
	// +optional
	FinishedAt metav1.Time `json:"finishedAt,omitempty"`
	// LastFinishedTime is the timestamp when the workflow was last finished.
	// if trigger is not schedule, this value is not set.
	// +optional
	LastFinishedTime metav1.Time `json:"lastFinishedTime,omitempty"`
}

// Note: WorkflowControlState constants are defined in lakeflow_consts.go
// The same enum is used for both spec.state (desired) and status.executionState (observed)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen=true
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +operator-sdk:csv:customresourcedefinitions:displayName="LakeFlow Operator"
// +k8s:openapi-gen=true
// +kubebuilder:selectablefield:JSONPath=".spec.state"
// +kubebuilder:selectablefield:JSONPath=".status.phase"
// +kubebuilder:selectablefield:JSONPath=".status.executionState"

// LakeFlow is the Schema for the LakeFlow API.
type LakeFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LakeFlowSpec   `json:"spec,omitempty"`
	Status LakeFlowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LakeFlowList contains a list of LakeFlow.
type LakeFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LakeFlow `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LakeFlow{}, &LakeFlowList{})
}
