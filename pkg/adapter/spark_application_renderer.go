package adapter

import (
	"fmt"
	"maps"
	"math"
	"strings"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

var _ TaskRenderer = (*sparkTaskRenderer)(nil)

// sparkTaskRenderer implements TaskRenderer for Spark tasks.
// Contains all Spark-specific logic directly (no unnecessary interfaces).
//
// It is stateless: the singleton registered in the TaskRendererFactory carries no
// per-conversion state. RenderTask reads the per-conversion credential resolver
// (with its memo cache) and target namespace from the shared ArgoRenderContext, so
// Secret lookups during SparkConf assembly use the LakeFlow namespace.
type sparkTaskRenderer struct {
}

// newSparkTaskRenderer creates a new Spark task renderer
func newSparkTaskRenderer() TaskRenderer {
	return &sparkTaskRenderer{}
}

// RenderTask renders the Argo template for an already-resolved Spark task. The
// shared render context supplies the per-conversion credential resolver and the
// LakeFlow being converted.
func (s *sparkTaskRenderer) RenderTask(ctx *ArgoRenderContext, task *v1alpha1.Task) (*argowfv1.Template, error) {
	return s.build(ctx, task)
}

// build renders the Argo template for an already-resolved Spark task. The
// credential resolver used during SparkConf assembly comes from the context.
func (s *sparkTaskRenderer) build(ctx *ArgoRenderContext, task *v1alpha1.Task) (*argowfv1.Template, error) {
	workflow := ctx.LakeFlow
	// Validate Spark local-dir PVC labels up front (invalid labels must fail the
	// build exactly as before). The actual StorageClass/PVC provisioning is now
	// performed by the reconciler (see reconciler.applyInfra) rather than here, so
	// builders stay pure and free of cluster side effects.
	if _, err := getSparkLocalDirPVCOptions(workflow); err != nil {
		return nil, err
	}

	// Build SparkApplication directly from SparkExecutorSpec
	sparkApp := s.buildSparkApplicationFromSpec(workflow, task)
	s.setSparkSpecConfig(sparkApp, *task.TaskSpec.SparkExecutorSpec, ctx.CredentialResolver, ctx.CloudProfile)
	s.setPySparkDependencies(sparkApp, task.TaskSpec.SparkExecutorSpec)
	s.setSparkSpecVolumes(ctx, sparkApp, *task.TaskSpec.SparkExecutorSpec)
	// Create Argo Template with ResourceTemplate containing SparkApplication
	template := &argowfv1.Template{
		Name: task.Name,
		// Add metadata with both workflow and task labels for consistent querying
		Metadata: argowfv1.Metadata{
			Labels: buildArgoTemplateLabels(workflow, task),
		},
	}

	// Add retry policy if specified
	template.RetryStrategy = buildRetryStrategy(task)

	// Apply task-level service account if specified
	if task.TaskSpec.ServiceAccountName != "" {
		template.ServiceAccountName = task.TaskSpec.ServiceAccountName
	}

	// Convert SparkApplication to YAML manifest for ResourceTemplate
	manifest, err := yaml.Marshal(sparkApp)
	if err != nil {
		logger.Error(err, "Failed to marshal SparkApplication to YAML", "task", task.Name, "lakeflow", workflow.Name, "namespace", workflow.Namespace)
		return nil, fmt.Errorf("failed to marshal SparkApplication to YAML for task %s: %w", task.Name, err)
	}

	// Clean up problematic fields from the manifest
	manifestStr := string(manifest)
	manifestStr = cleanSparkApplicationManifest(manifestStr)

	template.Resource = &argowfv1.ResourceTemplate{
		Action:           "create",
		Manifest:         manifestStr,
		SuccessCondition: "status.applicationState.state == COMPLETED",
		FailureCondition: "status.applicationState.state == FAILED",
	}
	return template, nil
}

// cleanSparkApplicationManifest removes problematic fields from the YAML manifest
// Note: For PySpark support, we intentionally keep the deps section as it contains
// pyFiles configuration required for Python dependency distribution
func cleanSparkApplicationManifest(manifest string) string {
	lines := strings.Split(manifest, "\n")
	var cleanedLines []string
	skipUntilSameLevel := false
	skipLevel := 0
	for _, line := range lines {
		// Calculate indentation level
		trimmed := strings.TrimLeft(line, " ")
		currentLevel := len(line) - len(trimmed)

		// Check if we're skipping a section
		if skipUntilSameLevel {
			if currentLevel <= skipLevel && trimmed != "" {
				skipUntilSameLevel = false
			} else {
				continue
			}
		}

		// Skip problematic fields
		// Note: deps section is now preserved for PySpark support (pyFiles)
		if strings.HasPrefix(trimmed, "restartPolicy:") ||
			strings.HasPrefix(trimmed, "status:") {
			skipUntilSameLevel = true
			skipLevel = currentLevel
			continue
		}

		cleanedLines = append(cleanedLines, line)
	}

	return strings.Join(cleanedLines, "\n")
}

// convertCpuToCores converts Kubernetes CPU resource to Spark cores with validation
// Enforces minimum 1 core as required by SparkOperator CRD validation
func (s *sparkTaskRenderer) convertCpuToCores(cpuQuantity *resource.Quantity) int32 {
	if cpuQuantity.IsZero() {
		logger.Info("CPU quantity is zero, setting to minimum 1 core")
		return 1
	}
	// Get millicores value (e.g., 500m = 500 millicores)
	millicores := cpuQuantity.MilliValue()
	// Convert millicores to cores (1000 millicores = 1 core)
	cores := float64(millicores) / 1000.0
	// Round to nearest integer and enforce minimum of 1 as per CRD validation
	sparkCores := int32(cores + 0.5)
	if sparkCores < 1 {
		sparkCores = 1
		logger.Info("CPU cores below minimum, setting to 1 core",
			"originalCpu", cpuQuantity.String(), "sparkCores", sparkCores)
	}
	logger.Info("CPU resource conversion", "originalCpu", cpuQuantity.String(),
		"millicores", millicores, "cores", cores, "sparkCores", sparkCores)
	return sparkCores
}

// calculateSparkApplicationTTL calculates TTL seconds for SparkApplication using unified strategy
// Priority order:
// 1. LakeFlow.Spec.TTLStrategy (unified approach)
// 2. System default (successTTL)
func (s *sparkTaskRenderer) calculateSparkApplicationTTL(lw *v1alpha1.LakeFlow, task *v1alpha1.Task) int64 {
	maxTTL := int64(0)
	// Check if TTLStrategy is specified
	if lw.Spec.TTLStrategy != nil {
		lakeTTL := lw.Spec.TTLStrategy
		if lakeTTL.SecondsAfterCompletion > 0 {
			// SecondsAfterCompletion covers both success and failure scenarios
			maxTTL = lakeTTL.SecondsAfterCompletion
			logger.Info("Using LakeFlow TTL strategy (completion)",
				"ttlSeconds", maxTTL, "task", task.Name, "workflow", lw.Name)
		} else {
			// Use maximum of success/failure TTLs since SparkApplication TTL is set at creation time
			// and we don't know the final outcome yet
			maxTTL = max(lakeTTL.SecondsAfterSuccess, lakeTTL.SecondsAfterFailure)
		}
		logger.Info("Using LakeFlow TTL strategy (max of success/failure)",
			"ttlSeconds", maxTTL, "secondsAfterSuccess", lakeTTL.SecondsAfterSuccess,
			"secondsAfterFailure", lakeTTL.SecondsAfterFailure, "task", task.Name, "workflow", lw.Name)
	}
	// Apply system default if no TTL is specified
	if maxTTL == 0 {
		maxTTL = int64(successTTL)
		logger.Info("Using system default TTL",
			"ttlSeconds", maxTTL, "task", task.Name, "workflow", lw.Name)
	}
	logger.Info("SparkApplication TTL calculated",
		"finalTtlSeconds", maxTTL, "task", task.Name, "workflow", lw.Name, "namespace", lw.Namespace)
	return maxTTL
}

// buildOwnerReferences builds owner references only if the LakeFlow has a valid UID
// This ensures compatibility with testing scenarios where UID may not be available
// Sets Controller=true for ownership tracking, but BlockOwnerDeletion=false to allow
// Spark Operator's TTL mechanism to delete SparkApplications independently
func (s *sparkTaskRenderer) buildOwnerReferences(workflow *v1alpha1.LakeFlow) []metav1.OwnerReference {
	// Only create owner references if UID is available and not empty
	if workflow.UID != "" {
		controller := true
		// Set blockOwnerDeletion to false to allow Spark Operator's TTL cleanup
		// to delete SparkApplications after completion, without waiting for
		// the parent LakeFlow to be deleted
		blockOwnerDeletion := false
		return []metav1.OwnerReference{
			{
				APIVersion:         workflow.APIVersion,
				Kind:               workflow.Kind,
				Name:               workflow.Name,
				UID:                workflow.UID,
				Controller:         &controller,
				BlockOwnerDeletion: &blockOwnerDeletion,
			},
		}
	}
	// Return empty slice if UID is not available
	return []metav1.OwnerReference{}
}

func buildSparkApplicationLabels(workflow *v1alpha1.LakeFlow, task *v1alpha1.Task) map[string]string {
	labels := map[string]string{
		v1alpha1.WorkflowNameLabel:       workflow.Name,
		v1alpha1.WorkflowTaskNameLabel:   task.Name,
		"workflows.argoproj.io/workflow": "{{workflow.name}}",
	}
	labels = addSparkLocalDirPVCLabels(labels, workflow.GetLabels())
	applyRerunRetryLabels(labels, workflow)
	return labels
}

// buildSparkApplicationFromSpec creates SparkApplication directly from SparkExecutorSpec
// All SparkApplication logic derives from SparkExecutorSpec as requested
func (s *sparkTaskRenderer) buildSparkApplicationFromSpec(workflow *v1alpha1.LakeFlow, task *v1alpha1.Task) *sparkv1beta2.SparkApplication {
	spec := task.TaskSpec.SparkExecutorSpec
	// Image is a required, user-specified field; the operator does not derive it.
	image := spec.Image
	sparkVersion := spec.SparkVersion
	if sparkVersion == "" {
		sparkVersion = DefaultSparkVersion
	}
	serviceAccount := task.TaskSpec.ServiceAccountName
	var serviceAccountPtr *string
	if serviceAccount != "" {
		serviceAccountPtr = &serviceAccount
	}
	// MainApplicationFile is a required field; when left empty the operator profile's
	// DefaultMainApplicationFile is applied later in setSparkSpecConfig (where the
	// cloud profile is available).
	mainAppLocationFile := spec.MainApplicationFile
	// Create minimal SparkApplication without problematic default fields
	sparkApp := &sparkv1beta2.SparkApplication{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "sparkoperator.k8s.io/v1beta2",
			Kind:       "SparkApplication",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-", task.Name),
			Namespace:    workflow.Namespace,
			// Apply both workflow and task labels consistently for querying
			Labels:          buildSparkApplicationLabels(workflow, task),
			OwnerReferences: s.buildOwnerReferences(workflow),
		},
		Spec: sparkv1beta2.SparkApplicationSpec{
			Type:                sparkv1beta2.SparkApplicationType(s.getSparkApplicationType(spec)),
			Mode:                "cluster",
			Image:               &image,
			ImagePullPolicy:     &[]string{"IfNotPresent"}[0],
			SparkVersion:        sparkVersion,
			MainClass:           s.getMainClassPointer(spec),
			MainApplicationFile: &mainAppLocationFile,
			SparkConf:           make(map[string]string),
		},
	}
	// Only set Arguments if they exist
	if len(spec.Arguments) > 0 {
		sparkApp.Spec.Arguments = spec.Arguments
	}

	// Use Spark Operator's BatchScheduler approach for Volcano integration
	// This ensures proper PodGroup creation and lifecycle management
	if spec.QueueName != "" {
		batchScheduler := "volcano"
		sparkApp.Spec.BatchScheduler = &batchScheduler
		sparkApp.Spec.BatchSchedulerOptions = &sparkv1beta2.BatchSchedulerConfiguration{
			Queue: &spec.QueueName,
		}
	}

	// Use unified TTL strategy from LakeFlow
	ttlSeconds := s.calculateSparkApplicationTTL(workflow, task)
	sparkApp.Spec.TimeToLiveSeconds = &ttlSeconds
	// Set driver configuration from SparkExecutorSpec
	driverMemory := spec.DriverResource.Resources.Requests.Memory().String()
	driverCores := s.convertCpuToCores(spec.DriverResource.Resources.Requests.Cpu())
	uid := int64(ExecutorUserUID)
	fsGroupChangePolicy := corev1.FSGroupChangeAlways

	// Set driver configuration from SparkExecutorSpec
	// PodSecurityContext with FSGroup ensures the emptyDir at /driver-local-dir
	// is group-owned by the spark user (UID 185), allowing the driver to write scratch data.
	sparkApp.Spec.Driver = sparkv1beta2.DriverSpec{
		SparkPodSpec: buildSparkPodSpec(driverCores, driverMemory, serviceAccountPtr, buildSparkAppVolumes()),
	}
	sparkApp.Spec.Driver.PodSecurityContext = &corev1.PodSecurityContext{
		FSGroup:             &uid,
		FSGroupChangePolicy: &fsGroupChangePolicy,
	}
	if spec.DriverMemoryOverhead != "" {
		sparkApp.Spec.Driver.MemoryOverhead = &spec.DriverMemoryOverhead
	}
	// Set executor configuration from SparkExecutorSpec
	executorMemory := spec.ExecutorResource.Resources.Requests.Memory().String()
	executorCores := s.convertCpuToCores(spec.ExecutorResource.Resources.Requests.Cpu())
	// Set DeleteOnTermination to false to retain executor pods until TTL expires
	// This allows for post-execution debugging and inspection while still cleaning up via TTL
	deleteOnTermination := false
	sparkApp.Spec.Executor = sparkv1beta2.ExecutorSpec{
		SparkPodSpec:        buildSparkPodSpec(executorCores, executorMemory, serviceAccountPtr, buildSparkAppVolumes()),
		Instances:           &spec.ExecutorResource.Replicas,
		DeleteOnTermination: &deleteOnTermination,
	}
	sparkApp.Spec.Executor.SecurityContext = &corev1.SecurityContext{
		RunAsUser:  &uid,
		RunAsGroup: &uid,
	}
	sparkApp.Spec.Executor.PodSecurityContext = &corev1.PodSecurityContext{
		FSGroup:             &uid,
		FSGroupChangePolicy: &fsGroupChangePolicy,
	}
	// Set executor memory overhead - use SparkPodSpec.MemoryOverhead as single source of truth
	// SparkOperator automatically handles the Spark configuration based on this setting
	var executorFinalOverheadMemory string
	if spec.ExecutorMemoryOverhead == "" {
		executorFinalOverheadMemory = memoryOverhead(*sparkApp.Spec.Executor.SparkPodSpec.Memory,
			ExecutorMemoryOverheadFactor,
		)
	} else {
		executorFinalOverheadMemory = spec.ExecutorMemoryOverhead
	}
	// Apply memory overhead to SparkPodSpec only - SparkOperator handles Spark config automatically
	if executorFinalOverheadMemory != "" {
		sparkApp.Spec.Executor.SparkPodSpec.MemoryOverhead = &executorFinalOverheadMemory
	}

	if task.TaskSpec.PodScheduling != nil {
		applyTaskSchedulingToSparkApplication(sparkApp, task.TaskSpec.PodScheduling, spec.QueueName)
	} else {
		// Apply the default volcano node selector at the pod level (driver/executor)
		// rather than the deprecated app-level spec.nodeSelector.
		sparkApp.Spec.Driver.NodeSelector = maps.Clone(defaultNodeSelectors)
		sparkApp.Spec.Executor.NodeSelector = maps.Clone(defaultNodeSelectors)
	}
	return sparkApp
}

func (s *sparkTaskRenderer) setSparkSpecVolumes(ctx *ArgoRenderContext, sparkApp *sparkv1beta2.SparkApplication, sparkExecutor v1alpha1.SparkExecutorSpec) {
	objStorage := sparkExecutor.ObjectStorage
	if objStorage == nil || objStorage.MountSpec == nil {
		return
	}
	// Resolve the claim name via the shared helper so the SparkApplication volume
	// references exactly the PVC the reconciler provisions (explicit pvcName, or a
	// derived namespace-shared name when pvcName is empty).
	claimName := cloud.EffectiveObjectStoragePVCName(objStorage, sparkExecutor.CredentialsRef, ctx.Namespace, ctx.CloudProfile)
	sparkApp.Spec.Volumes = []corev1.Volume{
		{
			Name: SparkExecutorVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claimName,
				},
			},
		},
	}
}

func buildEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "JAVA_VERSION",
			Value: "17.0.15",
		},
		{
			Name:  "JAVA_SPECIFICATION_VERSION",
			Value: "17",
		},
		{
			Name:  "LD_LIBRARY_PATH",
			Value: "/mnt/spark/native:/lib64:/usr/lib64:/lib:/usr/lib",
		},
	}
}

func buildSparkPodSpec(cores int32, memory string, serviceAccount *string, mounts []corev1.VolumeMount) sparkv1beta2.SparkPodSpec {
	logger.Info("Building SparkPodSpec", "cores", cores, "memory", memory, "serviceAccount", serviceAccount, "mounts", mounts)
	sparkPod := sparkv1beta2.SparkPodSpec{
		Cores:  &cores,  // Set cores field with minimum 1 validation
		Memory: &memory, // Set memory field directly
		Env:    buildEnv(),
	}

	if serviceAccount != nil {
		sparkPod.ServiceAccount = serviceAccount
	}
	if len(mounts) > 0 {
		sparkPod.VolumeMounts = mounts
	}

	return sparkPod
}

func buildHiveMetastoreConnectionUrl(hiveMetastoreSpec *v1alpha1.HiveMetastoreSpec) string {
	connUrl := fmt.Sprintf("jdbc:mysql://%s:%d/%s?useSSL=false&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true",
		hiveMetastoreSpec.HiveMetastoreHost, hiveMetastoreSpec.HiveMetastorePort, hiveMetastoreSpec.HiveMetastoreDB)
	for key, val := range hiveMetastoreSpec.ConnectionParams {
		if strings.Contains(connUrl, key) {
			continue
		}
		connUrl += fmt.Sprintf("&%s=%s", key, val)
	}
	return connUrl
}

func buildSparkAppVolumes() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      SparkExecutorVolumeName,
			MountPath: SparkExecutorMountPath,
		},
	}
}

func buildSparkExtraClassPath(sparkExecutor *v1alpha1.SparkExecutorSpec, profile cloudprofile.CloudProfile) string {
	classpath := buildDefaultExtraClassPath(profile)
	if udfJars := resolveUDFJars(sparkExecutor, profile); len(udfJars) > 0 {
		classpath = append(classpath, udfJars...)
		logger.Info("UDF libraries added to classpath",
			"sparkVersion", sparkExecutor.SparkVersion,
			"udfJars", udfJars,
			"userProvided", len(sparkExecutor.UDFJars) > 0)
	}
	if len(sparkExecutor.ExtraJars) > 0 {
		classpath = append(classpath, sparkExecutor.ExtraJars...)
	}
	return strings.Join(classpath, ":")
}

// resolveUDFJars returns the UDF JAR paths to append to the Spark extra classpath.
// Per-workflow jars from the SparkExecutorSpec take precedence; otherwise the
// operator profile defaults are applied, gated to the legacy Spark versions that
// historically required them.
func resolveUDFJars(sparkExecutor *v1alpha1.SparkExecutorSpec, profile cloudprofile.CloudProfile) []string {
	if len(sparkExecutor.UDFJars) > 0 {
		return sparkExecutor.UDFJars
	}
	if sparkExecutor.SparkVersion == "3.5.3" || sparkExecutor.SparkVersion == "3.5.2" {
		return profile.SparkRuntime.UDFJars
	}
	return nil
}

func memoryOverhead(executorMemory string, factor float64) string {
	// Standard Spark memory overhead calculation: max(executorMemory * factor, minOverhead)
	heapQty, err := resource.ParseQuantity(executorMemory)
	if err != nil {
		logger.Error(err, "Failed to parse executor memory", "executorMemory", executorMemory)
		return ""
	}

	// Calculate overhead as percentage of executor memory (rounded up)
	heapBytes := heapQty.Value()
	heapOverheadBytes := int64(math.Ceil(float64(heapBytes) * factor))

	// Minimum overhead: 400MB = 400 * 1024 * 1024 bytes
	minOverheadBytes := int64(MinMemoryOverhead * 1024 * 1024)

	// Take max of (factor * executorMemory, minOverhead)
	finalOverheadBytes := heapOverheadBytes
	if minOverheadBytes > heapOverheadBytes {
		finalOverheadBytes = minOverheadBytes
	}

	// Convert to megabytes (1 MB = 1024 * 1024 bytes)
	finalOverheadMB := finalOverheadBytes / (1024 * 1024)

	// Return in Spark format with lowercase 'm' (mebibytes)
	result := fmt.Sprintf("%dm", finalOverheadMB)

	logger.Info("Calculated executor memory overhead",
		"executorMemory", executorMemory,
		"factor", factor,
		"calculatedOverheadMB", heapOverheadBytes/(1024*1024),
		"minOverheadMB", minOverheadBytes/(1024*1024),
		"finalOverheadMB", finalOverheadMB,
		"sparkFormat", result)

	return result
}

// setPySparkDependencies configures PySpark-specific dependencies
// Sets PyFiles on SparkApplication.Spec.Deps for Python code distribution
func (s *sparkTaskRenderer) setPySparkDependencies(
	sparkApp *sparkv1beta2.SparkApplication,
	spec *v1alpha1.SparkExecutorSpec,
) {
	if spec.SparkAppType != v1alpha1.SparkAppTypePython {
		return
	}

	// Set PyFiles if specified (user project code)
	if len(spec.PyFiles) > 0 {
		sparkApp.Spec.Deps.PyFiles = spec.PyFiles
		logger.Info("PySpark dependencies configured",
			"pyFiles", spec.PyFiles)
	}
}

// getSparkApplicationType returns the Spark application type from spec
// Defaults to Java for backward compatibility with existing Spark jobs
func (s *sparkTaskRenderer) getSparkApplicationType(spec *v1alpha1.SparkExecutorSpec) v1alpha1.SparkApplicationType {
	if spec.SparkAppType == "" {
		return v1alpha1.SparkAppTypeJava
	}
	return spec.SparkAppType
}

// getMainClassPointer returns a pointer to MainClass for Java applications, nil for Python
// This is critical because the Spark Operator expects MainClass to be nil (not provided)
// for Python applications, not a pointer to an empty string
func (s *sparkTaskRenderer) getMainClassPointer(spec *v1alpha1.SparkExecutorSpec) *string {
	// Determine the application type (default to Java for backward compatibility)
	sparkAppType := s.getSparkApplicationType(spec)

	// Only set MainClass for Java applications
	if sparkAppType == v1alpha1.SparkAppTypeJava {
		// Even for Java, only set if non-empty
		if spec.MainClass != "" {
			return &spec.MainClass
		}
	}

	// For Python applications or empty MainClass, return nil
	return nil
}
