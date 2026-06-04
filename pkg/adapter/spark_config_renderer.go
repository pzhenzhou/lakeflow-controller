package adapter

import (
	"fmt"
	"strconv"

	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
)

const (
	hiveMetaUserNameKey = "userName"
	hiveMetaPasswordKey = "password"
)

// getHiveMetaCredentials resolves Hive metastore credentials through the
// per-conversion CredentialResolver supplied by the ArgoRenderContext, which
// memoizes Secret lookups so a credentialsRef shared by many tasks is fetched at
// most once. In production the resolver is always set (see newArgoRenderContext);
// a nil resolver (only reachable from tests that do not exercise Hive) means
// "skip credential resolution" and returns an informational error.
func (s *sparkTaskRenderer) getHiveMetaCredentials(resolver CredentialResolver, hiveMetastoreSpec *v1alpha1.HiveMetastoreSpec) (string, string, error) {
	if resolver == nil {
		return "", "", fmt.Errorf("no credential resolver configured for Hive metastore")
	}
	return resolver.HiveMetastore(hiveMetastoreSpec)
}

// mergeInto copies every entry from src into dst (src wins on key collisions) and
// returns dst for chaining. A nil src is a no-op. This is the single map-merge
// primitive for SparkConf assembly: callers own the dst clone, so no per-merge
// allocation occurs (unlike the previous lo.Assign helper chain, which allocated a
// fresh map on every feature step).
func mergeInto(dst, src map[string]string) map[string]string {
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// setSparkSpecConfig assembles the SparkApplication's SparkConf via a clone-once,
// mutate-in-place pipeline. The defaults map is cloned exactly once; every
// subsequent step mutates that single map directly.
//
// Configuration precedence (lowest to highest):
//  1. Default configs (defaultSparkSpecConfig) - base settings for all Spark apps
//  2. Feature configs (PVC, Hive3, Iceberg, PySpark, ...) - auto-configured per feature
//  3. User SparkConfig - explicit user overrides, always applied last so they win
//
// Object-storage credentials are supplied via the referenced Secret (credentialsRef)
// and consumed by the CSI driver; they are never embedded as plaintext in SparkConf.
//
// resolver resolves Secret-backed Hive credentials and is threaded down to
// applyHiveMetastoreConfig; it comes from the ArgoRenderContext.
func (s *sparkTaskRenderer) setSparkSpecConfig(sparkApp *sparkv1beta2.SparkApplication, sparkExecutor v1alpha1.SparkExecutorSpec, resolver CredentialResolver, profile cloudprofile.CloudProfile) {
	if profile.Name == "" {
		profile, _ = cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	}
	// MainApplicationFile is required, but apply the profile default defensively when
	// a caller leaves it empty (the cloud profile is only available here).
	if (sparkApp.Spec.MainApplicationFile == nil || *sparkApp.Spec.MainApplicationFile == "") &&
		profile.SparkRuntime.DefaultMainApplicationFile != "" {
		defaultMainApp := profile.SparkRuntime.DefaultMainApplicationFile
		sparkApp.Spec.MainApplicationFile = &defaultMainApp
	}
	localDirPVCOptions, err := getSparkLocalDirPVCOptionsFromLabels(sparkApp.GetLabels())
	if err != nil {
		logger.Error(err, "Skipping invalid Spark local-dir PVC labels on SparkApplication",
			"sparkApplication", sparkApp.Name,
			"namespace", sparkApp.Namespace)
	}

	// Clone the defaults once; the rest of the pipeline mutates this single map.
	defaults := buildDefaultSparkSpecConfig(profile)
	finalConfig := mergeInto(make(map[string]string, len(defaults)), defaults)
	if sparkExecutor.ExecutorOffHeapMemory != "" {
		finalConfig["spark.memory.offHeap.size"] = sparkExecutor.ExecutorOffHeapMemory
	}
	if sparkExecutor.ObjectStorage != nil && sparkExecutor.ObjectStorage.Endpoint != "" {
		finalConfig[profile.ObjectStorage.SparkConfPrefix+".endpoint"] = sparkExecutor.ObjectStorage.Endpoint
	}

	// Feature configs, applied in precedence order (each mutates finalConfig in place).
	s.applyFeatureConfigs(finalConfig, sparkExecutor, localDirPVCOptions, resolver, profile)

	// User SparkConfig wins last.
	mergeInto(finalConfig, sparkExecutor.SparkConfig)

	sparkApp.Spec.SparkConf = finalConfig
}

// applyFeatureConfigs applies every feature-specific config to conf in place,
// in precedence order. Each feature is declared once.
func (s *sparkTaskRenderer) applyFeatureConfigs(
	conf map[string]string,
	sparkExecutor v1alpha1.SparkExecutorSpec,
	localDirPVCOptions *sparkLocalDirPVCOptions,
	resolver CredentialResolver,
	profile cloudprofile.CloudProfile,
) {
	// Set default extra class path if not already configured
	defaultExtraClassPath := buildSparkExtraClassPath(&sparkExecutor, profile)
	if _, ok := conf["spark.driver.extraClassPath"]; !ok {
		conf["spark.driver.extraClassPath"] = defaultExtraClassPath
	}
	if _, ok := conf["spark.executor.extraClassPath"]; !ok {
		conf["spark.executor.extraClassPath"] = defaultExtraClassPath
	}

	s.applyPvcConfig(conf, sparkExecutor.BlockStorage, localDirPVCOptions, profile.BlockStorage.DefaultStorageClass)
	s.applyHiveMetastoreConfig(conf, sparkExecutor.HiveMetastoreSpec, resolver, profile)
	s.applyIcebergWarehouseConfig(conf, sparkExecutor.IcebergWarehouse, sparkExecutor.HiveMetastoreSpec)
	s.applyPySparkConfig(conf, &sparkExecutor)
	conf["spark.executor.extraLibraryPath"] = "/mnt/spark/native"
	conf["spark.driver.extraLibraryPath"] = "/mnt/spark/native"
}

// Helper methods for configuration management

// setConfigValue safely sets a configuration key-value pair if the value is not empty
func (s *sparkTaskRenderer) setConfigValue(config map[string]string, key, value string) {
	if value != "" {
		config[key] = value
	}
}

// setConfigIntValue safely sets a configuration key with an integer value if the value is greater than 0
func (s *sparkTaskRenderer) setConfigIntValue(config map[string]string, key string, value int) {
	if value > 0 {
		config[key] = strconv.Itoa(value)
	}
}

// setConfigInt64Value safely sets a configuration key with an int64 value if the value is greater than 0
func (s *sparkTaskRenderer) setConfigInt64Value(config map[string]string, key string, value int64) {
	if value > 0 {
		config[key] = strconv.FormatInt(value, 10)
	}
}

// Configuration extraction methods

// applyPvcConfig preserves the existing OSS PVC behavior while allowing workflow labels
// to switch Spark local dirs from emptyDir to driver/executor OnDemand EBS PVCs.
// It mutates conf in place.
func (s *sparkTaskRenderer) applyPvcConfig(
	conf map[string]string,
	blockStorage *v1alpha1.BlockStorage,
	localDirPVCOptions *sparkLocalDirPVCOptions,
	localDirStorageClass string,
) {
	if localDirPVCOptions != nil && localDirPVCOptions.Enabled {
		delete(conf, "spark.kubernetes.driver.volumes.emptyDir.spark-local-dir-1.mount.path")
		delete(conf, "spark.kubernetes.driver.volumes.emptyDir.spark-local-dir-1.mount.readOnly")
		delete(conf, "spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.path")
		delete(conf, "spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.readOnly")
		delete(conf, "spark.local.dir")

		mergeInto(conf, buildSparkLocalDirPVCConfig(localDirPVCOptions, localDirStorageClass))
		return
	}
	if blockStorage == nil {
		return
	}
	mergeInto(conf, defaultSparkExecutorVolumes)
	s.setConfigValue(conf, "spark.local.dir", ExecutorLocalDir)
}

// applyHiveMetastoreConfig configures Hive Metastore settings including credentials
// and connection pooling. It mutates conf in place.
func (s *sparkTaskRenderer) applyHiveMetastoreConfig(conf map[string]string, hiveMetastoreSpec *v1alpha1.HiveMetastoreSpec, resolver CredentialResolver, profile cloudprofile.CloudProfile) {
	if hiveMetastoreSpec == nil {
		return
	}
	// Apply Hive defaults first; the specific overrides below then win.
	mergeInto(conf, defaultHiveCatalogConfig)
	if hiveMetastoreSpec.LibPath != "" {
		s.setConfigValue(conf, "spark.sql.hive.metastore.jars.path", hiveMetastoreSpec.LibPath)
	} else {
		s.setConfigValue(conf, "spark.sql.hive.metastore.jars.path", profile.SparkRuntime.HiveMetastoreLibPath)
	}
	if hiveMetastoreSpec.HiveHmsUri != "" {
		s.setConfigValue(conf, "spark.hadoop.hive.metastore.uris", hiveMetastoreSpec.HiveHmsUri)
	} else {
		// Set connection URL and library path
		s.setConfigValue(conf, "spark.hadoop.javax.jdo.option.ConnectionDriverName", "com.mysql.cj.jdbc.Driver")
		s.setConfigValue(conf, "spark.hadoop.javax.jdo.option.ConnectionURL", buildHiveMetastoreConnectionUrl(hiveMetastoreSpec))
		// Handle credentials (with error handling but continue on failure)
		if user, password, err := s.getHiveMetaCredentials(resolver, hiveMetastoreSpec); err == nil {
			logger.Info("Successfully retrieved Hive metastore credentials", "UserName", user)
			s.setConfigValue(conf, "spark.hadoop.javax.jdo.option.ConnectionUserName", user)
			s.setConfigValue(conf, "spark.hadoop.javax.jdo.option.ConnectionPassword", password)
		} else {
			logger.Error(err, "Failed to get Hive metastore credentials")
		}
		// Configure HikariCP connection pooling if specified
		s.configureHikariCP(conf, hiveMetastoreSpec.HikariCP)
	}
}

// configureHikariCP sets up HikariCP connection pool configuration
func (s *sparkTaskRenderer) configureHikariCP(config map[string]string, hikariCP *v1alpha1.HiveMetastoreHikariCP) {
	if hikariCP == nil {
		return
	}
	config["spark.hadoop.datanucleus.connectionPoolingType"] = "HikariCP"
	s.setConfigIntValue(config, "spark.hadoop.hive.metastore.hikari.maxPoolSize", hikariCP.MaxPoolSize)
	// Only set minPoolSize if it's valid (> 0 and <= maxPoolSize)
	if hikariCP.MinPoolSize > 0 && hikariCP.MinPoolSize <= hikariCP.MaxPoolSize {
		s.setConfigIntValue(config, "spark.hadoop.hive.metastore.hikari.minPoolSize", hikariCP.MinPoolSize)
	}
	s.setConfigInt64Value(config, "spark.hadoop.hive.metastore.hikari.maxLifetime", hikariCP.MaxLifetime)
	s.setConfigInt64Value(config, "spark.hadoop.hive.metastore.hikari.validationTimeout", hikariCP.ValidationTimeout)
}

// applyIcebergWarehouseConfig configures Iceberg catalog settings with warehouse
// path. It mutates conf in place.
func (s *sparkTaskRenderer) applyIcebergWarehouseConfig(conf map[string]string, icebergWarehouse string, hiveMetastoreSpec *v1alpha1.HiveMetastoreSpec) {
	// Iceberg requires both warehouse path and Hive metastore configuration
	if icebergWarehouse == "" || hiveMetastoreSpec == nil {
		return
	}
	mergeInto(conf, sparkIcebergCatalogConfig)
	s.setConfigValue(conf, "spark.sql.catalog.iceberg.warehouse", icebergWarehouse)
	s.setConfigValue(conf, "spark.sql.catalog.iceberg.uri", hiveMetastoreSpec.HiveHmsUri)
}

// applyPySparkConfig configures PySpark-specific settings (Python site-packages
// archive distribution, PYTHONPATH, Arrow optimization). It mutates conf in place.
func (s *sparkTaskRenderer) applyPySparkConfig(
	conf map[string]string,
	sparkExecutor *v1alpha1.SparkExecutorSpec,
) {
	// Only apply PySpark config if SparkAppType is Python
	if sparkExecutor.SparkAppType != v1alpha1.SparkAppTypePython {
		return
	}

	// Configure Python site-packages archive if specified
	if sparkExecutor.PySitePkgsArchive != "" {
		// Use spark.archives to distribute the site-packages tarball
		// The #pyspark_pkgs creates a symlink name for easy referencing
		conf["spark.archives"] = sparkExecutor.PySitePkgsArchive + "#pyspark_pkgs"

		// Add to PYTHONPATH for both driver and executors
		// The archive structure is: pyspark_pkgs.tar.gz -> pyspark_pkgs/pyspark_pkgs/
		pythonPath := "./pyspark_pkgs/pyspark_pkgs"
		conf["spark.kubernetes.driverEnv.PYTHONPATH"] = pythonPath
		conf["spark.kubernetes.executorEnv.PYTHONPATH"] = pythonPath

		logger.Info("PySpark site-packages configured",
			"archive", sparkExecutor.PySitePkgsArchive,
			"pythonPath", pythonPath)
	}

	// Enable Arrow optimization for PySpark data transfer
	// Only set if not already configured (allow user override)
	if _, ok := conf["spark.sql.execution.arrow.pyspark.enabled"]; !ok {
		conf["spark.sql.execution.arrow.pyspark.enabled"] = "true"
		conf["spark.sql.execution.arrow.pyspark.fallback.enabled"] = "true"
		logger.Info("PySpark Arrow optimization enabled")
	}
}
