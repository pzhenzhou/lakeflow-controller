package adapter

import "github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"

const (
	ExecutorUserUID              = 185
	ExecutorMemoryOverheadFactor = 0.2
	MinMemoryOverhead            = 400 // Minimum memory overhead in MB
	GCOpts                       = " -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=45 -XX:+ParallelRefProcEnabled -XX:+AlwaysPreTouch"
	DriverJavaOpts               = "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Dlog4j2.formatMsgNoLookups=true" + GCOpts
	ExecutorJavaOpts             = "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED" + GCOpts

	ExecutorLocalDir        = "/executor-local-dir"
	DriverLocalDir          = "/driver-local-dir"
	SparkExecutorMountPath  = "/mnt/spark"
	SparkExecutorVolumeName = "spark-on-k8s-oss-volume"
	ForceJava17Opts         = "-Djava.version=17.0.15 -Djava.specification.version=17"
	DefaultSparkVersion     = "3.5.6"
)

var (
	defaultHiveCatalogConfig = map[string]string{
		// Hive Metastore Configuration
		"spark.sql.catalogImplementation":                 "hive",
		"spark.sql.hive.metastore.version":                "3.1.3",
		"spark.hadoop.hive.metastore.schema.verification": "false",
		"spark.sql.hive.metastore.jars":                   "path",
	}

	sparkIcebergCatalogConfig = map[string]string{
		"spark.sql.extensions":           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
		"spark.sql.catalog.iceberg":      "org.apache.iceberg.spark.SparkCatalog",
		"spark.sql.catalog.iceberg.type": "hive",
	}

	defaultSparkExecutorVolumes = map[string]string{
		"spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.path":     ExecutorLocalDir,
		"spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.readOnly": "false",
	}
)

func buildDefaultSparkSpecConfig(profile cloudprofile.CloudProfile) map[string]string {
	conf := map[string]string{
		// Java 17 Configuration (using constants)
		"spark.driver.defaultJavaOptions":   ForceJava17Opts,
		"spark.executor.defaultJavaOptions": ForceJava17Opts,
		// Java 17 Module System Compatibility (using constants)
		"spark.driver.extraJavaOptions":   DriverJavaOpts,
		"spark.executor.extraJavaOptions": ExecutorJavaOpts,
		// Basic Spark configuration
		"spark.sql.streaming.ui.enabled":      "false",
		"spark.sql.adaptive.skewJoin.enabled": "true",
		"spark.ui.enabled":                    "true",
		"spark.ui.showConsoleProgress":        "true",
		"spark.jars.excludes":                 "org.slf4j:slf4j-log4j12,log4j:log4j",
		// Driver emptyDir defaults for scratch storage — ephemeral, no StorageClass needed.
		"spark.kubernetes.driver.volumes.emptyDir.spark-local-dir-1.mount.path":     DriverLocalDir,
		"spark.kubernetes.driver.volumes.emptyDir.spark-local-dir-1.mount.readOnly": "false",
	}

	if profile.ObjectStorage.SparkConfPrefix != "" {
		if endpoint := profile.ObjectStorage.DefaultEndpoint; endpoint != "" {
			conf[profile.ObjectStorage.SparkConfPrefix+".endpoint"] = endpoint
		}
		if impl := profile.ObjectStorage.FilesystemImpl; impl != "" {
			conf[profile.ObjectStorage.SparkConfPrefix+".impl"] = impl
		}
	}
	if scheme := profile.ObjectStorage.FilesystemScheme; scheme != "" && profile.ObjectStorage.AbstractFilesystemImpl != "" {
		conf["spark.hadoop.fs.AbstractFileSystem."+scheme+".impl"] = profile.ObjectStorage.AbstractFilesystemImpl
	}
	return conf
}

// buildDefaultExtraClassPath assembles the operator's base Spark extra classpath
// from the cloud profile: vendor object-storage jars (e.g. Jindo SDK, hadoop-aws)
// followed by the Spark runtime jars (Iceberg, hadoop-lzo, MySQL connector). All
// paths come from the profile so they are configurable per environment rather than
// hardcoded; empty entries are skipped.
func buildDefaultExtraClassPath(profile cloudprofile.CloudProfile) []string {
	rt := profile.SparkRuntime
	classpath := make([]string, 0, len(profile.ObjectStorage.ExtraClasspath)+len(rt.IcebergRuntimeJars)+2)
	classpath = append(classpath, profile.ObjectStorage.ExtraClasspath...)
	classpath = append(classpath, rt.IcebergRuntimeJars...)
	if rt.LzoJar != "" {
		classpath = append(classpath, rt.LzoJar)
	}
	if rt.MysqlConnectorJar != "" {
		classpath = append(classpath, rt.MysqlConnectorJar)
	}
	return classpath
}
