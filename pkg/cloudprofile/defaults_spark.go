package cloudprofile

import (
	"os"
	"strings"
)

// Environment variables that override the built-in SparkRuntimeProfile defaults at
// process startup. List-valued variables are comma-separated; blank entries are
// dropped. Empty/unset variables leave the built-in default untouched.
const (
	EnvSparkIcebergRuntimeJars   = "SPARK_ICEBERG_RUNTIME_JARS"
	EnvSparkLzoJar               = "SPARK_LZO_JAR"
	EnvSparkMysqlConnectorJar    = "SPARK_MYSQL_CONNECTOR_JAR"
	EnvSparkHiveMetastoreLibPath = "SPARK_HIVE_METASTORE_LIB_PATH"
	EnvSparkDefaultMainAppFile   = "SPARK_DEFAULT_MAIN_APPLICATION_FILE"
	EnvSparkUDFJars              = "SPARK_UDF_JARS"
)

// defaultSparkRuntimeProfile returns the built-in Spark image/distribution jar
// locations. They are shared across cloud providers because they depend on the
// Spark image build (component versions), not the cloud vendor. Override per
// environment via the SPARK_* environment variables.
func defaultSparkRuntimeProfile() SparkRuntimeProfile {
	return SparkRuntimeProfile{
		IcebergRuntimeJars: []string{
			"/mnt/spark/jars/iceberg-spark-runtime/iceberg-spark-runtime-3.5_2.12-1.7.2.jar",
			"/mnt/spark/jars/iceberg-spark-runtime/iceberg-hive-metastore-1.7.2.jar",
		},
		LzoJar:                     "/mnt/spark/jars/lzo-lib/hadoop-lzo.jar",
		MysqlConnectorJar:          "/mnt/spark/jars/mysql_8.0/mysql-connector-j-8.0.33.jar",
		HiveMetastoreLibPath:       "/mnt/spark/jars/hive-3.1.3-lib/*.jar,/mnt/spark/jars/lzo-lib/hadoop-lzo.jar",
		DefaultMainApplicationFile: "local:///mnt/spark/jars/lakeflow-spark-executor/lakeflow-spark-executor.jar",
		UDFJars: []string{
			"/mnt/spark/jars/udf/udf.jar",
			"/mnt/spark/jars/udf/gzip_decode.jar",
			"/mnt/spark/jars/udf/custom-udaf.jar",
			"/mnt/spark/jars/udf/bitmap-udf.jar",
			"/mnt/spark/jars/udf/md5.jar",
		},
	}
}

// applySparkRuntimeEnvOverrides replaces any SparkRuntimeProfile field whose
// corresponding environment variable is set. It is applied after the built-in
// profile is selected so production can tune jar locations without rebuilding.
func applySparkRuntimeEnvOverrides(rt *SparkRuntimeProfile) {
	if v := envList(EnvSparkIcebergRuntimeJars); v != nil {
		rt.IcebergRuntimeJars = v
	}
	if v := envList(EnvSparkUDFJars); v != nil {
		rt.UDFJars = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvSparkLzoJar)); v != "" {
		rt.LzoJar = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvSparkMysqlConnectorJar)); v != "" {
		rt.MysqlConnectorJar = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvSparkHiveMetastoreLibPath)); v != "" {
		rt.HiveMetastoreLibPath = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvSparkDefaultMainAppFile)); v != "" {
		rt.DefaultMainApplicationFile = v
	}
}

// envList parses a comma-separated environment variable into a trimmed,
// non-empty slice. Returns nil when the variable is unset or yields no entries,
// so callers keep the built-in default.
func envList(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
