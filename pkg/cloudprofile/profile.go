package cloudprofile

import (
	"fmt"
	"os"
	"strings"

	storagev1 "k8s.io/api/storage/v1"
)

const (
	ProviderAliyun = "aliyun"
	ProviderAWS    = "aws"

	ParamSourceLiteral         = "literal"
	ParamSourceBucket          = "bucket"
	ParamSourcePath            = "path"
	ParamSourceEndpoint        = "endpoint"
	ParamSourceRegion          = "region"
	ParamSourceSecretName      = "secretName"
	ParamSourceSecretNamespace = "secretNamespace"
)

// CloudProfile is operator-level cloud configuration. It is not a Kubernetes
// API type; it is selected once at process startup from built-in profiles.
type CloudProfile struct {
	Name          string
	ObjectStorage ObjectStorageProfile
	BlockStorage  BlockStorageProfile
	Auth          AuthProfile
	SparkRuntime  SparkRuntimeProfile
}

// SparkRuntimeProfile describes Spark image/distribution jar locations. These are
// independent of the cloud vendor but specific to the production Spark image and
// component versions, so they live in the operator profile (configurable per
// environment) rather than as hardcoded constants. Per-workflow user jars (UDF,
// arbitrary extra jars) are configured on the CRD SparkExecutorSpec instead.
//
// Every field can be overridden at process startup via environment variables; see
// applySparkRuntimeEnvOverrides.
type SparkRuntimeProfile struct {
	// IcebergRuntimeJars are appended to the Spark driver/executor extra classpath
	// (iceberg-spark-runtime + iceberg-hive-metastore). Env: SPARK_ICEBERG_RUNTIME_JARS.
	IcebergRuntimeJars []string
	// LzoJar is the hadoop-lzo native codec jar, appended to the extra classpath.
	// Env: SPARK_LZO_JAR.
	LzoJar string
	// MysqlConnectorJar is the MySQL JDBC driver jar (Hive metastore / JDBC sources),
	// appended to the extra classpath. Env: SPARK_MYSQL_CONNECTOR_JAR.
	MysqlConnectorJar string
	// HiveMetastoreLibPath is the default value for spark.sql.hive.metastore.jars.path
	// when a task does not set HiveMetastoreSpec.LibPath. Env: SPARK_HIVE_METASTORE_LIB_PATH.
	HiveMetastoreLibPath string
	// DefaultMainApplicationFile is used when a SparkExecutorSpec leaves
	// MainApplicationFile empty. Env: SPARK_DEFAULT_MAIN_APPLICATION_FILE.
	DefaultMainApplicationFile string
	// UDFJars is the operator default set of UDF jars, used when a workflow does not
	// set its own SparkExecutorSpec.UDFJars. Env: SPARK_UDF_JARS.
	UDFJars []string
}

type ObjectStorageProfile struct {
	CSIDriver                  string
	ReclaimPolicy              string
	VolumeBindingMode          storagev1.VolumeBindingMode
	DefaultStorageClass        string
	DefaultEndpoint            string
	DefaultRegion              string
	FilesystemScheme           string
	FilesystemImpl             string
	AbstractFilesystemImpl     string
	SparkConfPrefix            string
	ExtraClasspath             []string
	StorageClassParameterRules []StorageClassParameterRule
}

type BlockStorageProfile struct {
	CSIDriver           string
	ReclaimPolicy       string
	VolumeBindingMode   storagev1.VolumeBindingMode
	DefaultStorageClass string
	Params              map[string]string
}

type AuthProfile struct {
	DefaultMode       string
	RoleAnnotationKey string
}

type StorageClassParameterRule struct {
	Key     string
	Source  string
	Literal string
}

// LoadFromEnv selects a built-in profile from CLOUD_PROVIDER. Empty means
// Aliyun for backward compatibility.
func LoadFromEnv() (CloudProfile, error) {
	return Builtin(strings.TrimSpace(os.Getenv("CLOUD_PROVIDER")))
}

// Builtin returns a named built-in profile. The returned value is safe for
// callers to mutate without affecting future Builtin calls.
func Builtin(name string) (CloudProfile, error) {
	if name == "" {
		name = ProviderAliyun
	}
	var profile CloudProfile
	switch strings.ToLower(name) {
	case ProviderAliyun:
		profile = cloneProfile(aliyunProfile())
	case ProviderAWS:
		profile = cloneProfile(awsProfile())
	default:
		return CloudProfile{}, fmt.Errorf("unknown cloud provider %q", name)
	}
	applySparkRuntimeEnvOverrides(&profile.SparkRuntime)
	return profile, nil
}

func cloneProfile(profile CloudProfile) CloudProfile {
	profile.ObjectStorage.ExtraClasspath = cloneStringSlice(profile.ObjectStorage.ExtraClasspath)
	profile.ObjectStorage.StorageClassParameterRules = cloneParameterRules(profile.ObjectStorage.StorageClassParameterRules)
	profile.BlockStorage.Params = cloneStringMap(profile.BlockStorage.Params)
	profile.SparkRuntime.IcebergRuntimeJars = cloneStringSlice(profile.SparkRuntime.IcebergRuntimeJars)
	profile.SparkRuntime.UDFJars = cloneStringSlice(profile.SparkRuntime.UDFJars)
	return profile
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneStringSlice(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func cloneParameterRules(src []StorageClassParameterRule) []StorageClassParameterRule {
	if src == nil {
		return nil
	}
	dst := make([]StorageClassParameterRule, len(src))
	copy(dst, src)
	return dst
}
