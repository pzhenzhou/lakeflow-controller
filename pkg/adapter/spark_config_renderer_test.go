package adapter

import (
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestMergeAllFeatureConfigs_RefactoredBehavior(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name           string
		baseConfig     map[string]string
		sparkExecutor  v1alpha1.SparkExecutorSpec
		expectedKeys   []string
		expectedValues map[string]string
	}{
		{
			name: "emptyDir configuration with executor and driver volumes",
			baseConfig: map[string]string{
				"spark.app.name": "test-app",
			},
			sparkExecutor: v1alpha1.SparkExecutorSpec{
				BlockStorage: &v1alpha1.BlockStorage{Mode: "emptyDir"},
			},
			expectedKeys: []string{
				"spark.app.name",
				"spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.path",
				"spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.readOnly",
				"spark.local.dir",
			},
			expectedValues: map[string]string{
				"spark.app.name": "test-app",
				"spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.path":     ExecutorLocalDir,
				"spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.readOnly": "false",
				"spark.local.dir": ExecutorLocalDir,
			},
		},
		{
			name: "Hive metastore configuration",
			baseConfig: map[string]string{
				"spark.app.name": "test-app",
			},
			sparkExecutor: v1alpha1.SparkExecutorSpec{
				HiveMetastoreSpec: &v1alpha1.HiveMetastoreSpec{
					HiveMetastoreHost: "localhost",
					HiveMetastorePort: 3306,
					HiveMetastoreDB:   "hive_metastore",
					LibPath:           "/opt/hive/lib/*",
					UserName:          "hive",
					Password:          "password",
				},
			},
			expectedKeys: []string{
				"spark.app.name",
				"spark.sql.catalogImplementation",
				"spark.hadoop.javax.jdo.option.ConnectionURL",
				"spark.sql.hive.metastore.jars.path",
				"spark.hadoop.javax.jdo.option.ConnectionUserName",
				"spark.hadoop.javax.jdo.option.ConnectionPassword",
			},
			expectedValues: map[string]string{
				"spark.app.name":                                   "test-app",
				"spark.sql.catalogImplementation":                  "hive",
				"spark.sql.hive.metastore.jars.path":               "/opt/hive/lib/*",
				"spark.hadoop.javax.jdo.option.ConnectionUserName": "hive",
				"spark.hadoop.javax.jdo.option.ConnectionPassword": "password",
			},
		},
		{
			name: "Iceberg warehouse configuration",
			baseConfig: map[string]string{
				"spark.app.name": "test-app",
			},
			sparkExecutor: v1alpha1.SparkExecutorSpec{
				IcebergWarehouse: "s3a://my-bucket/warehouse",
				HiveMetastoreSpec: &v1alpha1.HiveMetastoreSpec{
					HiveHmsUri: "thrift://hms:9083",
				},
			},
			expectedKeys: []string{
				"spark.app.name",
				"spark.sql.extensions",
				"spark.sql.catalog.iceberg.warehouse",
				"spark.sql.catalog.iceberg.uri",
			},
			expectedValues: map[string]string{
				"spark.app.name":                      "test-app",
				"spark.sql.extensions":                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
				"spark.sql.catalog.iceberg.warehouse": "s3a://my-bucket/warehouse",
				"spark.sql.catalog.iceberg.uri":       "thrift://hms:9083",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A real resolver is required for the inline Hive credentials case
			// (the inline path resolves offline without touching the cluster), so
			// ConnectionUserName/Password stay populated exactly as before.
			builder.applyFeatureConfigs(tt.baseConfig, tt.sparkExecutor, nil, newK8sCredentialResolver(""), testAliyunProfile())
			result := tt.baseConfig

			// Check that all expected keys are present
			for _, key := range tt.expectedKeys {
				assert.Contains(t, result, key, "Expected key %s to be present in result", key)
			}

			// Check that expected values match
			for key, expectedValue := range tt.expectedValues {
				assert.Equal(t, expectedValue, result[key], "Expected value for key %s", key)
			}

			// Verify extra classpath is set
			assert.Contains(t, result, "spark.driver.extraClassPath")
			assert.Contains(t, result, "spark.executor.extraClassPath")
		})
	}
}

func TestHelperMethods(t *testing.T) {
	builder := &sparkTaskRenderer{}

	t.Run("mergeInto", func(t *testing.T) {
		defaultConfig := map[string]string{
			"key1": "default1",
			"key2": "default2",
		}
		customConfig := map[string]string{
			"key2": "custom2",
			"key3": "custom3",
		}

		// Clone-once, mutate-in-place: start from a copy of defaults, then merge
		// the custom map over it (custom wins on collisions).
		result := mergeInto(map[string]string{}, defaultConfig)
		result = mergeInto(result, customConfig)

		expected := map[string]string{
			"key1": "default1",
			"key2": "custom2", // custom overrides default
			"key3": "custom3",
		}

		assert.Equal(t, expected, result)
	})

	t.Run("setConfigValue", func(t *testing.T) {
		config := make(map[string]string)

		builder.setConfigValue(config, "key1", "value1")
		builder.setConfigValue(config, "key2", "") // empty value should not be set

		assert.Equal(t, "value1", config["key1"])
		assert.NotContains(t, config, "key2")
	})

	t.Run("setConfigIntValue", func(t *testing.T) {
		config := make(map[string]string)

		builder.setConfigIntValue(config, "key1", 42)
		builder.setConfigIntValue(config, "key2", 0) // zero value should not be set

		assert.Equal(t, "42", config["key1"])
		assert.NotContains(t, config, "key2")
	})
}

func TestUserSparkConfigPrecedence(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name          string
		baseConfig    map[string]string
		sparkExecutor v1alpha1.SparkExecutorSpec
		expectedValue map[string]string
		description   string
	}{
		{
			name: "User SparkConfig overrides Iceberg catalog config",
			baseConfig: map[string]string{
				"spark.app.name": "test-app",
			},
			sparkExecutor: v1alpha1.SparkExecutorSpec{
				IcebergWarehouse: "oss://default-warehouse/iceberg",
				HiveMetastoreSpec: &v1alpha1.HiveMetastoreSpec{
					HiveHmsUri: "thrift://hms:9083",
				},
				SparkConfig: map[string]string{
					"spark.sql.catalog.iceberg.warehouse": "oss://user-custom-warehouse/iceberg",
					"spark.sql.catalog.iceberg.type":      "hadoop",
				},
			},
			expectedValue: map[string]string{
				"spark.sql.catalog.iceberg.warehouse": "oss://user-custom-warehouse/iceberg",
				"spark.sql.catalog.iceberg.type":      "hadoop", // user override
			},
			description: "User's sparkConfig should override Iceberg warehouse configuration",
		},
		{
			name: "User SparkConfig overrides Hive metastore config",
			baseConfig: map[string]string{
				"spark.app.name": "test-app",
			},
			sparkExecutor: v1alpha1.SparkExecutorSpec{
				HiveMetastoreSpec: &v1alpha1.HiveMetastoreSpec{
					HiveHmsUri: "thrift://hms:9083",
				},
				SparkConfig: map[string]string{
					"spark.sql.catalogImplementation":  "in-memory",
					"spark.sql.hive.metastore.version": "custom-version",
				},
			},
			expectedValue: map[string]string{
				"spark.sql.catalogImplementation":  "in-memory",      // user override
				"spark.sql.hive.metastore.version": "custom-version", // user override
			},
			description: "User's sparkConfig should override Hive metastore configuration",
		},
		{
			name: "User SparkConfig overrides SQL legacy settings",
			baseConfig: map[string]string{
				"spark.app.name": "test-app",
			},
			sparkExecutor: v1alpha1.SparkExecutorSpec{
				SparkConfig: map[string]string{
					"spark.sql.legacy.allowOverwriteTableBeingReadFrom": "true",
					"spark.sql.storeAssignmentPolicy":                   "LEGACY",
				},
			},
			expectedValue: map[string]string{
				"spark.sql.legacy.allowOverwriteTableBeingReadFrom": "true",
				"spark.sql.storeAssignmentPolicy":                   "LEGACY",
			},
			description: "User's sparkConfig SQL settings should be preserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A real resolver is required for the inline Hive credentials case
			// (the inline path resolves offline without touching the cluster), so
			// ConnectionUserName/Password stay populated exactly as before.
			builder.applyFeatureConfigs(tt.baseConfig, tt.sparkExecutor, nil, newK8sCredentialResolver(""), testAliyunProfile())
			result := tt.baseConfig

			// Apply user SparkConfig last (simulating what setSparkSpecConfig does)
			if tt.sparkExecutor.SparkConfig != nil {
				for k, v := range tt.sparkExecutor.SparkConfig {
					result[k] = v
				}
			}

			// Verify that user's settings are preserved
			for key, expectedValue := range tt.expectedValue {
				assert.Equal(t, expectedValue, result[key],
					"%s: Expected user's value for key %s to be preserved", tt.description, key)
			}
		})
	}
}

// Test_setPySparkConfig tests PySpark-specific Spark configuration
func Test_setPySparkConfig(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name            string
		sparkExecutor   *v1alpha1.SparkExecutorSpec
		baseConfig      map[string]string
		expectedKeys    []string
		expectedValues  map[string]string
		notExpectedKeys []string
	}{
		{
			name: "Java application should not add PySpark configs",
			sparkExecutor: &v1alpha1.SparkExecutorSpec{
				SparkAppType:      v1alpha1.SparkAppTypeJava,
				PySitePkgsArchive: "local:///mnt/spark/pyspark_sitepkgs.tar.gz",
			},
			baseConfig: map[string]string{
				"spark.app.name": "test-java-app",
			},
			expectedKeys: []string{"spark.app.name"},
			expectedValues: map[string]string{
				"spark.app.name": "test-java-app",
			},
			notExpectedKeys: []string{
				"spark.archives",
				"spark.kubernetes.driverEnv.PYTHONPATH",
				"spark.kubernetes.executorEnv.PYTHONPATH",
				"spark.sql.execution.arrow.pyspark.enabled",
			},
		},
		{
			name: "Python application without site-packages should only add Arrow config",
			sparkExecutor: &v1alpha1.SparkExecutorSpec{
				SparkAppType:      v1alpha1.SparkAppTypePython,
				PySitePkgsArchive: "",
			},
			baseConfig: map[string]string{
				"spark.app.name": "test-python-app",
			},
			expectedKeys: []string{
				"spark.app.name",
				"spark.sql.execution.arrow.pyspark.enabled",
				"spark.sql.execution.arrow.pyspark.fallback.enabled",
			},
			expectedValues: map[string]string{
				"spark.app.name": "test-python-app",
				"spark.sql.execution.arrow.pyspark.enabled":          "true",
				"spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
			},
			notExpectedKeys: []string{
				"spark.archives",
				"spark.kubernetes.driverEnv.PYTHONPATH",
				"spark.kubernetes.executorEnv.PYTHONPATH",
			},
		},
		{
			name: "Python application with site-packages should configure all PySpark settings",
			sparkExecutor: &v1alpha1.SparkExecutorSpec{
				SparkAppType:      v1alpha1.SparkAppTypePython,
				PySitePkgsArchive: "local:///mnt/spark/pyspark_sitepkgs.tar.gz",
			},
			baseConfig: map[string]string{
				"spark.app.name": "test-python-app",
			},
			expectedKeys: []string{
				"spark.app.name",
				"spark.archives",
				"spark.kubernetes.driverEnv.PYTHONPATH",
				"spark.kubernetes.executorEnv.PYTHONPATH",
				"spark.sql.execution.arrow.pyspark.enabled",
				"spark.sql.execution.arrow.pyspark.fallback.enabled",
			},
			expectedValues: map[string]string{
				"spark.app.name":                                     "test-python-app",
				"spark.archives":                                     "local:///mnt/spark/pyspark_sitepkgs.tar.gz#pyspark_pkgs",
				"spark.kubernetes.driverEnv.PYTHONPATH":              "./pyspark_pkgs/pyspark_pkgs",
				"spark.kubernetes.executorEnv.PYTHONPATH":            "./pyspark_pkgs/pyspark_pkgs",
				"spark.sql.execution.arrow.pyspark.enabled":          "true",
				"spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
			},
		},
		{
			name: "Python application should respect pre-existing Arrow config (user override)",
			sparkExecutor: &v1alpha1.SparkExecutorSpec{
				SparkAppType:      v1alpha1.SparkAppTypePython,
				PySitePkgsArchive: "",
			},
			baseConfig: map[string]string{
				"spark.app.name": "test-python-app",
				"spark.sql.execution.arrow.pyspark.enabled": "false", // User override
			},
			expectedKeys: []string{
				"spark.app.name",
				"spark.sql.execution.arrow.pyspark.enabled",
			},
			expectedValues: map[string]string{
				"spark.app.name": "test-python-app",
				"spark.sql.execution.arrow.pyspark.enabled": "false", // Should remain false (user override preserved)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder.applyPySparkConfig(tt.baseConfig, tt.sparkExecutor)
			result := tt.baseConfig

			// Check expected keys exist with correct values
			for _, key := range tt.expectedKeys {
				if _, ok := result[key]; !ok {
					t.Errorf("Expected key %s to be present in result", key)
				}
			}

			// Check expected values
			for key, expectedValue := range tt.expectedValues {
				if gotValue, ok := result[key]; !ok {
					t.Errorf("Expected key %s to be present in result", key)
				} else if gotValue != expectedValue {
					t.Errorf("For key %s, expected value %s, got %s", key, expectedValue, gotValue)
				}
			}

			// Check that unwanted keys are not present
			for _, key := range tt.notExpectedKeys {
				if _, ok := result[key]; ok {
					t.Errorf("Did not expect key %s to be present in result", key)
				}
			}
		})
	}
}
