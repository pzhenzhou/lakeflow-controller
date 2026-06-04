package adapter

import (
	"fmt"
	"testing"

	sparkv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Test_memoryOverhead tests the memoryOverhead function with various memory units and factors.
// Supported memory units by Kubernetes resource.ParseQuantity:
// - Binary units (base 1024): Ki, Mi, Gi, Ti, Pi, Ei (uppercase only)
// - Decimal units (base 1000): k, M, G, T, P, E (case-sensitive, uppercase for M/G/T/P/E)
// The function converts memory to Spark format using lowercase 'm' suffix (e.g., "400m")
func Test_memoryOverhead(t *testing.T) {
	tests := []struct {
		name           string
		executorMemory string
		factor         float64
		want           string
		wantEmpty      bool
	}{
		// Test with lowercase 'm' (mebibytes) - Kubernetes style
		{
			name:           "1Gi memory with 0.2 factor should calculate overhead correctly",
			executorMemory: "1Gi",
			factor:         0.2,
			// 1Gi = 1024Mi = 1073741824 bytes
			// 0.2 * 1073741824 = 214748364.8 bytes = 204.8 MB ≈ 204 MB
			// max(204, 400) = 400 MB
			want: "400m",
		},
		{
			name:           "2Gi memory with 0.2 factor should calculate overhead correctly",
			executorMemory: "2Gi",
			factor:         0.2,
			// 2Gi = 2147483648 bytes
			// 0.2 * 2147483648 = 429496729.6 bytes = 409.6 MB
			// 429496729.6 / (1024*1024) = 409.6 MB, integer division = 409 MB
			// max(409, 400) = 409 MB
			want: "409m",
		},
		{
			name:           "4096Mi memory with 0.2 factor should calculate overhead correctly",
			executorMemory: "4096Mi",
			factor:         0.2,
			// 4096Mi = 4294967296 bytes
			// 0.2 * 4294967296 = 858993459.2 bytes
			// 858993459.2 / (1024*1024) = 819.2 MB, integer division = 819 MB
			// max(819, 400) = 819 MB
			want: "819m",
		},
		{
			name:           "512Mi memory with 0.2 factor should use minimum overhead",
			executorMemory: "512Mi",
			factor:         0.2,
			// 512Mi = 536870912 bytes
			// 0.2 * 536870912 = 107374182.4 bytes = 102.4 MB ≈ 103 MB
			// max(103, 400) = 400 MB (minimum)
			want: "400m",
		},
		{
			name:           "Test with uppercase G (decimal gigabytes)",
			executorMemory: "2G",
			factor:         0.2,
			// 2G = 2000000000 bytes (decimal)
			// 0.2 * 2000000000 = 400000000 bytes = 381.47 MB ≈ 382 MB
			// max(382, 400) = 400 MB (minimum)
			want: "400m",
		},
		{
			name:           "Test with uppercase M (decimal megabytes)",
			executorMemory: "2048M",
			factor:         0.2,
			// 2048M = 2048000000 bytes (decimal)
			// 0.2 * 2048000000 = 409600000 bytes = 390.625 MB ≈ 391 MB
			// max(391, 400) = 400 MB (minimum)
			want: "400m",
		},
		{
			name:           "Test with uppercase Gi (binary gigabytes)",
			executorMemory: "3Gi",
			factor:         0.2,
			// 3Gi = 3221225472 bytes
			// 0.2 * 3221225472 = 644245094.4 bytes
			// 644245094.4 / (1024*1024) = 614.4 MB, integer division = 614 MB
			// max(614, 400) = 614 MB
			want: "614m",
		},
		{
			name:           "Test with uppercase Mi (binary megabytes)",
			executorMemory: "5000Mi",
			factor:         0.2,
			// 5000Mi = 5242880000 bytes
			// 0.2 * 5242880000 = 1048576000 bytes
			// 1048576000 / (1024*1024) = 1000 MB
			// max(1000, 400) = 1000 MB
			want: "1000m",
		},
		{
			name:           "Test with lowercase g (decimal gigabytes)",
			executorMemory: "3g",
			factor:         0.2,
			// Note: Kubernetes resource.ParseQuantity only supports uppercase G for decimal units
			// This test verifies the error case is handled
			want:      "",
			wantEmpty: true,
		},
		{
			name:           "Test with 0.1 factor",
			executorMemory: "8Gi",
			factor:         0.1,
			// 8Gi = 8589934592 bytes
			// 0.1 * 8589934592 = 858993459.2 bytes
			// 858993459.2 / (1024*1024) = 819.2 MB, integer division = 819 MB
			// max(819, 400) = 819 MB
			want: "819m",
		},
		{
			name:           "Test with 0.3 factor",
			executorMemory: "4Gi",
			factor:         0.3,
			// 4Gi = 4294967296 bytes
			// 0.3 * 4294967296 = 1288490188.8 bytes
			// 1288490188.8 / (1024*1024) = 1228.8 MB, integer division = 1228 MB
			// max(1228, 400) = 1228 MB
			want: "1228m",
		},
		{
			name:           "Invalid memory format should return empty",
			executorMemory: "invalid",
			factor:         0.2,
			want:           "",
			wantEmpty:      true,
		},
		{
			name:           "Empty memory string should return empty",
			executorMemory: "",
			factor:         0.2,
			want:           "",
			wantEmpty:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := memoryOverhead(tt.executorMemory, tt.factor)
			if tt.wantEmpty {
				if got != "" {
					t.Errorf("memoryOverhead() = %v, want empty string", got)
				}
				return
			}
			if got != tt.want {
				t.Errorf("memoryOverhead() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test_memoryOverhead_EdgeCases tests edge cases and boundary conditions
func Test_memoryOverhead_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		executorMemory string
		factor         float64
		description    string
	}{
		{
			name:           "Very small memory should use minimum",
			executorMemory: "100Mi",
			factor:         0.2,
			description:    "Should return 400m (minimum)",
		},
		{
			name:           "Zero factor should still use minimum",
			executorMemory: "2Gi",
			factor:         0.0,
			description:    "Should return 400m (minimum)",
		},
		{
			name:           "Large memory with high factor",
			executorMemory: "32Gi",
			factor:         0.3,
			description:    "Should calculate large overhead correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := memoryOverhead(tt.executorMemory, tt.factor)
			if got == "" {
				t.Errorf("memoryOverhead() returned empty string, description: %s", tt.description)
			}
			// Verify result is in correct Spark format (ends with 'm')
			if len(got) < 2 || got[len(got)-1] != 'm' {
				t.Errorf("memoryOverhead() = %v, should end with 'm', description: %s", got, tt.description)
			}
			t.Logf("%s: executorMemory=%s, factor=%.2f, result=%s", tt.name, tt.executorMemory, tt.factor, got)
		})
	}
}

// Test_memoryOverhead_MinimumEnforcement specifically tests minimum overhead enforcement
func Test_memoryOverhead_MinimumEnforcement(t *testing.T) {
	testCases := []string{"100Mi", "200Mi", "300Mi", "400Mi", "500Mi"}

	for _, memory := range testCases {
		t.Run("minimum_with_"+memory, func(t *testing.T) {
			got := memoryOverhead(memory, 0.2)
			if got == "" {
				t.Errorf("memoryOverhead(%s, 0.2) returned empty string", memory)
				return
			}

			// Parse the result to check it meets minimum
			var resultMB int
			_, err := fmt.Sscanf(got, "%dm", &resultMB)
			if err != nil {
				t.Errorf("Failed to parse result %s: %v", got, err)
				return
			}

			if resultMB < MinMemoryOverhead {
				t.Errorf("memoryOverhead(%s, 0.2) = %s (%dMB), should be at least %dMB",
					memory, got, resultMB, MinMemoryOverhead)
			}
			t.Logf("✓ %s with 0.2 factor => %s (minimum %dMB enforced)", memory, got, MinMemoryOverhead)
		})
	}
}

// Test_getSparkApplicationType tests the Spark application type resolution with backward compatibility
func Test_getSparkApplicationType(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name     string
		spec     *v1alpha1.SparkExecutorSpec
		expected v1alpha1.SparkApplicationType
	}{
		{
			name: "Default to Java when SparkAppType is empty (backward compatibility)",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: "",
			},
			expected: v1alpha1.SparkAppTypeJava,
		},
		{
			name: "Explicit Java type",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypeJava,
			},
			expected: v1alpha1.SparkAppTypeJava,
		},
		{
			name: "Explicit Python type",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypePython,
			},
			expected: v1alpha1.SparkAppTypePython,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := builder.getSparkApplicationType(tt.spec)
			if got != tt.expected {
				t.Errorf("getSparkApplicationType() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// Test_setPySparkDependencies tests the PySpark dependencies configuration
func Test_setPySparkDependencies(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name        string
		spec        *v1alpha1.SparkExecutorSpec
		expectDeps  bool
		expectFiles []string
	}{
		{
			name: "Java application should not set PyFiles",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypeJava,
				PyFiles:      []string{"should", "be", "ignored"},
			},
			expectDeps:  false,
			expectFiles: nil,
		},
		{
			name: "Python application with no PyFiles",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypePython,
				PyFiles:      nil,
			},
			expectDeps:  false,
			expectFiles: nil,
		},
		{
			name: "Python application with single PyFile",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypePython,
				PyFiles: []string{
					"local:///mnt/spark/myproject.zip",
				},
			},
			expectDeps: true,
			expectFiles: []string{
				"local:///mnt/spark/myproject.zip",
			},
		},
		{
			name: "Python application with multiple PyFiles",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypePython,
				PyFiles: []string{
					"local:///mnt/spark/project1.zip",
					"local:///mnt/spark/project2.zip",
					"local:///mnt/spark/utils.py",
				},
			},
			expectDeps: true,
			expectFiles: []string{
				"local:///mnt/spark/project1.zip",
				"local:///mnt/spark/project2.zip",
				"local:///mnt/spark/utils.py",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a minimal SparkApplication
			sparkApp := &sparkv1beta2.SparkApplication{
				Spec: sparkv1beta2.SparkApplicationSpec{
					Deps: sparkv1beta2.Dependencies{}, // Initialize Deps struct
				},
			}

			builder.setPySparkDependencies(sparkApp, tt.spec)

			if tt.expectDeps {
				if len(sparkApp.Spec.Deps.PyFiles) != len(tt.expectFiles) {
					t.Errorf("Expected %d PyFiles, got %d", len(tt.expectFiles), len(sparkApp.Spec.Deps.PyFiles))
					return
				}
				for i, expected := range tt.expectFiles {
					if sparkApp.Spec.Deps.PyFiles[i] != expected {
						t.Errorf("PyFiles[%d] = %v, want %v", i, sparkApp.Spec.Deps.PyFiles[i], expected)
					}
				}
			} else {
				if len(sparkApp.Spec.Deps.PyFiles) > 0 {
					t.Errorf("Expected no PyFiles, but got %v", sparkApp.Spec.Deps.PyFiles)
				}
			}
		})
	}
}

// Test_buildSparkApplicationFromSpec_PySparkIntegration tests the full integration of PySpark features
// in the SparkApplication building process, including type resolution, PyFiles, and configs
func Test_buildSparkApplicationFromSpec_PySparkIntegration(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name              string
		sparkExecutorSpec *v1alpha1.SparkExecutorSpec
		expectedType      string
		expectedMainFile  string
		expectedPyFiles   []string
		shouldHavePyFiles bool
		description       string
	}{
		{
			name: "Java Spark application (backward compatibility - no SparkAppType specified)",
			sparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
				SparkVersion:        "3.5.6",
				MainClass:           "com.example.JavaApp",
				MainApplicationFile: "local:///mnt/spark/jars/app.jar",
				QueueName:           "default",
				DriverResource: v1alpha1.SparkResource{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
				ExecutorResource: v1alpha1.SparkResource{
					Replicas: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
			expectedType:      "Java",
			expectedMainFile:  "local:///mnt/spark/jars/app.jar",
			shouldHavePyFiles: false,
			description:       "Java application without explicit SparkAppType should default to Java",
		},
		{
			name: "Java Spark application (explicit Java type)",
			sparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
				SparkAppType:        v1alpha1.SparkAppTypeJava,
				SparkVersion:        "3.5.6",
				MainClass:           "com.example.JavaApp",
				MainApplicationFile: "local:///mnt/spark/jars/app.jar",
				QueueName:           "default",
				DriverResource: v1alpha1.SparkResource{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
				ExecutorResource: v1alpha1.SparkResource{
					Replicas: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
			expectedType:      "Java",
			expectedMainFile:  "local:///mnt/spark/jars/app.jar",
			shouldHavePyFiles: false,
			description:       "Explicit Java type should work correctly",
		},
		{
			name: "PySpark application with PyFiles",
			sparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
				SparkAppType:        v1alpha1.SparkAppTypePython,
				SparkVersion:        "3.5.6",
				MainClass:           "", // Not used for Python
				MainApplicationFile: "local:///mnt/spark/main.py",
				PyFiles: []string{
					"local:///mnt/spark/mypyproject.zip",
					"local:///mnt/spark/utils.py",
				},
				QueueName: "default",
				DriverResource: v1alpha1.SparkResource{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
				ExecutorResource: v1alpha1.SparkResource{
					Replicas: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
			expectedType:     "Python",
			expectedMainFile: "local:///mnt/spark/main.py",
			expectedPyFiles: []string{
				"local:///mnt/spark/mypyproject.zip",
				"local:///mnt/spark/utils.py",
			},
			shouldHavePyFiles: true,
			description:       "Python application should set type and PyFiles correctly",
		},
		{
			name: "PySpark application without PyFiles",
			sparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
				SparkAppType:        v1alpha1.SparkAppTypePython,
				SparkVersion:        "3.5.6",
				MainClass:           "",
				MainApplicationFile: "local:///mnt/spark/simple.py",
				QueueName:           "default",
				DriverResource: v1alpha1.SparkResource{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
				ExecutorResource: v1alpha1.SparkResource{
					Replicas: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
			expectedType:      "Python",
			expectedMainFile:  "local:///mnt/spark/simple.py",
			shouldHavePyFiles: false,
			description:       "Python application without PyFiles should work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a minimal workflow and task
			workflow := &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-workflow",
					Namespace: "test-namespace",
					// Note: UID is intentionally empty to test the case where owner references are not set
				},
				Spec: v1alpha1.LakeFlowSpec{},
			}

			task := &v1alpha1.Task{
				Name: "test-task",
				TaskSpec: v1alpha1.TaskSpec{
					SparkExecutorSpec: tt.sparkExecutorSpec,
				},
			}

			// Build the SparkApplication
			sparkApp := builder.buildSparkApplicationFromSpec(workflow, task)

			// Verify SparkApplication type
			if string(sparkApp.Spec.Type) != tt.expectedType {
				t.Errorf("Expected SparkApplication type %s, got %s", tt.expectedType, sparkApp.Spec.Type)
			}

			// Verify MainApplicationFile
			if sparkApp.Spec.MainApplicationFile == nil || *sparkApp.Spec.MainApplicationFile != tt.expectedMainFile {
				if sparkApp.Spec.MainApplicationFile == nil {
					t.Errorf("Expected MainApplicationFile %s, got nil", tt.expectedMainFile)
				} else {
					t.Errorf("Expected MainApplicationFile %s, got %s", tt.expectedMainFile, *sparkApp.Spec.MainApplicationFile)
				}
			}

			// Apply PyFiles (simulating the full Build flow)
			builder.setPySparkDependencies(sparkApp, tt.sparkExecutorSpec)

			// Verify PyFiles
			if tt.shouldHavePyFiles {
				if len(sparkApp.Spec.Deps.PyFiles) != len(tt.expectedPyFiles) {
					t.Errorf("Expected %d PyFiles, got %d", len(tt.expectedPyFiles), len(sparkApp.Spec.Deps.PyFiles))
				} else {
					for i, expectedFile := range tt.expectedPyFiles {
						if sparkApp.Spec.Deps.PyFiles[i] != expectedFile {
							t.Errorf("PyFiles[%d]: expected %s, got %s", i, expectedFile, sparkApp.Spec.Deps.PyFiles[i])
						}
					}
				}
			} else {
				if len(sparkApp.Spec.Deps.PyFiles) > 0 {
					t.Errorf("Expected no PyFiles for %s, but got %v", tt.description, sparkApp.Spec.Deps.PyFiles)
				}
			}

			// Verify basic SparkApplication structure
			if sparkApp.Spec.SparkVersion != tt.sparkExecutorSpec.SparkVersion {
				t.Errorf("Expected SparkVersion %s, got %s", tt.sparkExecutorSpec.SparkVersion, sparkApp.Spec.SparkVersion)
			}

			if sparkApp.Spec.Mode != "cluster" {
				t.Errorf("Expected Mode 'cluster', got %s", sparkApp.Spec.Mode)
			}

			// Verify driver and executor configurations exist
			if sparkApp.Spec.Driver.Cores == nil {
				t.Error("Expected driver cores to be set")
			}
			if sparkApp.Spec.Executor.Cores == nil {
				t.Error("Expected executor cores to be set")
			}
		})
	}
}

// Test_buildSparkApplicationFromSpec_PySparkWithPVCAndSitePackages tests PySpark with OSS PVC and site-packages
func Test_buildSparkApplicationFromSpec_PySparkWithPVCAndSitePackages(t *testing.T) {
	builder := &sparkTaskRenderer{}

	sparkExecutorSpec := &v1alpha1.SparkExecutorSpec{
		SparkAppType:        v1alpha1.SparkAppTypePython,
		SparkVersion:        "3.5.6",
		MainClass:           "",
		MainApplicationFile: "local:///mnt/spark/main.py",
		PyFiles: []string{
			"local:///mnt/spark/mypyproject.zip",
		},
		PySitePkgsArchive: "local:///mnt/spark/pyspark-sitepkgs/pyspark_sitepkgs.tar.gz",
		QueueName:         "default",
		DriverResource: v1alpha1.SparkResource{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("4Gi"),
				},
			},
		},
		ExecutorResource: v1alpha1.SparkResource{
			Replicas: 3,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
		},
	}

	workflow := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pyspark-workflow",
			Namespace: "test-namespace",
		},
		Spec: v1alpha1.LakeFlowSpec{},
	}

	task := &v1alpha1.Task{
		Name: "pyspark-task",
		TaskSpec: v1alpha1.TaskSpec{
			SparkExecutorSpec: sparkExecutorSpec,
		},
	}

	// Build SparkApplication
	sparkApp := builder.buildSparkApplicationFromSpec(workflow, task)

	// Apply dependencies
	builder.setPySparkDependencies(sparkApp, sparkExecutorSpec)

	// Apply Spark config (including PySpark-specific configs)
	builder.setSparkSpecConfig(sparkApp, *sparkExecutorSpec, newK8sCredentialResolver("test-namespace"), testAliyunProfile())

	// Verify SparkApplication type
	if string(sparkApp.Spec.Type) != "Python" {
		t.Errorf("Expected type Python, got %s", sparkApp.Spec.Type)
	}

	// Verify PyFiles are set
	if len(sparkApp.Spec.Deps.PyFiles) != 1 {
		t.Errorf("Expected 1 PyFile, got %d", len(sparkApp.Spec.Deps.PyFiles))
	} else if sparkApp.Spec.Deps.PyFiles[0] != "local:///mnt/spark/mypyproject.zip" {
		t.Errorf("Expected PyFile local:///mnt/spark/mypyproject.zip, got %s", sparkApp.Spec.Deps.PyFiles[0])
	}

	// Verify PySpark site-packages configuration in SparkConf
	expectedArchive := "local:///mnt/spark/pyspark-sitepkgs/pyspark_sitepkgs.tar.gz#pyspark_pkgs"
	if sparkApp.Spec.SparkConf["spark.archives"] != expectedArchive {
		t.Errorf("Expected spark.archives %s, got %s", expectedArchive, sparkApp.Spec.SparkConf["spark.archives"])
	}

	expectedPythonPath := "./pyspark_pkgs/pyspark_pkgs"
	if sparkApp.Spec.SparkConf["spark.kubernetes.driverEnv.PYTHONPATH"] != expectedPythonPath {
		t.Errorf("Expected driver PYTHONPATH %s, got %s", expectedPythonPath, sparkApp.Spec.SparkConf["spark.kubernetes.driverEnv.PYTHONPATH"])
	}
	if sparkApp.Spec.SparkConf["spark.kubernetes.executorEnv.PYTHONPATH"] != expectedPythonPath {
		t.Errorf("Expected executor PYTHONPATH %s, got %s", expectedPythonPath, sparkApp.Spec.SparkConf["spark.kubernetes.executorEnv.PYTHONPATH"])
	}

	// Verify Arrow optimization is enabled
	if sparkApp.Spec.SparkConf["spark.sql.execution.arrow.pyspark.enabled"] != "true" {
		t.Errorf("Expected Arrow optimization to be enabled, got %s", sparkApp.Spec.SparkConf["spark.sql.execution.arrow.pyspark.enabled"])
	}
	if sparkApp.Spec.SparkConf["spark.sql.execution.arrow.pyspark.fallback.enabled"] != "true" {
		t.Errorf("Expected Arrow fallback to be enabled, got %s", sparkApp.Spec.SparkConf["spark.sql.execution.arrow.pyspark.fallback.enabled"])
	}

	// Verify executor replicas
	if sparkApp.Spec.Executor.Instances == nil || *sparkApp.Spec.Executor.Instances != 3 {
		if sparkApp.Spec.Executor.Instances == nil {
			t.Error("Expected executor instances to be set")
		} else {
			t.Errorf("Expected 3 executor instances, got %d", *sparkApp.Spec.Executor.Instances)
		}
	}
}

// Test_PySparkBackwardCompatibility verifies that existing Java Spark jobs work without changes
func Test_PySparkBackwardCompatibility(t *testing.T) {
	builder := &sparkTaskRenderer{}

	// Simulate an existing Java Spark job defined before PySpark support was added
	// This spec does NOT include SparkAppType field
	legacyJavaSpec := &v1alpha1.SparkExecutorSpec{
		SparkVersion:        "3.5.2",
		MainClass:           "com.example.LegacyApp",
		MainApplicationFile: "local:///mnt/spark/legacy-app.jar",
		Arguments:           []string{"--input", "s3://bucket/input", "--output", "s3://bucket/output"},
		QueueName:           "production",
		DriverResource: v1alpha1.SparkResource{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
		},
		ExecutorResource: v1alpha1.SparkResource{
			Replicas: 10,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("8"),
					corev1.ResourceMemory: resource.MustParse("16Gi"),
				},
			},
		},
		SparkConfig: map[string]string{
			"spark.sql.adaptive.enabled":      "true",
			"spark.dynamicAllocation.enabled": "false",
		},
	}

	workflow := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-workflow",
			Namespace: "test-namespace",
		},
	}

	task := &v1alpha1.Task{
		Name: "legacy-task",
		TaskSpec: v1alpha1.TaskSpec{
			SparkExecutorSpec: legacyJavaSpec,
		},
	}

	// Build SparkApplication
	sparkApp := builder.buildSparkApplicationFromSpec(workflow, task)
	builder.setPySparkDependencies(sparkApp, legacyJavaSpec)
	builder.setSparkSpecConfig(sparkApp, *legacyJavaSpec, newK8sCredentialResolver("test-namespace"), testAliyunProfile())

	// Critical assertion: SparkAppType must default to Java
	if string(sparkApp.Spec.Type) != "Java" {
		t.Errorf("BACKWARD COMPATIBILITY BROKEN: Expected default type Java for legacy spec, got %s", sparkApp.Spec.Type)
	}

	// Verify no Python-specific configurations are applied
	if _, hasArchives := sparkApp.Spec.SparkConf["spark.archives"]; hasArchives {
		t.Error("BACKWARD COMPATIBILITY BROKEN: Java job should not have spark.archives (PySpark)")
	}
	if _, hasPythonPath := sparkApp.Spec.SparkConf["spark.kubernetes.driverEnv.PYTHONPATH"]; hasPythonPath {
		t.Error("BACKWARD COMPATIBILITY BROKEN: Java job should not have PYTHONPATH")
	}
	if len(sparkApp.Spec.Deps.PyFiles) > 0 {
		t.Errorf("BACKWARD COMPATIBILITY BROKEN: Java job should not have PyFiles, got %v", sparkApp.Spec.Deps.PyFiles)
	}

	// Verify user's spark config is preserved
	if sparkApp.Spec.SparkConf["spark.sql.adaptive.enabled"] != "true" {
		t.Error("User's spark.sql.adaptive.enabled config was not preserved")
	}

	// Verify MainClass is set correctly (only used for Java)
	if sparkApp.Spec.MainClass == nil || *sparkApp.Spec.MainClass != "com.example.LegacyApp" {
		t.Error("MainClass should be set for Java application")
	}

	// Verify JAR file path
	if sparkApp.Spec.MainApplicationFile == nil || *sparkApp.Spec.MainApplicationFile != "local:///mnt/spark/legacy-app.jar" {
		t.Error("MainApplicationFile should point to JAR for Java application")
	}

	t.Log("✓ Backward compatibility verified: legacy Java Spark jobs work without modifications")
}

// Test_PySparkWithHiveAndIceberg verifies that PySpark works with Hive/Iceberg (JVM-based features)
func Test_PySparkWithHiveAndIceberg(t *testing.T) {
	builder := &sparkTaskRenderer{}

	pysparkWithHive := &v1alpha1.SparkExecutorSpec{
		SparkAppType:        v1alpha1.SparkAppTypePython,
		SparkVersion:        "3.5.6",
		MainApplicationFile: "local:///mnt/spark/iceberg_etl.py",
		PyFiles: []string{
			"local:///mnt/spark/iceberg_utils.zip",
		},
		QueueName: "default",
		HiveMetastoreSpec: &v1alpha1.HiveMetastoreSpec{
			HiveHmsUri: "thrift://hive-metastore:9083",
			LibPath:    "/opt/hive3/lib/*",
		},
		IcebergWarehouse: "oss://data-lake/iceberg-warehouse",
		DriverResource: v1alpha1.SparkResource{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("4Gi"),
				},
			},
		},
		ExecutorResource: v1alpha1.SparkResource{
			Replicas: 5,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
		},
	}

	workflow := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pyspark-iceberg-workflow",
			Namespace: "test-namespace",
		},
	}

	task := &v1alpha1.Task{
		Name: "pyspark-iceberg-task",
		TaskSpec: v1alpha1.TaskSpec{
			SparkExecutorSpec: pysparkWithHive,
		},
	}

	sparkApp := builder.buildSparkApplicationFromSpec(workflow, task)
	builder.setPySparkDependencies(sparkApp, pysparkWithHive)
	builder.setSparkSpecConfig(sparkApp, *pysparkWithHive, newK8sCredentialResolver("test-namespace"), testAliyunProfile())

	// Verify it's a Python application
	if string(sparkApp.Spec.Type) != "Python" {
		t.Errorf("Expected Python type, got %s", sparkApp.Spec.Type)
	}

	// Verify PyFiles are set
	if len(sparkApp.Spec.Deps.PyFiles) != 1 {
		t.Errorf("Expected 1 PyFile, got %d", len(sparkApp.Spec.Deps.PyFiles))
	}

	// Verify Hive configurations are applied (JVM-based, works with PySpark)
	if sparkApp.Spec.SparkConf["spark.sql.catalogImplementation"] != "hive" {
		t.Error("Hive catalog implementation should be configured")
	}
	if sparkApp.Spec.SparkConf["spark.hadoop.hive.metastore.uris"] != "thrift://hive-metastore:9083" {
		t.Error("Hive metastore URI should be configured")
	}

	// Verify Iceberg configurations are applied (JVM-based, works with PySpark)
	if sparkApp.Spec.SparkConf["spark.sql.catalog.iceberg.warehouse"] != "oss://data-lake/iceberg-warehouse" {
		t.Error("Iceberg warehouse should be configured")
	}
	if sparkApp.Spec.SparkConf["spark.sql.catalog.iceberg.type"] != "hive" {
		t.Error("Iceberg catalog type should be hive")
	}

	// Verify Arrow optimization for PySpark
	if sparkApp.Spec.SparkConf["spark.sql.execution.arrow.pyspark.enabled"] != "true" {
		t.Error("Arrow optimization should be enabled for PySpark")
	}

	t.Log("✓ PySpark works correctly with JVM-based features (Hive3, Iceberg)")
}

// Test_getMainClassPointer verifies that MainClass is correctly set to nil for Python
// and to a valid pointer for Java applications
func Test_getMainClassPointer(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name              string
		spec              *v1alpha1.SparkExecutorSpec
		expectedNil       bool
		expectedMainClass string
		description       string
	}{
		{
			name: "Java application with MainClass",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypeJava,
				MainClass:    "com.example.JavaApp",
			},
			expectedNil:       false,
			expectedMainClass: "com.example.JavaApp",
			description:       "Java app should have non-nil MainClass pointer",
		},
		{
			name: "Python application with empty MainClass",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypePython,
				MainClass:    "",
			},
			expectedNil: true,
			description: "Python app should have nil MainClass pointer",
		},
		{
			name: "Python application with MainClass incorrectly set (validation error case)",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypePython,
				MainClass:    "com.example.Wrong",
			},
			expectedNil: true,
			description: "Python app should have nil MainClass even if incorrectly specified",
		},
		{
			name: "Java application without MainClass (backward compatibility default)",
			spec: &v1alpha1.SparkExecutorSpec{
				MainClass: "com.example.LegacyApp",
			},
			expectedNil:       false,
			expectedMainClass: "com.example.LegacyApp",
			description:       "Legacy Java app without SparkAppType should have MainClass pointer",
		},
		{
			name: "Java application with empty MainClass",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType: v1alpha1.SparkAppTypeJava,
				MainClass:    "",
			},
			expectedNil: true,
			description: "Java app with empty MainClass should return nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := builder.getMainClassPointer(tt.spec)

			if tt.expectedNil {
				if result != nil {
					t.Errorf("%s: Expected nil MainClass pointer, got %v", tt.description, *result)
				}
			} else {
				if result == nil {
					t.Errorf("%s: Expected non-nil MainClass pointer, got nil", tt.description)
				} else if *result != tt.expectedMainClass {
					t.Errorf("%s: Expected MainClass %s, got %s", tt.description, tt.expectedMainClass, *result)
				}
			}
		})
	}

	t.Log("✓ MainClass pointer handling verified for Java and Python applications")
}

// Test_buildSparkApplicationFromSpec_MainClassPointer verifies that the built SparkApplication
// has the correct MainClass value (nil for Python, non-nil for Java)
func Test_buildSparkApplicationFromSpec_MainClassPointer(t *testing.T) {
	builder := &sparkTaskRenderer{}

	tests := []struct {
		name                   string
		spec                   *v1alpha1.SparkExecutorSpec
		expectMainClassNil     bool
		expectedMainClassValue string
		description            string
	}{
		{
			name: "PySpark application - MainClass should be nil",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType:        v1alpha1.SparkAppTypePython,
				SparkVersion:        "3.5.6",
				MainClass:           "", // Empty for Python
				MainApplicationFile: "local:///mnt/spark/main.py",
				QueueName:           "default",
				DriverResource: v1alpha1.SparkResource{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
				ExecutorResource: v1alpha1.SparkResource{
					Replicas: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
			expectMainClassNil: true,
			description:        "Python application must have nil MainClass (not pointer to empty string)",
		},
		{
			name: "Java application - MainClass should be set",
			spec: &v1alpha1.SparkExecutorSpec{
				SparkAppType:        v1alpha1.SparkAppTypeJava,
				SparkVersion:        "3.5.6",
				MainClass:           "com.example.JavaApp",
				MainApplicationFile: "local:///mnt/spark/app.jar",
				QueueName:           "default",
				DriverResource: v1alpha1.SparkResource{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
				ExecutorResource: v1alpha1.SparkResource{
					Replicas: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
			expectMainClassNil:     false,
			expectedMainClassValue: "com.example.JavaApp",
			description:            "Java application must have MainClass set to non-nil pointer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflow := &v1alpha1.LakeFlow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-workflow",
					Namespace: "test-namespace",
				},
			}

			task := &v1alpha1.Task{
				Name: "test-task",
				TaskSpec: v1alpha1.TaskSpec{
					SparkExecutorSpec: tt.spec,
				},
			}

			sparkApp := builder.buildSparkApplicationFromSpec(workflow, task)

			if tt.expectMainClassNil {
				if sparkApp.Spec.MainClass != nil {
					t.Errorf("%s: Expected MainClass to be nil, but got pointer to: %s",
						tt.description, *sparkApp.Spec.MainClass)
					t.Error("CRITICAL BUG: Spark Operator will reject SparkApplication with empty MainClass string for Python apps")
				} else {
					t.Logf("✓ %s: MainClass correctly set to nil", tt.description)
				}
			} else {
				if sparkApp.Spec.MainClass == nil {
					t.Errorf("%s: Expected MainClass to be set, but got nil", tt.description)
				} else if *sparkApp.Spec.MainClass != tt.expectedMainClassValue {
					t.Errorf("%s: Expected MainClass %s, got %s",
						tt.description, tt.expectedMainClassValue, *sparkApp.Spec.MainClass)
				} else {
					t.Logf("✓ %s: MainClass correctly set to %s", tt.description, *sparkApp.Spec.MainClass)
				}
			}
		})
	}
}

func Test_buildSparkApplicationFromSpec_AppliesTaskSchedulingSpec(t *testing.T) {
	builder := &sparkTaskRenderer{}
	workflow := &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wf-a",
			Namespace: "test-ns",
		},
	}

	task := &v1alpha1.Task{
		Name: "spark-task",
		TaskSpec: v1alpha1.TaskSpec{
			PodScheduling: &v1alpha1.TaskPodSchedulingSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "nodepool", Operator: corev1.NodeSelectorOpIn, Values: []string{"np-spark"}},
									},
								},
							},
						},
					},
				},
				NodeSelector: map[string]string{"nodepool": "np-spark"},
				Tolerations: []corev1.Toleration{
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "spark",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
					{
						MaxSkew:           1,
						TopologyKey:       "topology.kubernetes.io/zone",
						WhenUnsatisfiable: corev1.DoNotSchedule,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{v1alpha1.WorkflowNameLabel: "wf-a"},
						},
					},
				},
			},
			SparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
				MainApplicationFile: "local:///mnt/spark/app.py",
				QueueName:           "default",
				DriverResource: v1alpha1.SparkResource{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				},
				ExecutorResource: v1alpha1.SparkResource{
					Replicas: 1,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"cpu":    resource.MustParse("1"),
							"memory": resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
	}

	sparkApp := builder.buildSparkApplicationFromSpec(workflow, task)
	if sparkApp.Spec.NodeSelector != nil {
		t.Fatalf("expected deprecated app-level nodeSelector to be unset, got %v", sparkApp.Spec.NodeSelector)
	}
	if got := sparkApp.Spec.Driver.NodeSelector["nodepool"]; got != "np-spark" {
		t.Fatalf("expected driver nodepool selector np-spark, got %s", got)
	}
	if got := sparkApp.Spec.Driver.NodeSelector["volcano.sh/nodegroup-name"]; got != "volcano-batch-job" {
		t.Fatalf("expected default volcano nodegroup selector on driver, got %s", got)
	}
	if got := sparkApp.Spec.Executor.NodeSelector["nodepool"]; got != "np-spark" {
		t.Fatalf("expected executor nodepool selector np-spark, got %s", got)
	}
	if got := sparkApp.Spec.Executor.NodeSelector["volcano.sh/nodegroup-name"]; got != "volcano-batch-job" {
		t.Fatalf("expected default volcano nodegroup selector on executor, got %s", got)
	}
	if sparkApp.Spec.Driver.Affinity == nil {
		t.Fatalf("expected driver affinity to be set")
	}
	if sparkApp.Spec.Executor.Affinity == nil {
		t.Fatalf("expected executor affinity to be set")
	}
	if len(sparkApp.Spec.Driver.Tolerations) != 1 {
		t.Fatalf("expected one driver toleration, got %d", len(sparkApp.Spec.Driver.Tolerations))
	}
	if len(sparkApp.Spec.Executor.Tolerations) != 1 {
		t.Fatalf("expected one executor toleration, got %d", len(sparkApp.Spec.Executor.Tolerations))
	}
	if sparkApp.Spec.Driver.Template == nil || len(sparkApp.Spec.Driver.Template.Spec.TopologySpreadConstraints) != 1 {
		t.Fatalf("expected one driver topologySpreadConstraint in pod template")
	}
	if len(sparkApp.Spec.Driver.Template.Spec.Containers) == 0 ||
		sparkApp.Spec.Driver.Template.Spec.Containers[0].Name != "spark-kubernetes-driver" {
		t.Fatalf("expected driver template primary container name spark-kubernetes-driver")
	}
	if sparkApp.Spec.Executor.Template == nil || len(sparkApp.Spec.Executor.Template.Spec.TopologySpreadConstraints) != 1 {
		t.Fatalf("expected one executor topologySpreadConstraint in pod template")
	}
	if len(sparkApp.Spec.Executor.Template.Spec.Containers) == 0 ||
		sparkApp.Spec.Executor.Template.Spec.Containers[0].Name != "spark-kubernetes-executor" {
		t.Fatalf("expected executor template primary container name spark-kubernetes-executor")
	}
	if sparkApp.Spec.Driver.Template.Spec.TopologySpreadConstraints[0].WhenUnsatisfiable != corev1.DoNotSchedule {
		t.Fatalf("expected driver spread to map enforce to DoNotSchedule")
	}
}
