package common

import (
	"os"
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// createTestScheme creates a runtime scheme for testing with required types
func createTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	return scheme
}

func TestLoadFromLakeFlowYaml(t *testing.T) {
	// Create a temporary YAML file for testing
	validYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: test-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: test-task
      executor: bash
      commandExecutor:
        command: ["/bin/bash", "-c"]
        commandFile: ""
        args: ["echo hello world"]`

	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-lakeflow-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write YAML content
	if _, err := tmpFile.WriteString(validYAML); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Test successful loading
	scheme := createTestScheme()
	lw, err := LoadFromLakeFlowYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify the loaded workflow
	if lw.Name != "test-workflow" {
		t.Errorf("Expected name 'test-workflow', got: %s", lw.Name)
	}

	if lw.Namespace != "test-namespace" {
		t.Errorf("Expected namespace 'test-namespace', got: %s", lw.Namespace)
	}

	if len(lw.Spec.Tasks) != 1 {
		t.Errorf("Expected 1 task, got: %d", len(lw.Spec.Tasks))
	}

	if lw.Spec.Tasks[0].Name != "test-task" {
		t.Errorf("Expected task name 'test-task', got: %s", lw.Spec.Tasks[0].Name)
	}

	if lw.Spec.Tasks[0].Executor != v1alpha1.TaskExecutorBash {
		t.Errorf("Expected executor 'bash', got: %s", lw.Spec.Tasks[0].Executor)
	}
}

func TestLoadFromLakeFlowYaml_MultiDocument(t *testing.T) {
	// Create a multi-document YAML file with LakeFlow
	multiDocYAML := `apiVersion: v1
kind: Pod
metadata:
  name: example-pod
spec:
  containers:
  - name: nginx
    image: nginx
---
apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: multi-doc-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: python
      commandExecutor:
        command: ["/usr/bin/python3", "-c"]
        commandFile: ""
        args: ["print('Hello from Python')"]
---
apiVersion: v1
kind: Service
metadata:
  name: example-service
spec:
  ports:
  - port: 80`

	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-multi-doc-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write YAML content
	if _, err := tmpFile.WriteString(multiDocYAML); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Test successful loading from multi-document YAML
	scheme := createTestScheme()
	lw, err := LoadFromLakeFlowYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify the loaded workflow
	if lw.Name != "multi-doc-workflow" {
		t.Errorf("Expected name 'multi-doc-workflow', got: %s", lw.Name)
	}

	if lw.Spec.Tasks[0].Executor != v1alpha1.TaskExecutorPython {
		t.Errorf("Expected executor 'python', got: %s", lw.Spec.Tasks[0].Executor)
	}
}

func TestLoadFromLakeFlowYaml_FileNotFound(t *testing.T) {
	scheme := createTestScheme()
	_, err := LoadFromLakeFlowYaml(scheme, "nonexistent-file.yaml")
	if err == nil {
		t.Fatal("Expected error for nonexistent file, got nil")
	}
}

func TestLoadFromLakeFlowYaml_NoTasks(t *testing.T) {
	// Create temp file with workflow that has no tasks
	tmpFile, err := os.CreateTemp("", "test-no-tasks-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	noTasksYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: test-workflow
spec:
  trigger: {}
  tasks: []`

	tmpFile.WriteString(noTasksYAML)
	tmpFile.Close()

	scheme := createTestScheme()
	_, err = LoadFromLakeFlowYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for workflow with no tasks, got nil")
	}
}

func TestLoadFromLakeFlowYaml_WrongKind(t *testing.T) {
	// Create temp file with wrong kind
	tmpFile, err := os.CreateTemp("", "test-wrong-kind-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	wrongKindYAML := `apiVersion: lakeflow.io/v1alpha1
kind: SomeOtherResource
metadata:
  name: test-workflow
spec:
  tasks:
    - name: test-task`

	tmpFile.WriteString(wrongKindYAML)
	tmpFile.Close()

	scheme := createTestScheme()
	_, err = LoadFromLakeFlowYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for wrong kind, got nil")
	}
}

func TestLoadFromLakeFlowYaml_NoLakeFlowFound(t *testing.T) {
	// Create temp file with only other resources, no LakeFlow
	tmpFile, err := os.CreateTemp("", "test-no-lakeflow-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	noLakeFlowYAML := `apiVersion: v1
kind: Pod
metadata:
  name: example-pod
spec:
  containers:
  - name: nginx
    image: nginx
---
apiVersion: v1
kind: Service
metadata:
  name: example-service
spec:
  ports:
  - port: 80`

	tmpFile.WriteString(noLakeFlowYAML)
	tmpFile.Close()

	scheme := createTestScheme()
	_, err = LoadFromLakeFlowYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error when no LakeFlow found, got nil")
	}
}

func TestLoadFromLakeFlowYaml_NoName(t *testing.T) {
	// Create temp file with workflow that has no name
	tmpFile, err := os.CreateTemp("", "test-no-name-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	noNameYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: test-task
      executor: bash
      commandExecutor:
        image: busybox:latest
        inlineMode:
          command: ["/bin/bash", "-c"]
          args: ["echo hello"]`

	tmpFile.WriteString(noNameYAML)
	tmpFile.Close()

	scheme := createTestScheme()
	_, err = LoadFromLakeFlowYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for workflow with no name, got nil")
	}
}

func TestLoadFromLakeFlowYaml_RealExample(t *testing.T) {
	// Test with the actual schedule trigger example
	exampleYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: lw-schedule-trigger
  namespace: lakeflow-controller-test
  labels:
    app.kubernetes.io/name: lakeflow-controller
    app.kubernetes.io/managed-by: lakeflow-controller
    lakeflow.io/name: lw-schedule-trigger
spec:
  # Schedule trigger - runs every minute
  trigger:
    schedule:
      cron: "* * * * *"
      timezone: "UTC"
      jitter: "30s"
  
  # Daily data processing workflow
  tasks:
    - name: daily-extract
      executor: bash
      commandExecutor:
        command: ["/bin/bash", "-c"]
        commandFile: ""
        args: ["echo 'Daily extraction started'; DATE=$(date +%Y-%m-%d); echo \"Processing date: $DATE\"; mkdir -p /tmp/daily/$DATE; echo 'sample daily data' > /tmp/daily/$DATE/extract.txt; echo 'Daily extraction completed'"]
      retryPolicy:
        maxRetries: 3
        backOff: "60s"
        backoffFactor: 2.0
    
    - name: daily-transform
      executor: spark
      dependsOn: ["daily-extract"]
      sparkExecutor:
        mainClass: org.apache.spark.examples.SparkPi
        mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
        args: ["20"]
        queueName: default
        driverResource:
          replicas: 1
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
            limits:
              cpu: "2"
              memory: "2Gi"
        executorResource:
          replicas: 1
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
            limits:
              cpu: "2"
              memory: "2Gi"
        serviceAccount: "spark-operator-spark"
        sparkConfig:
          spark.sql.adaptive.enabled: "true"
      retryPolicy:
        maxRetries: 2
        backOff: "120s"`

	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-real-example-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write YAML content
	if _, err := tmpFile.WriteString(exampleYAML); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Test successful loading from real example
	scheme := createTestScheme()
	lw, err := LoadFromLakeFlowYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify the loaded workflow
	if lw.Name != "lw-schedule-trigger" {
		t.Errorf("Expected name 'lw-schedule-trigger', got: %s", lw.Name)
	}

	if lw.Namespace != "lakeflow-controller-test" {
		t.Errorf("Expected namespace 'lakeflow-controller-test', got: %s", lw.Namespace)
	}

	if len(lw.Spec.Tasks) != 2 {
		t.Errorf("Expected 2 tasks, got: %d", len(lw.Spec.Tasks))
	}

	// Check first task
	if lw.Spec.Tasks[0].Name != "daily-extract" {
		t.Errorf("Expected first task name 'daily-extract', got: %s", lw.Spec.Tasks[0].Name)
	}

	if lw.Spec.Tasks[0].Executor != v1alpha1.TaskExecutorBash {
		t.Errorf("Expected first task executor 'bash', got: %s", lw.Spec.Tasks[0].Executor)
	}

	// Check second task
	if lw.Spec.Tasks[1].Name != "daily-transform" {
		t.Errorf("Expected second task name 'daily-transform', got: %s", lw.Spec.Tasks[1].Name)
	}

	if lw.Spec.Tasks[1].Executor != v1alpha1.TaskExecutorSpark {
		t.Errorf("Expected second task executor 'spark', got: %s", lw.Spec.Tasks[1].Executor)
	}

	// Check dependencies
	if len(lw.Spec.Tasks[1].DependsOn) != 1 || lw.Spec.Tasks[1].DependsOn[0] != "daily-extract" {
		t.Errorf("Expected second task to depend on 'daily-extract', got: %v", lw.Spec.Tasks[1].DependsOn)
	}

	// Check schedule trigger
	if lw.Spec.WorkflowTrigger.Schedule.Cron != "* * * * *" {
		t.Errorf("Expected cron '* * * * *', got: %s", lw.Spec.WorkflowTrigger.Schedule.Cron)
	}

	if lw.Spec.WorkflowTrigger.Schedule.Timezone != "UTC" {
		t.Errorf("Expected timezone 'UTC', got: %s", lw.Spec.WorkflowTrigger.Schedule.Timezone)
	}

	if lw.Spec.WorkflowTrigger.Schedule.Jitter != "30s" {
		t.Errorf("Expected jitter '30s', got: %s", lw.Spec.WorkflowTrigger.Schedule.Jitter)
	}
}

func TestLoadAndValidateLakeFlowFromYaml(t *testing.T) {
	// Test with valid workflow
	validYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: test-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: bash
      commandExecutor:
        image: busybox:latest
        inlineMode:
          command: ["/bin/bash", "-c"]
          args: ["echo hello"]
    - name: task2
      executor: spark
      dependsOn: ["task1"]
      sparkExecutor:
        mainClass: org.apache.spark.examples.SparkPi
        mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
        args: ["10"]
        queueName: default
        driverResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
        executorResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"`

	tmpFile, err := os.CreateTemp("", "test-validate-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(validYAML)
	tmpFile.Close()

	// Should succeed
	scheme := createTestScheme()
	lw, err := LoadAndValidateLakeFlowFromYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error for valid workflow, got: %v", err)
	}

	if lw.Name != "test-workflow" {
		t.Errorf("Expected name 'test-workflow', got: %s", lw.Name)
	}
}

func TestLoadAndValidateLakeFlowFromYaml_InvalidDependency(t *testing.T) {
	// Test with invalid dependency
	invalidYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: test-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: bash
      commandExecutor:
        image: busybox:latest
        inlineMode:
          command: ["/bin/bash", "-c"]
          args: ["echo hello"]
    - name: task2
      executor: bash
      dependsOn: ["nonexistent-task"]
      commandExecutor:
        command: ["/bin/bash", "-c"]
        commandFile: ""
        args: ["echo world"]`

	tmpFile, err := os.CreateTemp("", "test-invalid-dep-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(invalidYAML)
	tmpFile.Close()

	// Should fail due to invalid dependency
	scheme := createTestScheme()
	_, err = LoadAndValidateLakeFlowFromYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for invalid dependency, got nil")
	}
}

func TestLoadAndValidateLakeFlowFromYaml_MissingSparkMainClass(t *testing.T) {
	// Test with spark executor missing mainClass
	invalidYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: test-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: spark
      sparkExecutor:
        mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
        args: ["10"]
        queueName: default
        driverResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
        executorResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"`

	tmpFile, err := os.CreateTemp("", "test-missing-mainclass-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(invalidYAML)
	tmpFile.Close()

	// Should fail due to missing mainClass
	scheme := createTestScheme()
	_, err = LoadAndValidateLakeFlowFromYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for missing spark mainClass, got nil")
	}
}

func TestLoadLakeFlowsFromYaml_Single(t *testing.T) {
	// Test with single LakeFlow
	validYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: test-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: test-task
      executor: bash
      commandExecutor:
        command: ["/bin/bash", "-c"]
        commandFile: ""
        args: ["echo hello world"]`

	tmpFile, err := os.CreateTemp("", "test-single-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(validYAML)
	tmpFile.Close()

	// Test loading all workflows
	scheme := createTestScheme()
	lws, err := LoadLakeFlowsFromYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(lws) != 1 {
		t.Fatalf("Expected 1 workflow, got: %d", len(lws))
	}

	if lws[0].Name != "test-workflow" {
		t.Errorf("Expected name 'test-workflow', got: %s", lws[0].Name)
	}
}

func TestLoadLakeFlowsFromYaml_Multiple(t *testing.T) {
	// Test with multiple LakeFlows in one file
	multiWorkflowYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: workflow1
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: bash
      commandExecutor:
        command: ["/bin/bash", "-c"]
        commandFile: ""
        args: ["echo workflow1"]
---
apiVersion: v1
kind: Pod
metadata:
  name: example-pod
spec:
  containers:
  - name: nginx
    image: nginx
---
apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: workflow2
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task2
      executor: python
      commandExecutor:
        command: ["/usr/bin/python3", "-c"]
        commandFile: ""
        args: ["print('workflow2')"]
---
apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: workflow3
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task3
      executor: bash
      commandExecutor:
        command: ["/bin/bash", "-c"]
        commandFile: ""
        args: ["echo workflow3"]`

	tmpFile, err := os.CreateTemp("", "test-multiple-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(multiWorkflowYAML)
	tmpFile.Close()

	// Test loading all workflows
	scheme := createTestScheme()
	lws, err := LoadLakeFlowsFromYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(lws) != 3 {
		t.Fatalf("Expected 3 workflows, got: %d", len(lws))
	}

	// Verify all three workflows
	expectedNames := []string{"workflow1", "workflow2", "workflow3"}
	expectedExecutors := []v1alpha1.TaskExecutor{v1alpha1.TaskExecutorBash, v1alpha1.TaskExecutorPython, v1alpha1.TaskExecutorBash}

	for i, lw := range lws {
		if lw.Name != expectedNames[i] {
			t.Errorf("Expected workflow %d name '%s', got: %s", i, expectedNames[i], lw.Name)
		}
		if lw.Spec.Tasks[0].Executor != expectedExecutors[i] {
			t.Errorf("Expected workflow %d executor '%s', got: %s", i, expectedExecutors[i], lw.Spec.Tasks[0].Executor)
		}
	}
}

func TestLoadFirstLakeFlowFromYaml(t *testing.T) {
	// Test with multiple workflows, should return only the first
	multiWorkflowYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: first-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: bash
      commandExecutor:
        command: ["/bin/bash", "-c"]
        commandFile: ""
        args: ["echo first"]
---
apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: second-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task2
      executor: python
      commandExecutor:
        image: python:3
        inlineMode:
          command: ["/usr/bin/python3", "-c"]
          args: ["print('second')"]`

	tmpFile, err := os.CreateTemp("", "test-first-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(multiWorkflowYAML)
	tmpFile.Close()

	// Test loading first workflow only
	scheme := createTestScheme()
	lw, err := LoadFirstLakeFlowFromYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if lw.Name != "first-workflow" {
		t.Errorf("Expected name 'first-workflow', got: %s", lw.Name)
	}

	if lw.Spec.Tasks[0].Executor != v1alpha1.TaskExecutorBash {
		t.Errorf("Expected executor 'bash', got: %s", lw.Spec.Tasks[0].Executor)
	}
}

func TestLoadAndValidateLakeFlowsFromYaml(t *testing.T) {
	// Test validation with multiple workflows
	multiWorkflowYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: valid-workflow1
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: bash
      commandExecutor:
        image: busybox:latest
        inlineMode:
          command: ["/bin/bash", "-c"]
          args: ["echo hello"]
---
apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: valid-workflow2
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: spark
      sparkExecutor:
        mainClass: org.apache.spark.examples.SparkPi
        mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
        args: ["10"]
        queueName: default
        driverResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
        executorResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"`

	tmpFile, err := os.CreateTemp("", "test-validate-multiple-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(multiWorkflowYAML)
	tmpFile.Close()

	// Should succeed with all valid workflows
	scheme := createTestScheme()
	lws, err := LoadAndValidateLakeFlowsFromYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error for valid workflows, got: %v", err)
	}

	if len(lws) != 2 {
		t.Fatalf("Expected 2 workflows, got: %d", len(lws))
	}

	if lws[0].Name != "valid-workflow1" {
		t.Errorf("Expected first workflow name 'valid-workflow1', got: %s", lws[0].Name)
	}

	if lws[1].Name != "valid-workflow2" {
		t.Errorf("Expected second workflow name 'valid-workflow2', got: %s", lws[1].Name)
	}
}

func TestLoadAndValidateLakeFlowsFromYaml_OneInvalid(t *testing.T) {
	// Test with one valid and one invalid workflow
	mixedWorkflowYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: valid-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: bash
      commandExecutor:
        image: busybox:latest
        inlineMode:
          command: ["/bin/bash", "-c"]
          args: ["echo hello"]
---
apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: invalid-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: task1
      executor: spark
      sparkExecutor:
        # Missing mainClass and mainApplicationFile
        args: ["10"]
        queueName: default
        driverResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
        executorResource:
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"`

	tmpFile, err := os.CreateTemp("", "test-mixed-validation-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(mixedWorkflowYAML)
	tmpFile.Close()

	// Should fail due to invalid second workflow
	scheme := createTestScheme()
	_, err = LoadAndValidateLakeFlowsFromYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for invalid workflow, got nil")
	}
}

// TestLoadAndValidateLakeFlowFromYaml_PySparkWithoutMainClass tests that PySpark workflows
// work correctly without MainClass field
func TestLoadAndValidateLakeFlowFromYaml_PySparkWithoutMainClass(t *testing.T) {
	// Test PySpark workflow without mainClass - should succeed
	pysparkYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: pyspark-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: pyspark-task
      executor: spark
      sparkExecutor:
        sparkAppType: Python
        mainApplicationFile: local:///mnt/spark/main.py
        pyFiles:
          - local:///mnt/spark/myproject.zip
        pySitePkgsArchive: local:///mnt/spark/pyspark_sitepkgs.tar.gz
        args: ["--input", "data.csv"]
        queueName: default
        driverResource:
          resources:
            requests:
              cpu: "2"
              memory: "4Gi"
        executorResource:
          replicas: 3
          resources:
            requests:
              cpu: "4"
              memory: "8Gi"`

	tmpFile, err := os.CreateTemp("", "test-pyspark-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(pysparkYAML)
	tmpFile.Close()

	// Should succeed - PySpark doesn't need mainClass
	scheme := createTestScheme()
	lw, err := LoadAndValidateLakeFlowFromYaml(scheme, tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error for PySpark without mainClass, got: %v", err)
	}

	if lw.Name != "pyspark-workflow" {
		t.Errorf("Expected name 'pyspark-workflow', got: %s", lw.Name)
	}

	sparkSpec := lw.Spec.Tasks[0].SparkExecutorSpec
	if sparkSpec.SparkAppType != v1alpha1.SparkAppTypePython {
		t.Errorf("Expected SparkAppType Python, got: %s", sparkSpec.SparkAppType)
	}

	if sparkSpec.MainClass != "" {
		t.Errorf("Expected empty mainClass for Python, got: %s", sparkSpec.MainClass)
	}

	if len(sparkSpec.PyFiles) != 1 {
		t.Errorf("Expected 1 PyFile, got: %d", len(sparkSpec.PyFiles))
	}

	if sparkSpec.PySitePkgsArchive == "" {
		t.Error("Expected PySitePkgsArchive to be set")
	}

	t.Log("✓ PySpark workflow without mainClass validated successfully")
}

// TestLoadAndValidateLakeFlowFromYaml_JavaWithoutMainClass tests that Java workflows
// without mainClass are properly rejected
func TestLoadAndValidateLakeFlowFromYaml_JavaWithoutMainClass(t *testing.T) {
	// Test Java workflow without mainClass - should fail
	javaNoMainClassYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: java-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: java-task
      executor: spark
      sparkExecutor:
        sparkAppType: Java
        mainApplicationFile: local:///mnt/spark/app.jar
        queueName: default
        driverResource:
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
        executorResource:
          resources:
            requests:
              cpu: "2"
              memory: "4Gi"`

	tmpFile, err := os.CreateTemp("", "test-java-no-mainclass-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(javaNoMainClassYAML)
	tmpFile.Close()

	// Should fail - Java applications require mainClass
	scheme := createTestScheme()
	_, err = LoadAndValidateLakeFlowFromYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for Java workflow without mainClass, got nil")
	}

	t.Logf("✓ Java workflow without mainClass correctly rejected: %v", err)
}

// TestLoadAndValidateLakeFlowFromYaml_PySparkWithMainClass tests that PySpark workflows
// with mainClass are properly rejected
func TestLoadAndValidateLakeFlowFromYaml_PySparkWithMainClass(t *testing.T) {
	// Test PySpark workflow with mainClass - should fail
	pysparkWithMainClassYAML := `apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: pyspark-workflow
  namespace: test-namespace
spec:
  trigger: {}
  tasks:
    - name: pyspark-task
      executor: spark
      sparkExecutor:
        sparkAppType: Python
        mainClass: com.example.WrongForPython
        mainApplicationFile: local:///mnt/spark/main.py
        queueName: default
        driverResource:
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
        executorResource:
          resources:
            requests:
              cpu: "2"
              memory: "4Gi"`

	tmpFile, err := os.CreateTemp("", "test-pyspark-with-mainclass-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(pysparkWithMainClassYAML)
	tmpFile.Close()

	// Should fail - Python applications should not have mainClass
	scheme := createTestScheme()
	_, err = LoadAndValidateLakeFlowFromYaml(scheme, tmpFile.Name())
	if err == nil {
		t.Fatal("Expected error for PySpark workflow with mainClass, got nil")
	}

	t.Logf("✓ PySpark workflow with mainClass correctly rejected: %v", err)
}
