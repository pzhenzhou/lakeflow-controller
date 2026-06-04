package common

import (
	"bytes"
	"fmt"
	"sort"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	crdv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"os"
	"path/filepath"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	KubeMasterUrlEnv = "KUBERNETES_MASTER"

	// DeploymentEnv is the environment variable name for deployment environment
	DeploymentEnv = "DEPLOYMENT_ENV"
)

var (
	logger = GetSharedLogger().WithName("lakeflow_utils")
)

// FindMostRelevantWorkflow finds the most relevant workflow from a list.
// Priority order:
//  1. Running workflows (highest priority - represent current state)
//  2. Most recently finished workflows (by FinishedAt timestamp)
//  3. Most recently created workflows (by CreationTimestamp)
//
// This ensures that after controller restart with TTL-retained workflows:
//   - Active executions take precedence
//   - For multiple completed workflows (retry/resubmit), the latest completion is selected
//
// Returns nil if the slice is empty.
func FindMostRelevantWorkflow(workflows []argowfv1.Workflow) *argowfv1.Workflow {
	if len(workflows) == 0 {
		return nil
	}

	// Sort by relevance
	sort.Slice(workflows, func(i, j int) bool {
		wfI := &workflows[i]
		wfJ := &workflows[j]

		// Priority 1: Running workflows come first
		isRunningI := wfI.Status.Phase == argowfv1.WorkflowRunning
		isRunningJ := wfJ.Status.Phase == argowfv1.WorkflowRunning

		if isRunningI && !isRunningJ {
			return true
		}
		if !isRunningI && isRunningJ {
			return false
		}

		// Priority 2: For finished workflows, sort by finish time (most recent first)
		isFinishedI := isWorkflowFinished(wfI.Status.Phase)
		isFinishedJ := isWorkflowFinished(wfJ.Status.Phase)

		if isFinishedI && isFinishedJ {
			// Both finished - compare finish times
			if !wfI.Status.FinishedAt.IsZero() && !wfJ.Status.FinishedAt.IsZero() {
				return wfI.Status.FinishedAt.After(wfJ.Status.FinishedAt.Time)
			}
			// Fallback to creation time if finish time is missing
		}

		// Priority 3: Default to creation timestamp for all other cases
		return wfI.CreationTimestamp.After(wfJ.CreationTimestamp.Time)
	})

	return &workflows[0]
}

// isWorkflowFinished checks if a workflow phase represents a finished state
func isWorkflowFinished(phase argowfv1.WorkflowPhase) bool {
	return phase == argowfv1.WorkflowSucceeded ||
		phase == argowfv1.WorkflowFailed ||
		phase == argowfv1.WorkflowError
}

// GetWorkflowSelectionReason returns a human-readable reason for why a workflow was selected
func GetWorkflowSelectionReason(wf *argowfv1.Workflow) string {
	if wf.Status.Phase == argowfv1.WorkflowRunning {
		return "running (highest priority)"
	}
	if isWorkflowFinished(wf.Status.Phase) && !wf.Status.FinishedAt.IsZero() {
		return "most recent finish time"
	}
	return "creation time"
}

// LoadLakeFlowsFromYaml loads all LakeFlow resources from a YAML file
// Returns an array of LakeFlow objects found in the file
func LoadLakeFlowsFromYaml(schema *runtime.Scheme, yamlFile string) ([]*v1alpha1.LakeFlow, error) {
	// Open the YAML file
	f, err := os.Open(yamlFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", yamlFile, err)
	}
	defer f.Close()

	y := yaml.NewYAMLOrJSONDecoder(f, 4096)
	dec := serializer.NewCodecFactory(schema).UniversalDeserializer()
	var lakeWorkflows []*v1alpha1.LakeFlow
	// Iterate through each document in the YAML file
	for {
		var rawObj runtime.RawExtension
		if err := y.Decode(&rawObj); err != nil {
			if err.Error() == "EOF" { // End of file
				break
			}
			return nil, fmt.Errorf("failed to decode YAML document: %w", err)
		}
		if len(rawObj.Raw) == 0 { // Empty document separator (e.g., "---")
			continue
		}

		// Decode the raw bytes into a runtime.Object and get its GVK
		obj, gvk, err := dec.Decode(rawObj.Raw, nil, nil)
		if err != nil {
			logger.Error(err, "failed to decode YAML document")
			return nil, fmt.Errorf("failed to deserialize object from raw data: %w", err)
		}

		// Check if the decoded object is our desired LakeFlow Custom Resource
		if gvk != nil && gvk.Kind == "LakeFlow" && gvk.Group == v1alpha1.GroupVersion.Group {
			if lakeWorkflow, ok := obj.(*v1alpha1.LakeFlow); ok {
				// Basic validation of required fields
				if lakeWorkflow.Name == "" {
					return nil, fmt.Errorf("LakeFlow in %s must have a name", yamlFile)
				}

				if len(lakeWorkflow.Spec.Tasks) == 0 {
					return nil, fmt.Errorf("LakeFlow in %s must have at least one task", yamlFile)
				}

				lakeWorkflows = append(lakeWorkflows, lakeWorkflow)
			} else {
				return nil, fmt.Errorf("decoded object was of kind %s/%s but could not be cast to *v1alpha1.LakeFlow", gvk.Group, gvk.Kind)
			}
		}
	}

	if len(lakeWorkflows) == 0 {
		return nil, fmt.Errorf("no LakeFlow (lakeflow.io/v1alpha1) found in %s", yamlFile)
	}

	return lakeWorkflows, nil
}

// LoadFirstLakeFlowFromYaml loads the first LakeFlow resource from a YAML file
// If the file contains multiple LakeFlow resources, only the first one is returned
func LoadFirstLakeFlowFromYaml(schema *runtime.Scheme, yamlFile string) (*v1alpha1.LakeFlow, error) {
	lakeWorkflows, err := LoadLakeFlowsFromYaml(schema, yamlFile)
	if err != nil {
		return nil, err
	}

	return lakeWorkflows[0], nil
}

// LoadFromLakeFlowYaml loads the first LakeFlow resource from a YAML file
// Note: If the file contains multiple LakeFlow resources, only the first one is returned.
// Use LoadLakeFlowsFromYaml() if you need to load all LakeFlow resources from the file.
func LoadFromLakeFlowYaml(schema *runtime.Scheme, yamlFile string) (*v1alpha1.LakeFlow, error) {
	return LoadFirstLakeFlowFromYaml(schema, yamlFile)
}

// LoadAndValidateLakeFlowFromYaml is a higher-level function that loads the first LakeFlow
// from a YAML file and performs additional validation
func LoadAndValidateLakeFlowFromYaml(schema *runtime.Scheme, yamlFile string) (*v1alpha1.LakeFlow, error) {
	// Load the LakeFlow from YAML
	lw, err := LoadFromLakeFlowYaml(schema, yamlFile)
	if err != nil {
		return nil, err
	}

	// Perform additional validation
	if err := validateLakeFlow(lw); err != nil {
		return nil, fmt.Errorf("validation failed for LakeFlow in %s: %w", yamlFile, err)
	}

	return lw, nil
}

// LoadAndValidateLakeFlowsFromYaml is a higher-level function that loads all LakeFlows
// from a YAML file and performs additional validation on each
func LoadAndValidateLakeFlowsFromYaml(schema *runtime.Scheme, yamlFile string) ([]*v1alpha1.LakeFlow, error) {
	// Load all LakeFlows from YAML
	lws, err := LoadLakeFlowsFromYaml(schema, yamlFile)
	if err != nil {
		return nil, err
	}

	// Validate each LakeFlow
	for i, lw := range lws {
		if err := validateLakeFlow(lw); err != nil {
			return nil, fmt.Errorf("validation failed for LakeFlow %d (%s) in %s: %w", i, lw.Name, yamlFile, err)
		}
	}

	return lws, nil
}

// validateLakeFlow performs additional validation on a LakeFlow object
func validateLakeFlow(lw *v1alpha1.LakeFlow) error {
	// Validate task executors
	for i, task := range lw.Spec.Tasks {
		switch task.Executor {
		case v1alpha1.TaskExecutorBash, v1alpha1.TaskExecutorPython:
			// For command executors, ensure at least one execution mode is specified
			cmdSpec := task.CommandExecutorSpec
			if cmdSpec.ScriptFileMode == nil && cmdSpec.InlineMode == nil {
				return fmt.Errorf("task %d (%s): %s executor requires one of scriptFileMode or inlineMode", i, task.Name, task.Executor)
			}

			// Validate that only one execution mode is specified (enforced by CRD CEL validation)
			// Note: CRD validation rule ensures exactly one mode is specified
			if cmdSpec.ScriptFileMode != nil {
				// Validate script file mode
				if cmdSpec.ScriptFileMode.ScriptPath == "" {
					return fmt.Errorf("task %d (%s): scriptFileMode requires scriptPath", i, task.Name)
				}
			}
			if cmdSpec.InlineMode != nil {
				// Validate inline mode
				if len(cmdSpec.InlineMode.Command) == 0 {
					return fmt.Errorf("task %d (%s): inlineMode requires command", i, task.Name)
				}
			}

		case v1alpha1.TaskExecutorSpark:
			// Validate Spark application type and MainClass requirements
			sparkAppType := task.SparkExecutorSpec.SparkAppType
			if sparkAppType == "" {
				sparkAppType = v1alpha1.SparkAppTypeJava // Default to Java
			}

			// MainClass is only required for Java/Scala applications, not for Python
			if sparkAppType == v1alpha1.SparkAppTypeJava && task.SparkExecutorSpec.MainClass == "" {
				return fmt.Errorf("task %d (%s): spark Java executor requires mainClass", i, task.Name)
			}

			// MainClass should be empty for Python applications
			if sparkAppType == v1alpha1.SparkAppTypePython && task.SparkExecutorSpec.MainClass != "" {
				return fmt.Errorf("task %d (%s): spark Python executor should not have mainClass", i, task.Name)
			}

			// MainApplicationFile is always required
			if task.SparkExecutorSpec.MainApplicationFile == "" {
				return fmt.Errorf("task %d (%s): spark executor requires mainApplicationFile", i, task.Name)
			}
		default:
			return fmt.Errorf("task %d (%s): unknown executor type: %s", i, task.Name, task.Executor)
		}
	}

	// Validate dependencies
	taskNames := make(map[string]bool)
	for _, task := range lw.Spec.Tasks {
		taskNames[task.Name] = true
	}

	for i, task := range lw.Spec.Tasks {
		for _, dep := range task.DependsOn {
			if !taskNames[dep] {
				return fmt.Errorf("task %d (%s): dependency '%s' not found in task list", i, task.Name, dep)
			}
		}
	}

	return nil
}

func ToYaml(obj runtime.Object, scheme *runtime.Scheme) (string, error) {
	ser := json.NewSerializerWithOptions(
		json.DefaultMetaFactory, scheme, scheme,
		json.SerializerOptions{
			Yaml:   true,
			Pretty: false,
			Strict: false,
		})

	var buf bytes.Buffer
	if err := ser.Encode(obj, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// PrettyYaml returns a pretty-printed YAML string for a runtime object. for test
func PrettyYaml(obj runtime.Object, scheme *runtime.Scheme) (string, error) {
	ser := json.NewSerializerWithOptions(
		json.DefaultMetaFactory, scheme, scheme,
		json.SerializerOptions{
			Yaml:   true,  // switch to YAML
			Pretty: true,  // 2-space indent, sorted keys
			Strict: false, // tolerate unknown fields
		})

	var buf bytes.Buffer
	if err := ser.Encode(obj, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func NewConfig() (*rest.Config, error) {
	var config *rest.Config
	var err error
	if masterUrl := os.Getenv(KubeMasterUrlEnv); len(masterUrl) > 0 {
		config, err = clientcmd.BuildConfigFromFlags(masterUrl, "")
	} else if kubeConf := os.Getenv(clientcmd.RecommendedConfigPathEnvVar); len(kubeConf) > 0 {
		config, err = clientcmd.BuildConfigFromFlags("", kubeConf)
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			if home := homedir.HomeDir(); len(home) > 0 {
				logger.Info("KubeWrapper NewConfig using default kube config path", "path", home+"/.kube/config")
				defaultConfigPath := filepath.Join(home, ".kube", "config")
				config, err = clientcmd.BuildConfigFromFlags("", defaultConfigPath)
			} else {
				logger.Error(err, "KubeWrapper Failed to init clientConfig")
			}
		}
	}
	return config, err
}

func NewKubClientSet() (*kubernetes.Clientset, error) {
	if config, err := NewConfig(); err != nil {
		return nil, err
	} else {
		return kubernetes.NewForConfig(config)
	}
}

func NewCrdClient() (crdv1.CustomResourceDefinitionInterface, error) {
	config, err := NewConfig()
	if err != nil {
		return nil, err
	} else {
		apiExtClientSet, apiExtErr := apiextensionsv1.NewForConfig(config)
		if apiExtErr == nil {
			return apiExtClientSet.ApiextensionsV1().CustomResourceDefinitions(), nil
		}
		return nil, apiExtErr
	}
}

func GetArgoResourceType(resource client.Object) string {
	switch resource.(type) {
	case *argowfv1.WorkflowTemplate:
		return "WorkflowTemplate"
	case *argowfv1.Workflow:
		return "Workflow"
	case *argowfv1.CronWorkflow:
		return "CronWorkflow"
	default:
		return "Unknown"
	}
}

func GetResourceName(resource client.Object) string {
	name := resource.GetName()
	if name == "" {
		return "<unnamed>"
	}
	return name
}
