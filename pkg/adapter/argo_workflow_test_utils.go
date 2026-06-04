package adapter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pzhenzhou/lakeflow-controller/pkg/common"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/adapter/argoclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var scheme = runtime.NewScheme()

// init registers the test schemes up front so helpers that load fixtures
// (loadExampleLakeFlow/loadMultiExampleWorkflow) work regardless of test
// execution order. Previously the shared scheme was only populated as a side
// effect of applyArgoWorkflow, so YAML-loading tests panicked ("no kind
// LakeFlow is registered") when they ran before any cluster-applying test.
func init() {
	registerSchemes()
}

func registerSchemes() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	_ = argowfv1.AddToScheme(scheme)
}

// Helper function to get value or 0 if pointer is nil
func getValue(ptr *int32) int32 {
	if ptr != nil {
		return *ptr
	}
	return 0
}

func loadMultiExampleWorkflow(filename string) []*v1alpha1.LakeFlow {
	examplePath := filepath.Join("../../example/lakeflow", filename)

	lw, err := common.LoadLakeFlowsFromYaml(scheme, examplePath)
	if err != nil {
		panic(fmt.Sprintf("failed to load multi example workflow: %v", err))
	}
	return lw
}

func loadExampleLakeFlow(filename string) *v1alpha1.LakeFlow {
	// Construct the path to the example file
	examplePath := filepath.Join("../../example/lakeflow", filename)

	// Load the LakeFlow from the YAML file using the utility function
	lw, err := common.LoadFromLakeFlowYaml(scheme, examplePath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load example LakeFlow from %s: %v", examplePath, err))
	}

	return lw
}

func printArgoWorkflow(argoResources *ArgoWorkflowCR) {
	fmt.Println("=== Generated Argo Resources ===")
	for i, obj := range argoResources.GetResources() {
		// Get object type and identifier
		objKind := obj.GetObjectKind().GroupVersionKind().Kind
		if objKind == "" {
			// Fallback to determine type from the object itself
			switch obj.(type) {
			case *argowfv1.WorkflowTemplate:
				objKind = "WorkflowTemplate"
			case *argowfv1.Workflow:
				objKind = "Workflow"
			case *argowfv1.CronWorkflow:
				objKind = "CronWorkflow"
			default:
				objKind = "Unknown"
			}
		}

		// Get name or generateName
		name := obj.GetName()
		if name == "" {
			generateName := obj.GetGenerateName()
			if generateName != "" {
				name = fmt.Sprintf("<generateName: %s>", generateName)
			} else {
				name = "<unnamed>"
			}
		}

		fmt.Printf("\n--- Object %d: %s %s ---\n", i+1, objKind, name)

		// Generate pretty YAML
		objYaml, err := common.PrettyYaml(obj, scheme)
		if err != nil {
			fmt.Printf("Error generating YAML: %v\n", err)
			continue
		}

		fmt.Println(objYaml)
	}
}

// getRestConfig creates a rest.Config for testing purposes
func getRestConfig() (*rest.Config, error) {
	var config *rest.Config
	var err error

	if masterUrl := os.Getenv("KUBERNETES_MASTER"); len(masterUrl) > 0 {
		config, err = clientcmd.BuildConfigFromFlags(masterUrl, "")
	} else if kubeConf := os.Getenv(clientcmd.RecommendedConfigPathEnvVar); len(kubeConf) > 0 {
		config, err = clientcmd.BuildConfigFromFlags("", kubeConf)
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			if home := homedir.HomeDir(); len(home) > 0 {
				defaultConfigPath := filepath.Join(home, ".kube", "config")
				config, err = clientcmd.BuildConfigFromFlags("", defaultConfigPath)
			}
		}
	}
	return config, err
}

// createArgoResourceClient creates a new ArgoResourceClient for testing
func createArgoResourceClient() (argoclient.ArgoResourceClient, error) {
	restConfig, err := getRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get rest config: %w", err)
	}

	return argoclient.NewArgoResourceClient(restConfig)
}

// applyArgoWorkflow applies all Argo workflow resources to the cluster
func applyArgoWorkflow(argoResources *ArgoWorkflowCR) (bool, string) {
	registerSchemes()
	resourceManager, err := createArgoResourceClient()
	if err != nil {
		return false, fmt.Sprintf("infrastructure_error: Failed to create ArgoResourceClient: %v", err)
	}
	ctx := context.Background()

	// First, check if Argo Workflows CRDs are available
	if !checkArgoCRDsAvailable(ctx, resourceManager) {
		fmt.Println("❌ Argo Workflows CRDs are not available in the cluster")
		fmt.Println("💡 To install Argo Workflows for testing, run: hack/install-argo-workflows.sh")
		return false, "missing_crds: Argo Workflows CRDs are not available in the cluster"
	}

	fmt.Println("✅ Argo Workflows CRDs are available")

	// Apply resources in order: WorkflowTemplate first, then other resources
	var createdResources []client.Object
	var createdWorkflows []*argowfv1.Workflow

	// Create WorkflowTemplate first
	if argoResources.WorkflowTemplate != nil {
		fmt.Printf("Creating WorkflowTemplate: %s/%s in namespace: %s\n",
			"WorkflowTemplate",
			argoResources.WorkflowTemplate.GetName(),
			argoResources.WorkflowTemplate.GetNamespace())

		err := resourceManager.Create(ctx, argoResources.WorkflowTemplate)
		if err != nil {
			fmt.Printf("❌ Failed to create WorkflowTemplate %s: %v\n", argoResources.WorkflowTemplate.GetName(), err)
			return false, fmt.Sprintf("creation_failed: Failed to create WorkflowTemplate: %v", err)
		}

		fmt.Printf("✓ Successfully created WorkflowTemplate: %s\n", argoResources.WorkflowTemplate.GetName())
		createdResources = append(createdResources, argoResources.WorkflowTemplate)

		// Verify the template exists with retry logic
		fmt.Println("⏳ Verifying WorkflowTemplate availability with retries...")
		if !verifyResourceStatusWithRetry(ctx, resourceManager, argoResources.WorkflowTemplate, 5, 2*time.Second) {
			fmt.Printf("❌ Failed to verify WorkflowTemplate %s after creation with retries\n", argoResources.WorkflowTemplate.GetName())
			return false, fmt.Sprintf("verification_failed: WorkflowTemplate not found after creation with retries")
		}
	}

	// Create other resources (Workflow, CronWorkflow)
	var remainingResources []client.Object
	if argoResources.Workflow != nil {
		remainingResources = append(remainingResources, argoResources.Workflow)
	}
	if argoResources.CronWorkflow != nil {
		remainingResources = append(remainingResources, argoResources.CronWorkflow)
	}

	for _, resource := range remainingResources {
		fmt.Printf("Creating resource: %s/%s in namespace: %s\n",
			resource.GetObjectKind().GroupVersionKind().Kind,
			getResourceName(resource),
			resource.GetNamespace())

		// Debug: show the resource details before creating
		fmt.Printf("  Resource details: Name=%s, GenerateName=%s, Namespace=%s\n",
			resource.GetName(), resource.GetGenerateName(), resource.GetNamespace())

		err := resourceManager.Create(ctx, resource)
		if err != nil {
			fmt.Printf("❌ Failed to create resource %s: %v\n", getResourceName(resource), err)
			fmt.Printf("  Error type: %T\n", err)
			fmt.Printf("  Error details: %+v\n", err)
			return false, fmt.Sprintf("creation_failed: Failed to create resource %s: %v", getResourceName(resource), err)
		}

		fmt.Printf("✓ Successfully created resource: %s\n", getResourceName(resource))

		// Debug: show the resource details after creating
		fmt.Printf("  After creation: Name=%s, GenerateName=%s, Namespace=%s\n",
			resource.GetName(), resource.GetGenerateName(), resource.GetNamespace())

		// Store workflows separately for status checking
		if workflow, ok := resource.(*argowfv1.Workflow); ok {
			createdWorkflows = append(createdWorkflows, workflow)
		}

		// Store the created resource (with potentially updated name for generateName resources)
		createdResources = append(createdResources, resource)
	}

	// Check workflow status for created workflows
	if len(createdWorkflows) > 0 {
		fmt.Println("\n=== Checking Workflow Status ===")
		for _, workflow := range createdWorkflows {
			statusOk, statusErr := waitForWorkflowStatus(ctx, resourceManager, workflow, 30*time.Second)
			if !statusOk {
				fmt.Printf("❌ Workflow %s did not reach a stable state\n", workflow.GetName())
				return false, fmt.Sprintf("workflow_failed: Workflow %s status check failed: %s", workflow.GetName(), statusErr)
			}
		}
	}

	// Verify all created resources
	fmt.Println("\n=== Verifying Created Resources ===")
	allVerified := true
	for _, resource := range createdResources {
		if !verifyResourceStatusWithRetry(ctx, resourceManager, resource, 3, 1*time.Second) {
			allVerified = false
		}
	}

	if !allVerified {
		return false, "verification_failed: Failed to verify one or more resources"
	}

	return true, "success"
}

// waitForWorkflowStatus waits for a workflow to reach a stable state (running, succeeded, or failed)
func waitForWorkflowStatus(ctx context.Context, resourceManager argoclient.ArgoResourceClient, workflow *argowfv1.Workflow, timeout time.Duration) (bool, string) {
	workflowName := workflow.GetName()
	namespace := workflow.GetNamespace()

	fmt.Printf("⏳ Waiting for workflow %s to start...\n", workflowName)

	startTime := time.Now()
	consecutiveNotFoundCount := 0
	for time.Since(startTime) < timeout {
		// Get current workflow status
		currentWorkflow := &argowfv1.Workflow{}
		err := resourceManager.Get(ctx, workflowName, namespace, currentWorkflow)
		if err != nil {
			fmt.Printf("⚠️ Failed to get workflow status: %v\n", err)
			consecutiveNotFoundCount++
			// If we can't find the workflow for too many consecutive attempts, it may have been deleted
			if consecutiveNotFoundCount >= 5 {
				return false, fmt.Sprintf("workflow_disappeared: Workflow %s disappeared after creation (not found for %d consecutive attempts)", workflowName, consecutiveNotFoundCount)
			}
			time.Sleep(2 * time.Second)
			continue
		}

		// Reset count if we successfully got the workflow
		consecutiveNotFoundCount = 0

		phase := currentWorkflow.Status.Phase
		fmt.Printf("📊 Workflow %s status: %s\n", workflowName, phase)

		switch phase {
		case argowfv1.WorkflowRunning:
			fmt.Printf("✅ Workflow %s is running\n", workflowName)
			return true, ""
		case argowfv1.WorkflowSucceeded:
			fmt.Printf("✅ Workflow %s completed successfully\n", workflowName)
			return true, ""
		case argowfv1.WorkflowFailed, argowfv1.WorkflowError:
			fmt.Printf("❌ Workflow %s failed with status: %s\n", workflowName, phase)
			if currentWorkflow.Status.Message != "" {
				fmt.Printf("   Error message: %s\n", currentWorkflow.Status.Message)
			}
			return false, fmt.Sprintf("workflow_failed: Workflow %s failed with status: %s", workflowName, phase)
		case argowfv1.WorkflowPending, "":
			// Still pending, continue waiting
			fmt.Printf("⏳ Workflow %s is still pending, waiting...\n", workflowName)
		default:
			fmt.Printf("⚠️ Workflow %s has unknown status: %s\n", workflowName, phase)
		}

		time.Sleep(2 * time.Second)
	}

	fmt.Printf("⏰ Timeout waiting for workflow %s to start (waited %v)\n", workflowName, timeout)
	return false, fmt.Sprintf("timeout: Timeout waiting for workflow %s to start (waited %v)", workflowName, timeout)
}

// getResourceName handles both named resources and generateName resources
func getResourceName(resource client.Object) string {
	name := resource.GetName()
	if name == "" {
		generateName := resource.GetGenerateName()
		if generateName != "" {
			return fmt.Sprintf("<generateName: %s>", generateName)
		}
		return "<unnamed>"
	}
	return name
}

func verifyWorkflowTemplate(ctx context.Context, resourceManager argoclient.ArgoResourceClient, wt *argowfv1.WorkflowTemplate) bool {
	fmt.Printf("Verifying WorkflowTemplate: %s/%s\n", wt.GetNamespace(), wt.GetName())

	retrievedWt := &argowfv1.WorkflowTemplate{}
	err := resourceManager.Get(ctx, wt.GetName(), wt.GetNamespace(), retrievedWt)
	if err != nil {
		fmt.Printf("❌ Failed to verify WorkflowTemplate %s: %v\n", wt.GetName(), err)
		return false
	}

	fmt.Printf("✓ WorkflowTemplate '%s' verified successfully\n", wt.GetName())
	fmt.Printf("  - Entrypoint: %s\n", retrievedWt.Spec.Entrypoint)
	fmt.Printf("  - Templates: %d\n", len(retrievedWt.Spec.Templates))
	return true
}

// checkArgoCRDsAvailable checks if Argo Workflows CRDs are installed
func checkArgoCRDsAvailable(ctx context.Context, resourceManager argoclient.ArgoResourceClient) bool {
	fmt.Println("🔍 Checking if Argo Workflows CRDs are available...")

	// Get CRD client to list installed CRDs
	crdClient, err := common.NewCrdClient()
	if err != nil {
		fmt.Printf("❌ Failed to create CRD client: %v\n", err)
		return false
	}

	// List all CRDs
	crds, err := crdClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		fmt.Printf("❌ Failed to list CRDs: %v\n", err)
		return false
	}

	// Required Argo Workflows CRDs
	requiredCRDs := []string{
		// Argo Workflows CRDs
		"clusterworkflowtemplates.argoproj.io",
		"cronworkflows.argoproj.io",
		"workflowartifactgctasks.argoproj.io",
		"workflows.argoproj.io",
		"workflowtaskresults.argoproj.io",
		"workflowtasksets.argoproj.io",
		"workflowtemplates.argoproj.io",
	}

	// Check if all required Argo CRDs are present
	foundCRDs := make(map[string]bool)
	for _, crd := range crds.Items {
		crdName := crd.Name
		for _, required := range requiredCRDs {
			if crdName == required {
				foundCRDs[required] = true
				fmt.Printf("✅ Found CRD: %s\n", crdName)
			}
		}
	}

	// Check if all required CRDs are found
	allFound := true
	for _, required := range requiredCRDs {
		if !foundCRDs[required] {
			fmt.Printf("❌ Missing required CRD: %s\n", required)
			allFound = false
		}
	}

	if allFound {
		fmt.Println("✅ All required Argo Workflows CRDs are available")
		return true
	} else {
		fmt.Println("❌ Some required Argo Workflows CRDs are missing")
		return false
	}
}

// verifyResourceStatusWithRetry adds retry logic to resource verification
func verifyResourceStatusWithRetry(ctx context.Context, resourceManager argoclient.ArgoResourceClient, resource client.Object, maxRetries int, retryDelay time.Duration) bool {
	// Get the actual name (important for generateName resources)
	actualName := resource.GetName()
	if actualName == "" {
		fmt.Printf("⚠ Skipping verification for resource with no name: %s\n", getResourceName(resource))
		return false
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			fmt.Printf("🔄 Retry %d/%d for %s\n", attempt, maxRetries, actualName)
			time.Sleep(retryDelay)
		}

		success := false
		switch obj := resource.(type) {
		case *argowfv1.WorkflowTemplate:
			success = verifyWorkflowTemplate(ctx, resourceManager, obj)
		case *argowfv1.Workflow:
			success = verifyWorkflow(ctx, resourceManager, obj)
		case *argowfv1.CronWorkflow:
			success = verifyCronWorkflow(ctx, resourceManager, obj)
		default:
			fmt.Printf("Unknown resource type for verification: %T\n", resource)
			return false
		}

		if success {
			return true
		}

		if attempt < maxRetries {
			fmt.Printf("⏳ Waiting before retry...\n")
		}
	}

	fmt.Printf("❌ Failed to verify resource %s after %d attempts\n", actualName, maxRetries)
	return false
}

func verifyCronWorkflow(ctx context.Context, resourceManager argoclient.ArgoResourceClient, cwf *argowfv1.CronWorkflow) bool {
	fmt.Printf("Verifying CronWorkflow: %s/%s\n", cwf.GetNamespace(), cwf.GetName())

	retrievedCwf := &argowfv1.CronWorkflow{}
	err := resourceManager.Get(ctx, cwf.GetName(), cwf.GetNamespace(), retrievedCwf)
	if err != nil {
		fmt.Printf("❌ Failed to verify CronWorkflow %s: %v\n", cwf.GetName(), err)
		return false
	}

	fmt.Printf("✓ CronWorkflow '%s' verified successfully\n", cwf.GetName())
	fmt.Printf("  - Schedules: %v\n", retrievedCwf.Spec.Schedules)
	fmt.Printf("  - Timezone: %s\n", retrievedCwf.Spec.Timezone)
	fmt.Printf("  - Suspended: %t\n", retrievedCwf.Spec.Suspend)

	if retrievedCwf.Spec.WorkflowSpec.WorkflowTemplateRef != nil {
		fmt.Printf("  - Template Ref: %s\n", retrievedCwf.Spec.WorkflowSpec.WorkflowTemplateRef.Name)
	}

	fmt.Printf("  - ✓ CronWorkflow is properly configured\n")
	return true
}

func verifyWorkflow(ctx context.Context, resourceManager argoclient.ArgoResourceClient, wf *argowfv1.Workflow) bool {
	fmt.Printf("Verifying Workflow: %s/%s\n", wf.GetNamespace(), wf.GetName())

	retrievedWf := &argowfv1.Workflow{}
	err := resourceManager.Get(ctx, wf.GetName(), wf.GetNamespace(), retrievedWf)
	if err != nil {
		fmt.Printf("❌ Failed to verify Workflow %s: %v\n", wf.GetName(), err)
		return false
	}

	fmt.Printf("✓ Workflow '%s' verified successfully\n", wf.GetName())

	// Check workflow status
	status := retrievedWf.Status.Phase
	if status == "" {
		status = "Pending"
	}

	fmt.Printf("  - Status: %s\n", status)
	if retrievedWf.Spec.WorkflowTemplateRef != nil {
		fmt.Printf("  - Template Ref: %s\n", retrievedWf.Spec.WorkflowTemplateRef.Name)
	}

	// Verify workflow is in a valid state
	validStates := []argowfv1.WorkflowPhase{
		argowfv1.WorkflowPending,
		argowfv1.WorkflowRunning,
		argowfv1.WorkflowSucceeded,
		"", // Empty status is valid for newly created workflows
	}

	isValidState := false
	for _, validState := range validStates {
		if status == validState {
			isValidState = true
			break
		}
	}

	if isValidState {
		fmt.Printf("  - ✓ Workflow is in normal working state\n")
		return true
	} else {
		fmt.Printf("  - ❌ Workflow is in unexpected state: %s\n", status)
		return false
	}
}

// cleanupArgoTestResource centralizes the cleanup logic for Argo workflow test resources
// It deletes all resources in the given ArgoWorkflowCR and logs the cleanup progress
func cleanupArgoTestResource(argoResources *ArgoWorkflowCR) {
	fmt.Println("🧹 Cleaning up test resources...")
	resourceManager, err := createArgoResourceClient()
	if err != nil {
		fmt.Printf("⚠️ Failed to create resource manager for cleanup: %v\n", err)
		return
	}
	ctx := context.Background()

	for _, resource := range argoResources.GetResources() {
		err := resourceManager.Delete(ctx, resource.GetName(), resource.GetNamespace(), resource)
		if err != nil {
			fmt.Printf("⚠️ Failed to clean up %s: %v\n", getResourceName(resource), err)
		} else {
			fmt.Printf("✓ Cleaned up %s\n", getResourceName(resource))
		}
	}
}

// createWorkflowUsingClient creates a LakeFlow resource directly in Kubernetes using controller-runtime client
func createWorkflowUsingClient(testWf *v1alpha1.LakeFlow) error {
	// Get Kubernetes config
	config, err := getRestConfig()
	if err != nil {
		return fmt.Errorf("failed to get kubernetes config: %w", err)
	}

	// Create controller-runtime client
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create the LakeFlow resource directly in Kubernetes
	ctx := context.Background()
	err = k8sClient.Create(ctx, testWf)
	if err != nil {
		return fmt.Errorf("failed to create LakeFlow: %w", err)
	}

	fmt.Printf("LakeFlow created successfully: %s/%s\n", testWf.Namespace, testWf.Name)
	fmt.Println("Controller will automatically convert to Argo resources")

	return nil
}
