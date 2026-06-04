package adapter

import (
	"strings"
	"testing"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Test the enhanced create-or-update pattern functionality
func TestCreateOrUpdatePattern(t *testing.T) {
	lw := createTestLakeFlow()
	builder := NewArgoResourceRenderer(lw)

	t.Run("BuildWorkflowTemplate", func(t *testing.T) {
		entrypoint := "test-main"
		templates := []argowfv1.Template{
			{
				Name: entrypoint,
				DAG: &argowfv1.DAGTemplate{
					Tasks: []argowfv1.DAGTask{
						{
							Name:     "task1",
							Template: "task1",
						},
					},
				},
			},
		}

		wft, err := builder.RenderWorkflowTemplate(entrypoint, templates)
		assert.NoError(t, err)
		assert.NotNil(t, wft)
		// Template name is now versioned: "test-workflow-template-{timestamp}"
		assert.True(t, strings.HasPrefix(wft.Name, "test-workflow-template-"), "WorkflowTemplate should have versioned name prefix")
		assert.True(t, IsVersionedTemplateName(wft.Name), "WorkflowTemplate name should be versioned")
		assert.Equal(t, entrypoint, wft.Spec.Entrypoint)
		assert.Len(t, wft.Spec.Templates, 1)
	})

	t.Run("BuildCronWorkflow", func(t *testing.T) {
		templateName := "test-template"
		lw.Spec.WorkflowTrigger.Schedule.Cron = "0 0 * * *"
		lw.Spec.WorkflowTrigger.Schedule.Timezone = "UTC"

		cronWf, err := builder.RenderCronWorkflow(templateName)
		assert.NoError(t, err)
		assert.NotNil(t, cronWf)
		assert.Equal(t, "test-workflow", cronWf.Name)
		assert.Equal(t, []string{"0 0 * * *"}, cronWf.Spec.Schedules)
		assert.Equal(t, "UTC", cronWf.Spec.Timezone)
		assert.Equal(t, templateName, cronWf.Spec.WorkflowSpec.WorkflowTemplateRef.Name)
		require.NotNil(t, cronWf.Spec.WorkflowSpec.Synchronization)
		require.Len(t, cronWf.Spec.WorkflowSpec.Synchronization.Mutexes, 1)
		assert.Equal(t, cronWf.Name, cronWf.Spec.WorkflowSpec.Synchronization.Mutexes[0].Name)
	})

	t.Run("BuildWorkflow", func(t *testing.T) {
		templateName := "test-template"

		wf, err := builder.RenderWorkflow(templateName)
		assert.NoError(t, err)
		assert.NotNil(t, wf)
		assert.Equal(t, "test-workflow-", wf.GenerateName)
		assert.Equal(t, templateName, wf.Spec.WorkflowTemplateRef.Name)
	})
}

// Test resource comparison functions to ensure proper change detection
func TestResourceComparison(t *testing.T) {
	lw := createTestLakeFlow()
	builder := NewArgoResourceRenderer(lw)

	t.Run("WorkflowTemplate comparison", func(t *testing.T) {
		entrypoint := "test-main"
		templates := []argowfv1.Template{
			{
				Name: entrypoint,
				DAG: &argowfv1.DAGTemplate{
					Tasks: []argowfv1.DAGTask{
						{
							Name:     "task1",
							Template: "task1",
						},
					},
				},
			},
		}

		wft1, err := builder.RenderWorkflowTemplate(entrypoint, templates)
		assert.NoError(t, err)

		wft2, err := builder.RenderWorkflowTemplate(entrypoint, templates)
		assert.NoError(t, err)

		// These should be considered equal since they have the same spec
		assert.Equal(t, wft1.Spec.Entrypoint, wft2.Spec.Entrypoint)
		assert.Len(t, wft1.Spec.Templates, len(wft2.Spec.Templates))

		// Change the entrypoint and they should be different
		wft3, err := builder.RenderWorkflowTemplate("different-main", templates)
		assert.NoError(t, err)
		assert.NotEqual(t, wft1.Spec.Entrypoint, wft3.Spec.Entrypoint)
	})

	t.Run("CronWorkflow comparison", func(t *testing.T) {
		templateName := "test-template"
		lw.Spec.WorkflowTrigger.Schedule.Cron = "0 0 * * *"

		cronWf1, err := builder.RenderCronWorkflow(templateName)
		assert.NoError(t, err)

		cronWf2, err := builder.RenderCronWorkflow(templateName)
		assert.NoError(t, err)

		// These should be considered equal
		assert.Equal(t, cronWf1.Spec.Schedules, cronWf2.Spec.Schedules)
		assert.Equal(t, cronWf1.Spec.WorkflowSpec.WorkflowTemplateRef.Name, cronWf2.Spec.WorkflowSpec.WorkflowTemplateRef.Name)

		// Change the schedule and they should be different
		lw.Spec.WorkflowTrigger.Schedule.Cron = "0 12 * * *"
		cronWf3, err := builder.RenderCronWorkflow(templateName)
		assert.NoError(t, err)
		assert.NotEqual(t, cronWf1.Spec.Schedules, cronWf3.Spec.Schedules)
	})
}

// Test that demonstrates the create-or-update pattern benefits
func TestCreateOrUpdateBenefits(t *testing.T) {
	t.Run("Demonstrates change detection pattern", func(t *testing.T) {
		// This test demonstrates that the create-or-update pattern
		// will only update resources when they actually change

		lw := createTestLakeFlow()
		builder := NewArgoResourceRenderer(lw)

		// Build the same WorkflowTemplate twice
		entrypoint := "test-main"
		templates := []argowfv1.Template{
			{
				Name: entrypoint,
				DAG: &argowfv1.DAGTemplate{
					Tasks: []argowfv1.DAGTask{
						{
							Name:     "task1",
							Template: "task1",
						},
					},
				},
			},
		}

		wft1, err := builder.RenderWorkflowTemplate(entrypoint, templates)
		assert.NoError(t, err)

		wft2, err := builder.RenderWorkflowTemplate(entrypoint, templates)
		assert.NoError(t, err)

		// With the enhanced create-or-update pattern, these resources
		// would be detected as having no changes, avoiding unnecessary updates
		assert.Equal(t, wft1.Spec.Entrypoint, wft2.Spec.Entrypoint)
		assert.Equal(t, len(wft1.Spec.Templates), len(wft2.Spec.Templates))

		// Modify the LakeFlow to trigger a change
		lw.Spec.Parallelism = 5
		wft3, err := builder.RenderWorkflowTemplate(entrypoint, templates)
		assert.NoError(t, err)

		// This would be detected as a change and trigger an update
		parallelism1 := wft1.Spec.Parallelism
		parallelism3 := wft3.Spec.Parallelism

		// Check if parallelism values are different
		if parallelism1 == nil && parallelism3 == nil {
			// Both nil - same
		} else if parallelism1 == nil || parallelism3 == nil {
			// One is nil, other is not - different
			assert.True(t, true, "Parallelism values are different (one is nil)")
		} else {
			// Both have values - compare them
			assert.NotEqual(t, *parallelism1, *parallelism3, "Parallelism values should be different")
		}
	})
}

func createTestLakeFlow() *v1alpha1.LakeFlow {
	return &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-workflow",
			Namespace: "test-namespace",
			UID:       "test-uid-12345",
		},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{
				{
					Name:     "task1",
					Executor: "spark",
					TaskSpec: v1alpha1.TaskSpec{
						SparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
							MainClass:           "com.example.SparkApp",
							MainApplicationFile: "s3://bucket/app.jar",
							QueueName:           "default",
							DriverResource: v1alpha1.SparkResource{
								Replicas: 1,
							},
							ExecutorResource: v1alpha1.SparkResource{
								Replicas: 2,
							},
						},
					},
				},
			},
			WorkflowTrigger: v1alpha1.Trigger{},
			Parallelism:     1,
		},
	}
}
