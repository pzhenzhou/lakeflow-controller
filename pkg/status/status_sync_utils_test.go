package status

import (
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestUpdateStatusReferencesClearsStaleCronWorkflowWhenNotScheduled(t *testing.T) {
	handler := &reconcileStateHandler{}
	lw := &v1alpha1.LakeFlow{}
	lw.Name = "example"
	lw.Spec.WorkflowTrigger.Schedule.Cron = "" // immediate
	lw.Status.CronWorkflow = "old-cron"
	lw.Status.WorkflowName = "wf-keep"

	handler.updateStatusReferences(lw, map[string]string{
		"WorkflowTemplate": "example-template-v2",
	})

	assert.Equal(t, "", lw.Status.CronWorkflow)
	assert.Equal(t, "wf-keep", lw.Status.WorkflowName)
	assert.Equal(t, "example-template-v2", lw.Status.WorkflowTemplate)
}

func TestUpdateStatusReferencesDependencyModeClearsStaleWorkflowName(t *testing.T) {
	handler := &reconcileStateHandler{}
	lw := &v1alpha1.LakeFlow{}
	lw.Name = "example"
	lw.Spec.WorkflowTrigger.Depend.Workflows = []v1alpha1.Upstream{
		{Name: "upstream-a", Namespace: "default"},
	}
	lw.Status.CronWorkflow = "old-cron"
	lw.Status.WorkflowName = "old-immediate-workflow"

	handler.updateStatusReferences(lw, map[string]string{
		"WorkflowTemplate": "example-template-v3",
	})

	assert.Equal(t, "", lw.Status.CronWorkflow)
	assert.Equal(t, "", lw.Status.WorkflowName)
	assert.Equal(t, "example-template-v3", lw.Status.WorkflowTemplate)
}

func TestUpdateStatusReferencesScheduleModeClearsWorkflowName(t *testing.T) {
	handler := &reconcileStateHandler{}
	lw := &v1alpha1.LakeFlow{}
	lw.Name = "example"
	lw.Spec.WorkflowTrigger.Schedule.Cron = "*/5 * * * *"
	lw.Status.WorkflowName = "stale-dependency-workflow"

	handler.updateStatusReferences(lw, map[string]string{
		"CronWorkflow": "example-cron",
	})

	assert.Equal(t, "example-cron", lw.Status.CronWorkflow)
	assert.Equal(t, "", lw.Status.WorkflowName)
}

func TestIsScheduledTriggerUsesSpecOnly(t *testing.T) {
	lw := &v1alpha1.LakeFlow{}
	lw.Status.CronWorkflow = "stale-cron"
	assert.False(t, isScheduledTrigger(lw))

	lw.Spec.WorkflowTrigger.Schedule.Cron = "0 * * * *"
	lw.Status.CronWorkflow = ""
	assert.True(t, isScheduledTrigger(lw))
}
