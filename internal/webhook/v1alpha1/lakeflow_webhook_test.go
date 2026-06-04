/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"context"
	"strings"
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func validSparkTask(name string) v1alpha1.Task {
	res := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}
	return v1alpha1.Task{
		Name:     name,
		Executor: v1alpha1.TaskExecutorSpark,
		TaskSpec: v1alpha1.TaskSpec{
			SparkExecutorSpec: &v1alpha1.SparkExecutorSpec{
				Image:               "spark:3.5.6",
				MainClass:           "com.example.Main",
				MainApplicationFile: "local:///mnt/spark/app.jar",
				QueueName:           "default",
				DriverResource:      v1alpha1.SparkResource{Resources: res},
				ExecutorResource:    v1alpha1.SparkResource{Resources: res},
			},
		},
	}
}

func validLakeFlow() *v1alpha1.LakeFlow {
	return &v1alpha1.LakeFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf", Namespace: "ns"},
		Spec: v1alpha1.LakeFlowSpec{
			Tasks: []v1alpha1.Task{validSparkTask("task-a")},
		},
	}
}

// assertInvalidWithField asserts the error is a StatusError Invalid carrying a
// cause for a field path containing fieldSubstr.
func assertInvalidWithField(t *testing.T, err error, fieldSubstr string) {
	t.Helper()
	require.Error(t, err)
	require.True(t, apierrors.IsInvalid(err), "expected an Invalid status error, got %v", err)
	statusErr, ok := err.(*apierrors.StatusError)
	require.True(t, ok, "expected *StatusError, got %T", err)
	require.NotNil(t, statusErr.ErrStatus.Details)
	for _, cause := range statusErr.ErrStatus.Details.Causes {
		if cause.Field != "" && strings.Contains(cause.Field, fieldSubstr) {
			return
		}
	}
	t.Fatalf("expected a cause for field containing %q, got causes: %v", fieldSubstr, statusErr.ErrStatus.Details.Causes)
}

func TestValidateCreate_Valid(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	warnings, err := v.ValidateCreate(context.Background(), validLakeFlow())
	assert.Nil(t, warnings)
	assert.NoError(t, err)
}

func TestValidateCreate_WrongType(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	_, err := v.ValidateCreate(context.Background(), &corev1.Pod{})
	require.Error(t, err)
	assert.False(t, apierrors.IsInvalid(err))
}

func TestValidateCreate_TooManyTasks(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	lw := validLakeFlow()
	lw.Spec.Tasks = nil
	for i := 0; i <= 30; i++ {
		lw.Spec.Tasks = append(lw.Spec.Tasks, validSparkTask("task-"+string(rune('a'+i%26))+string(rune('a'+i/26))))
	}
	_, err := v.ValidateCreate(context.Background(), lw)
	assertInvalidWithField(t, err, "spec.tasks")
}

func TestValidateCreate_TaskNameTooLong(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	lw := validLakeFlow()
	long := ""
	for i := 0; i < 41; i++ {
		long += "a"
	}
	lw.Spec.Tasks[0].Name = long
	_, err := v.ValidateCreate(context.Background(), lw)
	assertInvalidWithField(t, err, "tasks[0].name")
}

func TestValidateUpdate_TenantKeyImmutable(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	oldLw := validLakeFlow()
	oldLw.Spec.TenantKey = "t1"
	newLw := validLakeFlow()
	newLw.Spec.TenantKey = "t2"
	_, err := v.ValidateUpdate(context.Background(), oldLw, newLw)
	assertInvalidWithField(t, err, "spec.tenantKey")
}

func TestValidateUpdate_TriggerMutable(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	oldLw := validLakeFlow()
	newLw := validLakeFlow()
	newLw.Spec.WorkflowTrigger.Schedule.Cron = "* * * * *"
	_, err := v.ValidateUpdate(context.Background(), oldLw, newLw)
	assert.NoError(t, err)
}

func TestValidateUpdate_WrongType(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	var notLakeFlow runtime.Object = &corev1.Pod{}
	_, err := v.ValidateUpdate(context.Background(), notLakeFlow, validLakeFlow())
	require.Error(t, err)
}

func TestValidateDelete_NoOp(t *testing.T) {
	v := &LakeFlowCustomValidator{}
	warnings, err := v.ValidateDelete(context.Background(), validLakeFlow())
	assert.Nil(t, warnings)
	assert.NoError(t, err)
}
