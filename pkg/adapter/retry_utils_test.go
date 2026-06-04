package adapter

import (
	"testing"
	"time"

	argowfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestBuildRetryStrategy(t *testing.T) {
	tests := []struct {
		name     string
		task     *v1alpha1.Task
		expected *argowfv1.RetryStrategy
	}{
		{
			name: "nil retryPolicy should return nil",
			task: &v1alpha1.Task{
				Name:        "test-task",
				RetryPolicy: nil,
			},
			expected: nil,
		},
		{
			name: "maxRetries=0 should explicitly set limit to 0 (not return nil)",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries: 0,
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(0),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
			},
		},
		{
			name: "maxRetries=3 without backoff",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries: 3,
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(3),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
			},
		},
		{
			name: "maxRetries=5 with backoff configuration",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries:    5,
					BackOff:       metav1.Duration{Duration: 2 * time.Minute},
					BackoffFactor: 2.0,
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(5),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
				Backoff: &argowfv1.Backoff{
					Duration: "2m0s",
					Factor:   intstrPtr(2), // Whole number uses integer format
				},
			},
		},
		{
			name: "maxRetries=1 with backoff",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries:    1,
					BackOff:       metav1.Duration{Duration: 30 * time.Second},
					BackoffFactor: 1.5,
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(1),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
				Backoff: &argowfv1.Backoff{
					Duration: "30s",
					Factor:   intstrStringPtr("1.5"),
				},
			},
		},
		{
			name: "maxRetries=0 with backoff configuration should ignore backoff",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries:    0,
					BackOff:       metav1.Duration{Duration: 2 * time.Minute},
					BackoffFactor: 2.0,
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(0),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
				// Backoff should not be set when maxRetries is 0
				Backoff: nil,
			},
		},
		{
			name: "maxRetries=3 with backoff and maxDuration",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries:    3,
					BackOff:       metav1.Duration{Duration: 1 * time.Minute},
					BackoffFactor: 2.0,
					MaxDuration:   metav1.Duration{Duration: 5 * time.Minute},
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(3),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
				Backoff: &argowfv1.Backoff{
					Duration:    "1m0s",
					Factor:      intstrPtr(2), // Whole number uses integer format
					MaxDuration: "5m0s",
				},
			},
		},
		{
			name: "maxRetries>0 with backoff but no maxDuration should not set maxDuration",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries:    3,
					BackOff:       metav1.Duration{Duration: 1 * time.Minute},
					BackoffFactor: 2.0,
					MaxDuration:   metav1.Duration{Duration: 0}, // Not set
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(3),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
				Backoff: &argowfv1.Backoff{
					Duration:    "1m0s",
					Factor:      intstrPtr(2), // Whole number uses integer format
					MaxDuration: "",           // Should be empty string when not set
				},
			},
		},
		{
			name: "maxDuration without backoff should be ignored (backoff not initialized)",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries:  3,
					MaxDuration: metav1.Duration{Duration: 5 * time.Minute},
					// No BackOff specified
				},
			},
			expected: &argowfv1.RetryStrategy{
				Limit:       intstrPtr(3),
				RetryPolicy: argowfv1.RetryPolicyOnFailure,
				Backoff:     nil, // Backoff not initialized, so MaxDuration is ignored
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildRetryStrategy(tt.task)

			// Check if both are nil
			if tt.expected == nil && result == nil {
				return
			}

			// Check if one is nil but not the other
			if (tt.expected == nil) != (result == nil) {
				t.Errorf("Expected nil mismatch: expected=%v, got=%v", tt.expected == nil, result == nil)
				return
			}

			// Compare Limit
			if !compareIntstrPtr(tt.expected.Limit, result.Limit) {
				t.Errorf("Limit mismatch: expected=%v, got=%v",
					formatIntstrPtr(tt.expected.Limit),
					formatIntstrPtr(result.Limit))
			}

			// Compare RetryPolicy
			if tt.expected.RetryPolicy != result.RetryPolicy {
				t.Errorf("RetryPolicy mismatch: expected=%v, got=%v",
					tt.expected.RetryPolicy, result.RetryPolicy)
			}

			// Compare Backoff
			if !compareBackoff(tt.expected.Backoff, result.Backoff) {
				t.Errorf("Backoff mismatch: expected=%v, got=%v",
					formatBackoff(tt.expected.Backoff),
					formatBackoff(result.Backoff))
			}
		})
	}
}

func TestApplySparkRetryDefaults(t *testing.T) {
	tests := []struct {
		name     string
		task     *v1alpha1.Task
		expected *v1alpha1.TaskRetryPolicy
	}{
		{
			name: "nil retryPolicy should be set to fail-fast (maxRetries=0)",
			task: &v1alpha1.Task{
				Name:        "test-task",
				RetryPolicy: nil,
			},
			// Spark tasks always enforce fail-fast behavior
			expected: &v1alpha1.TaskRetryPolicy{
				MaxRetries: 0,
			},
		},
		{
			name: "maxRetries=0 should remain fail-fast",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries: 0,
				},
			},
			expected: &v1alpha1.TaskRetryPolicy{
				MaxRetries: 0,
			},
		},
		{
			name: "maxRetries>0 should be ignored and forced to fail-fast",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries: 3,
				},
			},
			// User-configured retries are ignored for Spark tasks
			expected: &v1alpha1.TaskRetryPolicy{
				MaxRetries: 0,
			},
		},
		{
			name: "maxRetries>0 with backoff should be ignored and forced to fail-fast",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries: 3,
					BackOff:    metav1.Duration{Duration: 10 * time.Second},
				},
			},
			// User-configured retries and backoff are ignored for Spark tasks
			expected: &v1alpha1.TaskRetryPolicy{
				MaxRetries: 0,
			},
		},
		{
			name: "any retry configuration should be ignored for Spark tasks",
			task: &v1alpha1.Task{
				Name: "test-task",
				RetryPolicy: &v1alpha1.TaskRetryPolicy{
					MaxRetries:    5,
					BackOff:       metav1.Duration{Duration: 2 * time.Minute},
					BackoffFactor: 3.0,
				},
			},
			// All retry configuration is ignored - Spark tasks always fail fast
			expected: &v1alpha1.TaskRetryPolicy{
				MaxRetries: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applySparkRetryDefaults(tt.task)

			// Check if both are nil
			if tt.expected == nil && tt.task.RetryPolicy == nil {
				return
			}

			// Check if one is nil but not the other
			if (tt.expected == nil) != (tt.task.RetryPolicy == nil) {
				t.Errorf("Expected nil mismatch: expected=%v, got=%v",
					tt.expected == nil, tt.task.RetryPolicy == nil)
				return
			}

			// Compare fields
			if tt.expected.MaxRetries != tt.task.RetryPolicy.MaxRetries {
				t.Errorf("MaxRetries mismatch: expected=%d, got=%d",
					tt.expected.MaxRetries, tt.task.RetryPolicy.MaxRetries)
			}

			if tt.expected.BackOff.Duration != tt.task.RetryPolicy.BackOff.Duration {
				t.Errorf("BackOff mismatch: expected=%v, got=%v",
					tt.expected.BackOff.Duration, tt.task.RetryPolicy.BackOff.Duration)
			}

			if tt.expected.BackoffFactor != tt.task.RetryPolicy.BackoffFactor {
				t.Errorf("BackoffFactor mismatch: expected=%v, got=%v",
					tt.expected.BackoffFactor, tt.task.RetryPolicy.BackoffFactor)
			}
		})
	}
}

// Helper functions for test comparisons

func intstrPtr(val int32) *intstr.IntOrString {
	i := intstr.FromInt32(val)
	return &i
}

func intstrStringPtr(val string) *intstr.IntOrString {
	i := intstr.FromString(val)
	return &i
}

func compareIntstrPtr(a, b *intstr.IntOrString) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.IntVal == b.IntVal && a.StrVal == b.StrVal && a.Type == b.Type
}

func compareBackoff(a, b *argowfv1.Backoff) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Duration != b.Duration {
		return false
	}
	if a.MaxDuration != b.MaxDuration {
		return false
	}
	return compareIntstrPtr(a.Factor, b.Factor)
}

func formatIntstrPtr(i *intstr.IntOrString) string {
	if i == nil {
		return "nil"
	}
	if i.Type == intstr.Int {
		return i.String()
	}
	return i.StrVal
}

func formatBackoff(b *argowfv1.Backoff) string {
	if b == nil {
		return "nil"
	}
	result := "Duration=" + b.Duration + ", Factor=" + formatIntstrPtr(b.Factor)
	if b.MaxDuration != "" {
		result += ", MaxDuration=" + b.MaxDuration
	}
	return result
}
