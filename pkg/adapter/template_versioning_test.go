package adapter

import (
	"testing"
	"time"
)

func TestGenerateVersionedTemplateName(t *testing.T) {
	lwName := "my-workflow"

	// Get current timestamp before and after to bound the result
	beforeTs := time.Now().Unix()
	result := GenerateVersionedTemplateName(lwName)
	afterTs := time.Now().Unix()

	// Verify the name has correct prefix
	expectedPrefix := "my-workflow-template-"
	if len(result) <= len(expectedPrefix) {
		t.Errorf("Generated name too short: %s", result)
		return
	}

	if result[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("Generated name has wrong prefix: got %s, want prefix %s", result, expectedPrefix)
	}

	// Verify the timestamp is within expected range
	ts := GetTemplateTimestamp(result)
	if ts < beforeTs || ts > afterTs {
		t.Errorf("Timestamp out of expected range: got %d, want between %d and %d", ts, beforeTs, afterTs)
	}
}

func TestGenerateVersionedTemplateNameWithTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		lwName    string
		timestamp int64
		expected  string
	}{
		{
			name:      "simple name",
			lwName:    "my-workflow",
			timestamp: 1736150400,
			expected:  "my-workflow-template-1736150400",
		},
		{
			name:      "name with hyphens",
			lwName:    "my-complex-workflow-name",
			timestamp: 1736150400,
			expected:  "my-complex-workflow-name-template-1736150400",
		},
		{
			name:      "single word name",
			lwName:    "workflow",
			timestamp: 1000000000,
			expected:  "workflow-template-1000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateVersionedTemplateNameWithTimestamp(tt.lwName, tt.timestamp)
			if result != tt.expected {
				t.Errorf("GenerateVersionedTemplateNameWithTimestamp(%q, %d) = %q, want %q",
					tt.lwName, tt.timestamp, result, tt.expected)
			}
		})
	}
}

func TestGetTemplateTimestamp(t *testing.T) {
	tests := []struct {
		name         string
		templateName string
		expectedTs   int64
	}{
		{
			name:         "versioned template",
			templateName: "my-workflow-template-1736150400",
			expectedTs:   1736150400,
		},
		{
			name:         "versioned template with long timestamp",
			templateName: "my-workflow-template-17361504000",
			expectedTs:   17361504000,
		},
		{
			name:         "legacy template",
			templateName: "my-workflow-template",
			expectedTs:   0,
		},
		{
			name:         "complex name versioned",
			templateName: "my-complex-workflow-name-template-1736150400",
			expectedTs:   1736150400,
		},
		{
			name:         "empty string",
			templateName: "",
			expectedTs:   0,
		},
		{
			name:         "invalid format - no suffix",
			templateName: "my-workflow",
			expectedTs:   0,
		},
		{
			name:         "invalid format - wrong suffix",
			templateName: "my-workflow-tmpl",
			expectedTs:   0,
		},
		{
			name:         "template with short number (not timestamp)",
			templateName: "my-workflow-template-123",
			expectedTs:   0, // Short numbers are not valid timestamps
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := GetTemplateTimestamp(tt.templateName)
			if ts != tt.expectedTs {
				t.Errorf("GetTemplateTimestamp(%q) = %d, want %d", tt.templateName, ts, tt.expectedTs)
			}
		})
	}
}

func TestIsVersionedTemplateName(t *testing.T) {
	tests := []struct {
		name         string
		templateName string
		expected     bool
	}{
		{
			name:         "versioned template",
			templateName: "my-workflow-template-1736150400",
			expected:     true,
		},
		{
			name:         "versioned template with long timestamp",
			templateName: "my-workflow-template-17361504000",
			expected:     true,
		},
		{
			name:         "legacy template",
			templateName: "my-workflow-template",
			expected:     false,
		},
		{
			name:         "empty string",
			templateName: "",
			expected:     false,
		},
		{
			name:         "invalid format",
			templateName: "my-workflow",
			expected:     false,
		},
		{
			name:         "template with short number",
			templateName: "my-workflow-template-123",
			expected:     false, // 123 is only 3 digits, not 10+
		},
		{
			name:         "complex name versioned",
			templateName: "my-complex-workflow-name-template-1736150400",
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsVersionedTemplateName(tt.templateName)
			if result != tt.expected {
				t.Errorf("IsVersionedTemplateName(%q) = %v, want %v", tt.templateName, result, tt.expected)
			}
		})
	}
}

func TestExtractLakeFlowName(t *testing.T) {
	tests := []struct {
		name         string
		templateName string
		expectedLW   string
	}{
		{
			name:         "versioned template",
			templateName: "my-workflow-template-1736150400",
			expectedLW:   "my-workflow",
		},
		{
			name:         "legacy template",
			templateName: "my-workflow-template",
			expectedLW:   "my-workflow",
		},
		{
			name:         "complex name versioned",
			templateName: "my-complex-workflow-name-template-1736150400",
			expectedLW:   "my-complex-workflow-name",
		},
		{
			name:         "complex name legacy",
			templateName: "my-complex-workflow-name-template",
			expectedLW:   "my-complex-workflow-name",
		},
		{
			name:         "empty string",
			templateName: "",
			expectedLW:   "",
		},
		{
			name:         "invalid format",
			templateName: "my-workflow",
			expectedLW:   "",
		},
		{
			name:         "just suffix",
			templateName: "-template",
			expectedLW:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lwName := ExtractLakeFlowName(tt.templateName)
			if lwName != tt.expectedLW {
				t.Errorf("ExtractLakeFlowName(%q) = %q, want %q", tt.templateName, lwName, tt.expectedLW)
			}
		})
	}
}

func TestCompareTemplateVersions(t *testing.T) {
	tests := []struct {
		name     string
		t1       string
		t2       string
		expected int // -1, 0, or 1
	}{
		{
			name:     "t1 older than t2",
			t1:       "my-workflow-template-1736150000",
			t2:       "my-workflow-template-1736150400",
			expected: -1,
		},
		{
			name:     "t1 newer than t2",
			t1:       "my-workflow-template-1736150400",
			t2:       "my-workflow-template-1736150000",
			expected: 1,
		},
		{
			name:     "same timestamp",
			t1:       "my-workflow-template-1736150400",
			t2:       "my-workflow-template-1736150400",
			expected: 0,
		},
		{
			name:     "both legacy",
			t1:       "my-workflow-template",
			t2:       "my-workflow-template",
			expected: 0,
		},
		{
			name:     "legacy vs versioned (legacy is older)",
			t1:       "my-workflow-template",
			t2:       "my-workflow-template-1736150400",
			expected: -1,
		},
		{
			name:     "versioned vs legacy (versioned is newer)",
			t1:       "my-workflow-template-1736150400",
			t2:       "my-workflow-template",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareTemplateVersions(tt.t1, tt.t2)
			if result != tt.expected {
				t.Errorf("CompareTemplateVersions(%q, %q) = %d, want %d", tt.t1, tt.t2, result, tt.expected)
			}
		})
	}
}
