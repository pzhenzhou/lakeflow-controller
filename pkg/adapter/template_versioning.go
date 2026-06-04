package adapter

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Template naming patterns:
// - Legacy format: "{name}-template" (e.g., "my-workflow-template")
// - Versioned format: "{name}-template-{timestamp}" (e.g., "my-workflow-template-1736150400")

const (
	// templateSuffix is the standard suffix for workflow templates
	templateSuffix = "-template"
	// templateSuffixWithDash is used to split versioned template names
	templateSuffixWithDash = "-template-"
	// minTimestampDigits is the minimum digits for a valid Unix timestamp (10 digits = year 2001+)
	minTimestampDigits = 10
)

// GenerateVersionedTemplateName generates a versioned template name using Unix timestamp.
// This is the ONLY method used for creating new templates.
// Example: "my-workflow" -> "my-workflow-template-1736150400"
func GenerateVersionedTemplateName(lwName string) string {
	return fmt.Sprintf("%s%s-%d", lwName, templateSuffix, time.Now().Unix())
}

// GenerateVersionedTemplateNameWithTimestamp generates a versioned template name using a specific timestamp.
// This is useful for testing or when you need deterministic naming.
// Example: GenerateVersionedTemplateNameWithTimestamp("my-workflow", 1736150400) -> "my-workflow-template-1736150400"
func GenerateVersionedTemplateNameWithTimestamp(lwName string, timestamp int64) string {
	return fmt.Sprintf("%s%s-%d", lwName, templateSuffix, timestamp)
}

// GetTemplateTimestamp extracts the Unix timestamp from a versioned template name.
// Returns 0 if the template is in legacy format or invalid.
//
// Examples:
//   - "my-workflow-template-1736150400" -> 1736150400
//   - "my-workflow-template" -> 0 (legacy format)
//   - "" -> 0
func GetTemplateTimestamp(templateName string) int64 {
	if templateName == "" {
		return 0
	}

	// Split by "-template-" and get the last part
	idx := strings.LastIndex(templateName, templateSuffixWithDash)
	if idx == -1 {
		return 0 // No "-template-" found, legacy format or invalid
	}

	// Get the part after "-template-"
	timestampStr := templateName[idx+len(templateSuffixWithDash):]

	// Must be at least 10 digits (Unix timestamp)
	if len(timestampStr) < minTimestampDigits {
		return 0
	}

	// Try to parse as int64
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0
	}

	return timestamp
}

// IsVersionedTemplateName checks if a template name follows the versioned naming pattern.
// Returns true for versioned templates (with timestamp), false for legacy templates.
//
// Examples:
//   - "my-workflow-template-1736150400" -> true
//   - "my-workflow-template" -> false (legacy format)
//   - "" -> false
func IsVersionedTemplateName(templateName string) bool {
	return GetTemplateTimestamp(templateName) > 0
}

// ExtractLakeFlowName extracts the LakeFlow name from a template name.
// Works with both legacy and versioned template names.
// Returns empty string if invalid.
//
// Examples:
//   - "my-workflow-template-1736150400" -> "my-workflow"
//   - "my-workflow-template" -> "my-workflow"
//   - "invalid" -> ""
func ExtractLakeFlowName(templateName string) string {
	if templateName == "" {
		return ""
	}

	// Try versioned format first: split by "-template-"
	idx := strings.LastIndex(templateName, templateSuffixWithDash)
	if idx > 0 {
		// Verify the suffix is actually a timestamp
		if GetTemplateTimestamp(templateName) > 0 {
			return templateName[:idx]
		}
	}

	// Try legacy format: ends with "-template"
	if strings.HasSuffix(templateName, templateSuffix) {
		lwName := strings.TrimSuffix(templateName, templateSuffix)
		return lwName
	}

	return ""
}

// CompareTemplateVersions compares two versioned template names and returns:
//   - negative if t1 is older than t2
//   - zero if t1 and t2 have the same timestamp (or both are legacy)
//   - positive if t1 is newer than t2
//
// Legacy templates (without timestamp) are treated as having timestamp 0 (oldest).
func CompareTemplateVersions(t1, t2 string) int {
	ts1 := GetTemplateTimestamp(t1)
	ts2 := GetTemplateTimestamp(t2)

	if ts1 < ts2 {
		return -1
	}
	if ts1 > ts2 {
		return 1
	}
	return 0
}
