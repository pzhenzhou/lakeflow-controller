package reconciler

import (
	"context"
	"fmt"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	logger                = common.GetSharedLogger().WithName("workflow-reconciler")
	systemManagedPrefixes = []string{
		"controller-uid",
		"job-name",
		"workflows.argoproj.io/",
	}
)

// ReconcileAction represents the action taken during reconciliation
type ReconcileAction string

// ResourceAction represents the action taken on a specific resource
type ResourceAction string

const (
	ResourceActionCreated ResourceAction = "Created"
	ResourceActionUpdated ResourceAction = "Updated"
)

const (
	// ReconcileActionCreated indicates resources were created
	ReconcileActionCreated ReconcileAction = "Created"
	// ReconcileActionUpdated indicates resources were updated
	ReconcileActionUpdated ReconcileAction = "Updated"
	// ReconcileActionNoChange indicates no changes were needed
	ReconcileActionNoChange ReconcileAction = "NoChange"
	// ReconcileActionError indicates an error occurred during reconciliation
	ReconcileActionError ReconcileAction = "Error"
)

// ArgoResourceProcess is a function type for processing Argo resources
type ArgoResourceProcess func(ctx context.Context, lw *v1alpha1.LakeFlow, resource client.Object) (*ReconcileResult, error)

// ReconcileResult contains the result of a reconciliation operation
type ReconcileResult struct {
	Action        ReconcileAction
	Message       string
	ResourceNames map[string]string // resource type -> resource name
	Error         error
}

// errorResult builds a ReconcileResult for a failed reconciliation. The message
// is formatted from format/args (callers keep their existing wording, including
// any "%v" of err), and err is stored verbatim in the Error field.
func errorResult(err error, format string, args ...any) *ReconcileResult {
	return &ReconcileResult{
		Action:  ReconcileActionError,
		Message: fmt.Sprintf(format, args...),
		Error:   err,
	}
}

// NewErrorResult builds a ReconcileResult for a failed reconciliation from err
// alone, using err's message. It lets callers outside this package report an
// error outcome without depending on the ReconcileResult struct layout.
func NewErrorResult(err error) *ReconcileResult {
	return errorResult(err, "%v", err)
}
