package validation

import (
	"context"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
)

// Manager orchestrates all validation logic for a LakeFlow.
type Manager interface {
	// Validate performs all checks and returns a single error if any validation fails.
	Validate(ctx context.Context, lw *v1alpha1.LakeFlow) error
}

// validationManager implements the Manager interface.
type validationManager struct {
}

// NewManager creates a new validation manager.
func NewManager() Manager {
	return &validationManager{}
}

// Validate runs all stateless validation checks against the LakeFlow. It is the
// in-reconcile safety net and shares its rule set with the admission webhook via
// ValidateCreate, so a LakeFlow that fails admission also fails reconcile.
func (v *validationManager) Validate(_ context.Context, lw *v1alpha1.LakeFlow) error {
	return ValidateCreate(lw).ToAggregate()
}
