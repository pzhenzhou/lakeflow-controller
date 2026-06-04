#!/usr/bin/env bash

# Copyright 2025.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Kubebuilder Code Generation - Complete Cleanup
# ===============================================
# This script completely removes all incorrectly generated traditional client code
# and uses Kubebuilder's modern approach for code generation.
#

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

echo "🎯 Kubebuilder 4.6.0 Modern Code Generation - Complete Cleanup"
echo "=============================================================="

# Clean up ALL incorrectly generated traditional client code
echo "🧹 Performing comprehensive cleanup of incorrectly generated files..."

# Remove incorrectly generated traditional client directories
if [ -d "${ROOT}/pkg/client" ]; then
    echo "  • Removing pkg/client/"
    rm -rf "${ROOT}/pkg/client"
fi

if [ -d "${ROOT}/versioned" ]; then
    echo "  • Removing versioned/"
    rm -rf "${ROOT}/versioned"
fi

# Clean up incorrectly placed files in API package
echo "  • Cleaning API package of generated files..."
rm -f "${ROOT}/api/v1alpha1/dependtrigger.go"
rm -f "${ROOT}/api/v1alpha1/task.go"

# Clean up the incorrectly generated utils.go at root level
if [ -f "${ROOT}/utils.go" ]; then
    echo "  • Removing incorrectly generated utils.go"
    rm -f "${ROOT}/utils.go"
fi

# Clean up any other generated files that might be causing issues
find "${ROOT}" -name "*_generated.go" -not -path "*/api/v1alpha1/zz_generated.deepcopy.go" -delete 2>/dev/null || true
find "${ROOT}" -name "clientset_generated.go" -delete 2>/dev/null || true

echo ""
echo "✅ Use these Kubebuilder commands instead:"
echo ""
echo "  make manifests    # Generate CRDs and RBAC"
echo "  make generate     # Generate deepcopy methods"
echo "  make fmt         # Format code"
echo "  make vet         # Vet code"
echo "  make build       # Build the operator"
echo ""

echo "🚀 For client usage in your code:"
echo ""
echo "Instead of traditional clientsets, use controller-runtime's client:"
echo ""
echo "  import \"sigs.k8s.io/controller-runtime/pkg/client\""
echo ""
echo "  // In your reconciler:"
echo "  func (r *LakeFlowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {"
echo "      var lw v1alpha1.LakeFlow"
echo "      if err := r.Get(ctx, req.NamespacedName, &lw); err != nil {"
echo "          return ctrl.Result{}, client.IgnoreNotFound(err)"
echo "      }"
echo "      // Work with lw directly - no generated clientset needed!"
echo "  }"
echo ""

echo "���� For testing, use controller-runtime's fake client:"
echo ""
echo "  import \"sigs.k8s.io/controller-runtime/pkg/client/fake\""
echo "  import \"k8s.io/apimachinery/pkg/runtime\""
echo ""
echo "  // Create fake client for testing"
echo "  scheme := runtime.NewScheme()"
echo "  v1alpha1.AddToScheme(scheme)"
echo "  fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()"
echo ""

echo "❌ What you DON'T need:"
echo "  • Traditional clientsets (pkg/client/versioned)"
echo "  • Listers and informers"
echo "  • k8s.io/code-generator"
echo "  • Manual client generation scripts"
echo ""
echo "✅ Everything you need is built into controller-runtime!"

# Clean up go.mod in case there are any references to the removed packages
echo ""
echo "🔄 Cleaning up go.mod..."
go mod tidy

# Run the standard Kubebuilder generation
echo ""
echo "🔄 Running standard Kubebuilder code generation..."
make generate
make manifests

echo ""
echo "🎉 Code generation completed successfully!"
echo "Your project now uses the modern Kubebuilder approach with all legacy code removed."
