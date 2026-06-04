#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# install-volcano.sh – Idempotent installer/upgrader for Volcano via Helm.
# Detects existing RBAC resources and skips creation to avoid "already exists" errors.
# -----------------------------------------------------------------------------

set -euo pipefail

# Initialize Helm --set arguments array for conditional RBAC flags
HELM_SET_ARGS=()

# Default variables
VOLCANO_NAMESPACE="volcano-system"
RELEASE_NAME="volcano"
REPO_NAME="volcano-sh"
REPO_URL="https://volcano-sh.github.io/helm-charts"

# Colored output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'
print_status() { echo -e "${GREEN}[INFO]${NC} $*"; }
print_warning() { echo -e "${YELLOW}[WARN]${NC} $*"; }
print_error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# 1) Ensure namespace exists
if ! kubectl get ns "$VOLCANO_NAMESPACE" &>/dev/null; then
  print_status "Creating namespace '$VOLCANO_NAMESPACE'"
  kubectl create namespace "$VOLCANO_NAMESPACE"
else
  print_status "Namespace '$VOLCANO_NAMESPACE' already exists"
fi

# 2) Add or update Helm repo
if ! helm repo list | grep -q "^$REPO_NAME"; then
  print_status "Adding Helm repo '$REPO_NAME'"
  helm repo add "$REPO_NAME" "$REPO_URL"
fi
helm repo update "$REPO_NAME"

# 3) Detect existing cluster-scoped RBAC
CLUSTER_RBAC_EXISTS=false
if kubectl get clusterrole volcano-admission &>/dev/null; then
  print_warning "ClusterRole 'volcano-admission' exists"
  CLUSTER_RBAC_EXISTS=true
fi
if kubectl get clusterrolebinding volcano-admission-role &>/dev/null; then
  print_warning "ClusterRoleBinding 'volcano-admission-role' exists"
  CLUSTER_RBAC_EXISTS=true
fi

# 4) Detect existing namespaced RBAC
NAMESPACED_RBAC_EXISTS=false
if kubectl get sa volcano-admission -n "$VOLCANO_NAMESPACE" &>/dev/null; then
  print_warning "ServiceAccount 'volcano-admission' exists"
  NAMESPACED_RBAC_EXISTS=true
fi
if kubectl get role volcano-admission-init -n "$VOLCANO_NAMESPACE" &>/dev/null; then
  print_warning "Role 'volcano-admission-init' exists"
  NAMESPACED_RBAC_EXISTS=true
fi
if kubectl get rolebinding volcano-admission-init-role -n "$VOLCANO_NAMESPACE" &>/dev/null; then
  print_warning "RoleBinding 'volcano-admission-init-role' exists"
  NAMESPACED_RBAC_EXISTS=true
fi

# 5) Conditionally skip RBAC creation via Helm flags
if [[ "$CLUSTER_RBAC_EXISTS" == "true" ]]; then
  print_status "Skipping cluster-scoped RBAC creation"
  HELM_SET_ARGS+=(--set admission.enabled=false)
  HELM_SET_ARGS+=(--set serviceAccount.create=false)
fi
if [[ "$NAMESPACED_RBAC_EXISTS" == "true" ]]; then
  print_status "Skipping namespaced RBAC creation"
  HELM_SET_ARGS+=(--set admission.init.enabled=false)
fi

# 6) Install or upgrade Volcano
print_status "Installing/upgrading Volcano via Helm"
if [[ ${#HELM_SET_ARGS[@]} -gt 0 ]]; then
  helm upgrade --install "$RELEASE_NAME" "$REPO_NAME"/volcano \
    --namespace "$VOLCANO_NAMESPACE" \
    --create-namespace \
    "${HELM_SET_ARGS[@]}"
else
  helm upgrade --install "$RELEASE_NAME" "$REPO_NAME"/volcano \
    --namespace "$VOLCANO_NAMESPACE" \
    --create-namespace
fi

print_status "✅ Volcano install/upgrade complete" "✅ Volcano install/upgrade complete"
