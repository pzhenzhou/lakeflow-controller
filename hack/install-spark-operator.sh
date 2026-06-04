#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# install-spark-operator.sh – Idempotent installer/upgrader for Spark-Operator via Helm.
# It skips creation of existing RBAC (cluster- and namespace-scoped) and ensures
# service accounts and namespace watching are configured.
# -----------------------------------------------------------------------------

set -euo pipefail

# Default namespace where Spark Operator is deployed
SPARK_OPERATOR_NAMESPACE="spark-operator"
# Comma-separated list of namespaces to watch (empty = all namespaces)
WATCH_NAMESPACES="${WATCH_NAMESPACES:-}"

# Helper functions for colored output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'
print_status() { echo -e "${GREEN}[INFO]${NC} $*"; }
print_warning() { echo -e "${YELLOW}[WARN]${NC} $*"; }
print_error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# 1) Ensure namespace exists
if ! kubectl get ns "$SPARK_OPERATOR_NAMESPACE" &>/dev/null; then
  print_status "Creating namespace '$SPARK_OPERATOR_NAMESPACE'"
  kubectl create namespace "$SPARK_OPERATOR_NAMESPACE"
else
  print_status "Namespace '$SPARK_OPERATOR_NAMESPACE' already exists"
fi

# 2) Add/Update Helm repo
if ! helm repo list | grep -q "^spark-operator"; then
  print_status "Adding Spark Operator Helm repository"
  helm repo add spark-operator https://kubeflow.github.io/spark-operator
else
  print_status "Spark Operator Helm repository already exists"
fi
helm repo update spark-operator

# 3) Determine if cluster-scoped RBAC exists
CLUSTER_RBAC_EXISTS=false
if kubectl get clusterrole spark-operator-controller &>/dev/null; then
  print_warning "Found existing ClusterRole 'spark-operator-controller'"
  CLUSTER_RBAC_EXISTS=true
fi
if kubectl get clusterrolebinding spark-operator-controller &>/dev/null; then
  print_warning "Found existing ClusterRoleBinding 'spark-operator-controller'"
  CLUSTER_RBAC_EXISTS=true
fi

# 4) Determine if namespace-scoped RBAC exists in watched namespaces
NAMESPACED_RBAC_EXISTS=false
if [[ -n "$WATCH_NAMESPACES" ]]; then
  IFS=',' read -ra NAMESPACES <<< "$WATCH_NAMESPACES"
else
  # If empty, default to operator namespace only
  NAMESPACES=("$SPARK_OPERATOR_NAMESPACE")
fi
for ns in "${NAMESPACES[@]}"; do
  if kubectl get role spark-operator-controller -n "$ns" &>/dev/null; then
    print_warning "Found existing Role 'spark-operator-controller' in namespace '$ns'"
    NAMESPACED_RBAC_EXISTS=true
  fi
  if kubectl get rolebinding spark-operator-controller -n "$ns" &>/dev/null; then
    print_warning "Found existing RoleBinding 'spark-operator-controller' in namespace '$ns'"
    NAMESPACED_RBAC_EXISTS=true
  fi
done

# 5) Build Helm --set values
HELM_SET_ARGS=()
# Skip cluster-scoped RBAC if exists
if [[ "$CLUSTER_RBAC_EXISTS" == "true" ]]; then
  print_status "Skipping creation of cluster-scoped RBAC resources"
  HELM_SET_ARGS+=(--set controller.rbac.create=false)
  HELM_SET_ARGS+=(--set webhook.rbac.create=false)
fi
# Skip all RBAC if namespace-scoped exists
if [[ "$NAMESPACED_RBAC_EXISTS" == "true" ]]; then
  print_status "Skipping creation of namespace-scoped RBAC resources"
  HELM_SET_ARGS+=(--set rbac.create=false)
  HELM_SET_ARGS+=(--set rbac.createClusterRole=false)
  HELM_SET_ARGS+=(--set rbac.createRole=false)
fi
# Always ensure service accounts
HELM_SET_ARGS+=(--set controller.serviceAccount.create=true)
HELM_SET_ARGS+=(--set webhook.serviceAccount.create=true)

# Configure namespace watching
if [[ -n "$WATCH_NAMESPACES" ]]; then
  HELM_SET_ARGS+=(--set spark.jobNamespaces={$(echo "$WATCH_NAMESPACES" | sed 's/,/,/g')})
else
  HELM_SET_ARGS+=(--set spark.jobNamespaces={""})
fi

# Enable Volcano batch scheduler integration
HELM_SET_ARGS+=(--set webhook.enable=true)
HELM_SET_ARGS+=(--set controller.batchScheduler.enable=true)

# 6) Install or upgrade with Helm
print_status "Installing/upgrading Spark Operator via Helm"
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace "$SPARK_OPERATOR_NAMESPACE" \
  --create-namespace \
  "${HELM_SET_ARGS[@]}"

print_status "✅ Spark Operator installation/upgrade complete"
