#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# install-argo-workflow.sh – Idempotent installer / upgrader for Argo Workflows
# -----------------------------------------------------------------------------
# • Installs / upgrades Argo Workflows CRDs and the workflow‑controller only
#   (the argo‑server UI is disabled).
# • Automatically installs / updates the Argo CLI binary under /usr/local/bin
#   if it is not already present.
# • Designed to be safe to run repeatedly; it detects existing resources and
#   Helm release states and takes the appropriate action.
# -----------------------------------------------------------------------------
set -euo pipefail

# ----------------------------- Configuration ---------------------------------
NAMESPACE="${NAMESPACE:-argo}"
RELEASE_NAME="argo-workflows"
REPO_NAME="argo"
REPO_URL="https://argoproj.github.io/argo-helm"
CHART_VERSION="0.45.21"    # Latest helm chart for Argo Workflows (bundles Argo Workflows v3.7.0)    # Helm chart that bundles Argo Workflows v3.6.10
CLI_VERSION="v3.7.0"       # Argo CLI version matching controller       # Argo CLI version to install
WAIT_TIMEOUT="5m"           # helm --wait timeout
INSTALL_DIR="/usr/local/bin" # Destination for the Argo CLI binary

# --------------------------- Helper functions --------------------------------
log()   { printf "\033[32m[INFO ]\033[0m %s\n" "$*"; }
warn()  { printf "\033[33m[WARN ]\033[0m %s\n" "$*"; }
error() { printf "\033[31m[ERROR]\033[0m %s\n" "$*"; exit 1; }
need()  { command -v "$1" &>/dev/null || error "$1 is required but not found"; }

# --------------------------- Pre‑flight checks -------------------------------
log "Checking prerequisites…"
need kubectl
need helm
kubectl cluster-info &>/dev/null || error "Unable to connect to the Kubernetes cluster"
log "All prerequisites satisfied"

# --------------------- Install / upgrade Argo CLI ----------------------------
install_cli() {
  local cli_path="${INSTALL_DIR}/argo"
  if [[ -x "$cli_path" ]]; then
    log "Argo CLI already present at $cli_path — skipping download"
    return
  fi

  log "Downloading Argo CLI ${CLI_VERSION}…"
  local os="linux"
  [[ "$(uname -s)" == "Darwin" ]] && os="darwin"

  curl -sSL -o "argo-${os}-amd64.gz" \
    "https://github.com/argoproj/argo-workflows/releases/download/${CLI_VERSION}/argo-${os}-amd64.gz" \
    || error "Failed to download Argo CLI — check network or CLI_VERSION"

  gunzip "argo-${os}-amd64.gz"
  chmod +x "argo-${os}-amd64"
  sudo mv "argo-${os}-amd64" "$cli_path"
  log "Argo CLI installed to $cli_path"
}

install_cli

# --------------------------- Helm repository ---------------------------------
if helm repo list | grep -q "^${REPO_NAME}[[:space:]]"; then
  log "Helm repo '${REPO_NAME}' already exists — updating…"
  helm repo update "${REPO_NAME}"
else
  log "Adding Helm repo '${REPO_NAME}'"
  helm repo add "${REPO_NAME}" "${REPO_URL}"
  helm repo update "${REPO_NAME}"
fi

# --------------------------- Namespace ---------------------------------------
if ! kubectl get ns "${NAMESPACE}" &>/dev/null; then
  log "Creating namespace '${NAMESPACE}'"
  kubectl create namespace "${NAMESPACE}"
else
  log "Namespace '${NAMESPACE}' already exists"
fi

# --------------------------- Helm install / upgrade --------------------------
HELM_VALUES=(
  --namespace "${NAMESPACE}"
  --create-namespace
  --wait --timeout "${WAIT_TIMEOUT}"
  --set server.enabled=false
  --set controller.workflowNamespaces={"${NAMESPACE}"}
  --set workflow.rbac.create=true
  --set workflow.serviceAccount.create=true
  --set controller.rbac.writeConfigMaps=true
)

release_exists() { helm status "${RELEASE_NAME}" -n "${NAMESPACE}" &>/dev/null; }

if release_exists; then
  STATUS=$(helm status "${RELEASE_NAME}" -n "${NAMESPACE}" -o json | jq -r '.info.status')
  case "${STATUS}" in
    deployed)
      log "Helm release '${RELEASE_NAME}' is deployed — performing upgrade to chart ${CHART_VERSION}"
      helm upgrade "${RELEASE_NAME}" "${REPO_NAME}/${RELEASE_NAME}" --version "${CHART_VERSION}" "${HELM_VALUES[@]}" ;;
    pending-install|failed)
      warn "Release is in '${STATUS}' — uninstalling then reinstalling"
      helm uninstall "${RELEASE_NAME}" -n "${NAMESPACE}" || true
      helm install "${RELEASE_NAME}" "${REPO_NAME}/${RELEASE_NAME}" --version "${CHART_VERSION}" "${HELM_VALUES[@]}" ;;
    pending-upgrade)
      warn "Release is pending-upgrade — rolling back then upgrading"
      helm rollback "${RELEASE_NAME}" -n "${NAMESPACE}"
      helm upgrade  "${RELEASE_NAME}" "${REPO_NAME}/${RELEASE_NAME}" --version "${CHART_VERSION}" "${HELM_VALUES[@]}" ;;
    *)
      warn "Release is in unexpected state (${STATUS}) — attempting upgrade"
      helm upgrade  "${RELEASE_NAME}" "${REPO_NAME}/${RELEASE_NAME}" --version "${CHART_VERSION}" "${HELM_VALUES[@]}" ;;
  esac
else
  log "Installing Helm release '${RELEASE_NAME}' (chart ${CHART_VERSION})"
  helm install "${RELEASE_NAME}" "${REPO_NAME}/${RELEASE_NAME}" --version "${CHART_VERSION}" "${HELM_VALUES[@]}"
fi

# --------------------------- Verification ------------------------------------
log "Verifying workflow-controller pod(s)…"
kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/component=workflow-controller

log "Verifying essential CRDs…"
REQUIRED_CRDS=(
  workflows.argoproj.io
  workflowtemplates.argoproj.io
  cronworkflows.argoproj.io
  clusterworkflowtemplates.argoproj.io
)
for crd in "${REQUIRED_CRDS[@]}"; do
  if kubectl get crd "$crd" &>/dev/null; then
    log "CRD '$crd' is present"
  else
    error "CRD '$crd' is missing — installation incomplete"
  fi
done

log "Verifying workflow-controller ConfigMap permissions…"
CONTROLLER_ROLE="argo-workflows-workflow-controller"
if kubectl get clusterrole "${CONTROLLER_ROLE}" &>/dev/null; then
  CONFIGMAP_VERBS=$(kubectl get clusterrole "${CONTROLLER_ROLE}" -o jsonpath='{.rules[?(@.resources[0]=="configmaps")].verbs}' 2>/dev/null || echo "[]")
  if echo "${CONFIGMAP_VERBS}" | grep -q "create"; then
    log "✓ ConfigMap create permission verified"
  else
    warn "ConfigMap create permission not found in ClusterRole — may need manual verification"
  fi
else
  warn "ClusterRole '${CONTROLLER_ROLE}' not found"
fi

log "✅ Argo Workflows controller and CLI installation / upgrade complete"
