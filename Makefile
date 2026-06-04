# Image URL to use all building/pushing image targets.
# No default is provided on purpose: IMG must be set explicitly, e.g.
#   make docker-build IMG=docker-registry.lakeflow.io/data-prod/lakeflow-controller:latest
IMG ?=

# Development image URL for k3d registry (accessible from both host and cluster)
DEV_IMG ?= dev-registry.localhost:5001/lakeflow-controller:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd:allowDangerousTypes=true webhook paths="./api/..." paths="./internal/..." paths="./pkg/..." paths="./cmd/..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..." paths="./internal/..." paths="./pkg/..." paths="./cmd/..."

.PHONY: generate-client
generate-client: ## Generate typed client, listers, and informers for v1alpha1.LakeFlow.
	bash hack/update-codegen.sh

.PHONY: verify-client
verify-client: ## Verify that the generated client code is up-to-date.
	bash hack/verify-codegen.sh

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt $$(go list ./... | grep -v /deploy)

.PHONY: vet
vet: ## Run go vet against code.
	go vet $$(go list ./... | grep -v /deploy)

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e | grep -v /deploy) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= lakeflow-controller-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	$(KIND) create cluster --name $(KIND_CLUSTER)

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND_CLUSTER=$(KIND_CLUSTER) go test ./test/e2e/ -v -ginkgo.v
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# require-img fails fast when IMG is not set, so image build/push targets never
# fall back to an implicit default registry/tag.
.PHONY: require-img
require-img:
	@if [ -z "$(strip $(IMG))" ]; then \
		echo "Error: IMG is required. Usage: make <target> IMG=<registry>/<image>:<tag>"; \
		exit 1; \
	fi

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: require-img ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: require-img ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64
.PHONY: docker-buildx
docker-buildx: require-img ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	# sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name lakeflow-controller-builder
	$(CONTAINER_TOOL) buildx use lakeflow-controller-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile .
	- $(CONTAINER_TOOL) buildx rm lakeflow-controller-builder

.PHONY:docker-buildx-dev
docker-build-dev: ## Build and push docker image for development to k3d registry (Apple Silicon optimized)
	@echo "🔨 Building image for k3d registry..."
	$(CONTAINER_TOOL) build --platform linux/arm64 -t localhost:5001/lakeflow-controller:latest .
	@echo "📤 Pushing image to k3d registry: localhost:5001"
	$(CONTAINER_TOOL) push localhost:5001/lakeflow-controller:latest
	@echo "🏷️  Tagging image for cluster access: $(DEV_IMG)"
	$(CONTAINER_TOOL) tag localhost:5001/lakeflow-controller:latest $(DEV_IMG)
	@echo "✅ Successfully built and pushed to k3d registry"
	@echo "📋 Push URL: localhost:5001/lakeflow-controller:latest"
	@echo "📋 Cluster URL: $(DEV_IMG)"

.PHONY: docker-buildx-dev
docker-buildx-dev: docker-build-dev ## Alias for docker-build-dev (for backward compatibility)

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml

.PHONY: build-deploy
build-deploy: ## Build the deploy CLI tool for automated deployments.
	@echo "🔨 Building deploy tool..."
	cd deploy && go build -o ../bin/deploy .
	@echo "✅ Deploy tool built successfully: bin/deploy"
	@echo "📋 Usage: ./bin/deploy --help"

.PHONY: tidy-deploy
tidy-deploy: ## Run go mod tidy for the deploy tool.
	cd deploy && go mod tidy

##@ Testing

.PHONY: copy-rbac
copy-rbac: ## Copy RBAC resources to a target namespace. Usage: make copy-rbac NAMESPACE=my-namespace
	@if [ -z "$(NAMESPACE)" ]; then \
		echo "Error: NAMESPACE parameter is required. Usage: make copy-rbac NAMESPACE=my-namespace"; \
		exit 1; \
	fi
	@echo "📋 Creating RBAC resources in namespace: $(NAMESPACE)"
	@$(KUBECTL) create namespace $(NAMESPACE) --dry-run=client -o yaml | $(KUBECTL) apply -f -
	@echo "✓ Namespace $(NAMESPACE) created/verified"
	@# Create ServiceAccount
	@echo "apiVersion: v1" > /tmp/rbac-$(NAMESPACE).yaml
	@echo "kind: ServiceAccount" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "metadata:" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  name: lakeflow-controller" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  namespace: $(NAMESPACE)" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  labels:" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "    app.kubernetes.io/name: lakeflow-controller" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "    app.kubernetes.io/managed-by: kustomize" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "---" >> /tmp/rbac-$(NAMESPACE).yaml
	@# Create Role (namespace-scoped version of ClusterRole)
	@awk '/^kind: ClusterRole$$/ {print "kind: Role"; next} \
		/^  name: manager-role$$/ {print "  name: lakeflow-controller-role"; next} \
		/^  namespace:/ {next} \
		/^metadata:$$/ {print; print "  namespace: $(NAMESPACE)"; next} \
		{print}' config/rbac/role.yaml >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "---" >> /tmp/rbac-$(NAMESPACE).yaml
	@# Create RoleBinding (namespace-scoped version of ClusterRoleBinding)
	@echo "apiVersion: rbac.authorization.k8s.io/v1" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "kind: RoleBinding" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "metadata:" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  name: lakeflow-controller-rolebinding" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  namespace: $(NAMESPACE)" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  labels:" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "    app.kubernetes.io/name: lakeflow-controller" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "    app.kubernetes.io/managed-by: kustomize" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "roleRef:" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  apiGroup: rbac.authorization.k8s.io" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  kind: Role" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  name: lakeflow-controller-role" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "subjects:" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "- kind: ServiceAccount" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  name: lakeflow-controller" >> /tmp/rbac-$(NAMESPACE).yaml
	@echo "  namespace: $(NAMESPACE)" >> /tmp/rbac-$(NAMESPACE).yaml
	@$(KUBECTL) apply -f /tmp/rbac-$(NAMESPACE).yaml
	@rm -f /tmp/rbac-$(NAMESPACE).yaml
	@echo "✅ RBAC resources created successfully in namespace: $(NAMESPACE)"
	@echo "📋 Created resources:"
	@echo "  - ServiceAccount: lakeflow-controller"
	@echo "  - Role: lakeflow-controller-role"
	@echo "  - RoleBinding: lakeflow-controller-rolebinding"
	@echo ""
	@echo "🔧 To use this service account in your workflows, add this to your LakeFlow:"
	@echo "  spec:"
	@echo "    serviceAccountName: lakeflow-controller"

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: deploy-dev ## Deploy controller to the k3d cluster (alias for deploy-dev).

.PHONY: undeploy
undeploy: undeploy-dev ## Undeploy controller from the k3d cluster (alias for undeploy-dev).

.PHONY: undeploy-dev
undeploy-dev: kustomize ## Undeploy controller from k3d cluster using dev overlay.
	@echo "🗑️  Undeploying from k3d cluster using dev overlay"
	$(KUSTOMIZE) build config/overlays/dev | $(KUBECTL) delete --ignore-not-found=true -f -
	@echo "✅ Successfully undeployed from k3d cluster"

.PHONY: rbac
rbac: kustomize ## Apply RBAC configuration to the K8s cluster specified in ~/.kube/config.
	$(KUBECTL) create namespace lakeflow-controller-system --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUSTOMIZE) build config/rbac | $(KUBECTL) apply -f -

.PHONY: deploy-dev
deploy-dev: manifests kustomize ## Deploy controller to k3d cluster using dev overlay.
	@echo "🚀 Deploying to k3d cluster using dev overlay"
	$(KUSTOMIZE) build config/overlays/dev | $(KUBECTL) apply -f -
	@echo "✅ Successfully deployed to k3d cluster"
	@echo "📋 Namespace: lakeflow-controller-dev"
	@echo "📋 Image: $(DEV_IMG)"

.PHONY: build-and-deploy-dev
build-and-deploy-dev: docker-buildx-dev deploy-dev ## Build and deploy to k3d cluster in one command.
	@echo "🎉 Development build and deploy completed!"

.PHONY: redeploy
redeploy: redeploy-dev ## Redeploy controller to the k3d cluster (alias for redeploy-dev).

.PHONY: redeploy-dev
redeploy-dev: manifests kustomize ## Redeploy controller to k3d cluster (wipe namespace resources + fresh deploy).
	@echo "🔄 Redeploying to k3d cluster (keeping RBAC/CRDs)..."
	@DEV_NS=$$(grep "^namespace:" config/overlays/dev/kustomization.yaml | awk '{print $$2}') && \
	echo "🗑️  Deleting operator deployment resources in $$DEV_NS namespace" && \
	$(KUBECTL) delete deployment -l app.kubernetes.io/name=lakeflow-controller -n "$$DEV_NS" --ignore-not-found=true && \
	$(KUBECTL) delete service -l app.kubernetes.io/name=lakeflow-controller -n "$$DEV_NS" --ignore-not-found=true
	@echo "🚀 Deploying fresh image to k3d cluster"
	$(KUSTOMIZE) build config/overlays/dev | $(KUBECTL) apply -f -
	@DEV_NS=$$(grep "^namespace:" config/overlays/dev/kustomization.yaml | awk '{print $$2}') && \
	echo "✅ Development redeploy completed!" && \
	echo "📋 Namespace: $$DEV_NS" && \
	echo "📋 Image: $(DEV_IMG)"

# Backward compatibility aliases
.PHONY: dev-docker-build
dev-docker-build: docker-build-dev ## Backward compatibility alias for docker-build-dev

.PHONY: dev-docker-buildx
dev-docker-buildx: docker-buildx-dev ## Backward compatibility alias for docker-buildx-dev

.PHONY: dev-build-and-deploy
dev-build-and-deploy: build-and-deploy-dev ## Backward compatibility alias for build-and-deploy-dev


##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.6.0
CONTROLLER_TOOLS_VERSION ?= v0.18.0
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.1.0

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef
