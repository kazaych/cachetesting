SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

LOCALBIN ?= $(shell pwd)/bin
ENVTEST ?= $(LOCALBIN)/setup-envtest
ENVTEST_VERSION ?= release-0.23
ENVTEST_K8S_VERSION ?= 1.35
REPORT ?= cache_comparison.html
PDF ?= cache_comparison_report.pdf

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: test
test: setup-envtest ## Run the cache comparison and generate the HTML report.
	@echo "Running cache comparison..."
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
	  go test . -run TestCacheComparison -v -count=1 -timeout 600s
	@echo "Report: $(REPORT)"

.PHONY: open
open: ## Open the generated HTML report.
	@open "$(REPORT)" 2>/dev/null || xdg-open "$(REPORT)" 2>/dev/null || echo "Open $(REPORT) in your browser."

.PHONY: pdf
pdf: ## Generate PDF from the HTML report (requires weasyprint).
	@command -v weasyprint >/dev/null || { echo "weasyprint is required: pip install weasyprint"; exit 1; }
	weasyprint "$(REPORT)" "$(PDF)"
	@echo "PDF: $(PDF)"

.PHONY: setup-envtest
setup-envtest: $(ENVTEST) ## Download kube-apiserver and etcd binaries for envtest.
	@echo "Setting up envtest binaries for Kubernetes $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path >/dev/null

$(ENVTEST): | $(LOCALBIN)
	GOBIN="$(LOCALBIN)" go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

.PHONY: clean
clean: ## Remove generated reports and local envtest binaries.
	rm -rf "$(LOCALBIN)" "$(REPORT)" "$(PDF)"
