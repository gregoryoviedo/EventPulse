SHELL := /bin/bash

GO_MODULES := services/ingestion-gateway services/metrics-aggregator services/mcp-server pkg/telemetry pkg/events
COMPOSE   := docker compose
TOFU      := tofu
K3D       := k3d
K3D_CLUSTER := eventpulse
K3D_PORTS := 18080:80@loadbalancer 18090:80@loadbalancer
REGISTRY  ?= ghcr.io/eventpulse
IMG_TAG   ?= 0.1.0
SERVICES  := ingestion-gateway document-processor metrics-aggregator mcp-server

.PHONY: dev-env dev-env-down lint build-all test clean tidy gen-types \
	k8s-cluster-up k8s-cluster-down k8s-import-images k8s-dev-env k8s-dev-env-down \
	tofu-init tofu-plan tofu-apply tofu-destroy k8s-verify \
	images-build images-push

## dev-env: start Kafka + Postgres + all services via docker-compose
dev-env:
	$(COMPOSE) up -d

## dev-env-down: stop and remove the dev environment
dev-env-down:
	$(COMPOSE) down -v

## lint: run Go linter and Python linter (if available)
##
## golangci-lint is run per module: in workspace mode (go.work) a bare
## `golangci-lint run ./...` from the repo root does not resolve because `./...`
## would span several modules.
lint:
	@for m in $(GO_MODULES); do (cd $$m && golangci-lint run ./...) || exit 1; done
	@command -v flake8 >/dev/null 2>&1 && flake8 services/document-processor || echo "flake8 not installed, skipping python lint"

## build-all: build every Go module and the docker images
##
## The `./...` pattern must run inside each module: from the repo root it would
## match across the go.work modules and the go command rejects it.
build-all:
	@for m in $(GO_MODULES); do (cd $$m && go build ./...) || exit 1; done
	$(COMPOSE) build

## test: run Go tests across all modules
test:
	@for m in $(GO_MODULES); do (cd $$m && go test ./...) || exit 1; done

## tidy: go mod tidy + go work sync across modules
tidy:
	@for m in $(GO_MODULES); do (cd $$m && go mod tidy); done
	go work sync

## gen-types: placeholder for pgvector type generation
gen-types:
	@echo "pgvector type generation placeholder (deferred)"

## clean: tear down dev env and clean Go build artifacts
clean:
	$(COMPOSE) down -v
	go clean ./...

# ---------------------------------------------------------------------------
# Kubernetes (k3d) + OpenTofu
# ---------------------------------------------------------------------------

## k8s-cluster-up: create the local k3d cluster (idempotent)
k8s-cluster-up:
	@$(K3D) cluster create $(K3D_CLUSTER) --agents 1 $(K3D_PORTS) --wait 2>/dev/null \
		|| echo "cluster '$(K3D_CLUSTER)' already exists"

## k8s-cluster-down: delete the local k3d cluster
k8s-cluster-down:
	$(K3D) cluster delete $(K3D_CLUSTER)

## k8s-import-images: tag and import the compose images into the cluster
k8s-import-images:
	@for svc in ingestion-gateway document-processor metrics-aggregator mcp-server; do \
		docker tag "eventpulse-$$svc:latest" "ghcr.io/eventpulse/$$svc:0.1.0" 2>/dev/null || true; \
		$(K3D) image import "ghcr.io/eventpulse/$$svc:0.1.0" -c $(K3D_CLUSTER); \
	done

## tofu-init: init the OpenTofu workspace
tofu-init:
	cd deploy/opentofu && $(TOFU) init

## tofu-plan: show the planned infrastructure changes
tofu-plan:
	cd deploy/opentofu && $(TOFU) plan

## tofu-apply: bootstrap the cluster and deploy the stack
tofu-apply:
	cd deploy/opentofu && $(TOFU) apply

## tofu-destroy: tear down the OpenTofu-managed stack (cluster remains)
tofu-destroy:
	cd deploy/opentofu && $(TOFU) destroy

## k8s-dev-env: full local Kubernetes environment (cluster + deploy)
k8s-dev-env: k8s-cluster-up tofu-apply

## k8s-dev-env-down: uninstall the release and delete the cluster
k8s-dev-env-down: tofu-destroy k8s-cluster-down

## k8s-verify: smoke-test the deployed stack through the ingress
k8s-verify:
	@echo "== ingress health =="; \
	curl -fsS -H "Host: events.localhost" localhost:18080/healthz; echo; \
	curl -fsS -H "Host: mcp.localhost" localhost:18090/healthz; echo; \
	echo "== publish a test event =="; \
	curl -fsS -X POST -H "Host: events.localhost" localhost:18080/api/v1/events \
		-H 'Content-Type: application/json' \
		-d '{"source":"make-verify","event_type":"document_uploaded","payload":{"document_id":"doc-make-001","title":"Verificacion","content":"Evento de prueba desde make k8s-verify."}}'

## images-build: build the four service images for $(REGISTRY)/$(IMG_TAG)
images-build:
	@for svc in $(SERVICES); do \
		echo "Building $$svc..."; \
		docker build -t "$(REGISTRY)/$$svc:$(IMG_TAG)" -f "deploy/docker/$$svc.Dockerfile" .; \
	done

## images-push: publish the images to $(REGISTRY) (needs docker login)
images-push: images-build
	@for svc in $(SERVICES); do \
		echo "Pushing $$svc..."; \
		docker push "$(REGISTRY)/$$svc:$(IMG_TAG)"; \
	done
