SHELL := /bin/bash

GO_MODULES := services/ingestion-gateway services/metrics-aggregator services/mcp-server pkg/telemetry pkg/events
COMPOSE   := docker compose

.PHONY: dev-env dev-env-down lint build-all test clean tidy gen-types

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
