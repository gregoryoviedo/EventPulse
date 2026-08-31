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
lint:
	golangci-lint run ./...
	@command -v flake8 >/dev/null 2>&1 && flake8 services/document-processor || echo "flake8 not installed, skipping python lint"

## build-all: build every Go module and the docker images
build-all:
	go build ./...
	$(COMPOSE) build

## test: run Go tests across all modules
test:
	go test ./...

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
