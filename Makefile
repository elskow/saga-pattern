GO_IMAGE ?= golang:1.25.0
K6_IMAGE ?= grafana/k6:0.55.0
NODE_IMAGE ?= node:20-alpine
CACHE_ROOT := .cache
GO_BUILD_CACHE := $(CACHE_ROOT)/go-build
GO_MOD_CACHE := $(CACHE_ROOT)/go-mod
SMOKE_RESULTS_DIR := test/parity/results

SERVER_PACKAGES := \
	./choreography-saga/order-service/cmd/server \
	./choreography-saga/payment-service/cmd/server \
	./choreography-saga/inventory-service/cmd/server \
	./choreography-saga/shipping-service/cmd/server \
	./orchestration-saga/order-service/cmd/server \
	./orchestration-saga/payment-service/cmd/server \
	./orchestration-saga/inventory-service/cmd/server \
	./orchestration-saga/shipping-service/cmd/server

BUILD_OUTPUTS := \
	build/bin/choreography-order-service \
	build/bin/choreography-payment-service \
	build/bin/choreography-inventory-service \
	build/bin/choreography-shipping-service \
	build/bin/orchestration-order-service \
	build/bin/orchestration-payment-service \
	build/bin/orchestration-inventory-service \
	build/bin/orchestration-shipping-service

DEFAULT_TEST_PACKAGES := ./common/... ./choreography-saga/... ./orchestration-framework/... ./orchestration-saga/...

.PHONY: help tidy build test test-parity clean clean-docker clean-all up-infra up-observability up-choreography up-orchestration up-dual-local status-dual-local down-dual-local down smoke-choreography smoke-choreography-go smoke-orchestration smoke-orchestration-go k6-choreography-quick k6-orchestration-quick thesis-compare-quick thesis-compare-baseline web-install web-dev web-build web-lint

define RUN_GO_SHELL
	@if command -v go >/dev/null 2>&1; then \
		/bin/sh -lc '$(1)'; \
	elif command -v docker >/dev/null 2>&1; then \
		mkdir -p $(GO_BUILD_CACHE) $(GO_MOD_CACHE); \
		docker run --rm \
			--network host \
			-u "$$(id -u):$$(id -g)" \
			-v "$(CURDIR):/workspace" \
			-w /workspace \
			-e GOCACHE=/workspace/$(GO_BUILD_CACHE) \
			-e GOMODCACHE=/workspace/$(GO_MOD_CACHE) \
			$(GO_IMAGE) \
			/bin/sh -lc 'export PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin; $(1)'; \
	else \
		printf 'error: neither go nor docker is available\n' >&2; \
		exit 1; \
	fi
endef

define RUN_NODE_SHELL
	@if command -v npm >/dev/null 2>&1; then \
		/bin/sh -lc 'cd web && $(1)'; \
	elif command -v docker >/dev/null 2>&1; then \
		docker run --rm \
			--network host \
			-u "$$(id -u):$$(id -g)" \
			-v "$(CURDIR)/web:/workspace" \
			-w /workspace \
			$(NODE_IMAGE) \
			/bin/sh -lc '$(1)'; \
	else \
		printf 'error: neither npm nor docker is available\n' >&2; \
		exit 1; \
	fi
endef

help:
	@printf '%s\n' \
		'Available targets:' \
		'  help                         Show this command summary' \
		'  tidy                         Sync go.mod and go.sum for the root Go workspace' \
		'  build                        Build all scaffolded Go service binaries into build/bin/' \
		'  test                         Run the default Go unit/integration suite (excludes live parity smoke)' \
		'  test-parity                  Run the live parity suite against a running stack' \
		'  clean                        Remove generated build and Go cache directories' \
		'  clean-docker                  Stop all stacks and remove saga images, volumes, and build cache' \
		'  clean-all                     Run clean + clean-docker (full reset)' \
		'  up-infra                     Start the existing local infra stack via local runner' \
		'  up-observability             Start Grafana local observability via local runner' \
		'  up-choreography              Start infra plus the local choreography stack via local runner' \
		'  up-orchestration             Start infra plus the local orchestration stack via local runner' \
		'  up-dual-local                Start shared infra/observability plus choreography and orchestration together' \
		'  status-dual-local            Show status for the dev-only dual local mode' \
		'  down-dual-local              Stop the dev-only dual local mode' \
		'  down                         Stop all local stacks via local runner' \
		'  smoke-choreography           Alias for smoke-choreography-go' \
		'  smoke-choreography-go        Run the parity smoke harness against the Go choreography stack' \
		'  smoke-orchestration          Alias for smoke-orchestration-go' \
		'  smoke-orchestration-go       Run the parity smoke harness against the Go orchestration stack' \
		'  k6-choreography-quick        Run the quick k6 choreography thesis smoke locally' \
		'  k6-orchestration-quick       Run the quick k6 orchestration thesis smoke locally' \
		'  web-install                  Install web app dependencies inside web/' \
		'  web-dev                      Start the web app locally on port 4173' \
		'  web-build                    Build the web app bundle into web/dist/' \
		'  web-lint                     Run the web app ESLint checks' \
		'  thesis-compare-quick         Run the paired quick thesis comparison protocol locally' \
		'  thesis-compare-baseline      Run the paired thesis-baseline protocol locally'

tidy:
	$(call RUN_GO_SHELL,go mod tidy)

build: tidy
	@mkdir -p build/bin
	$(call RUN_GO_SHELL,go build -o build/bin/choreography-order-service ./choreography-saga/order-service/cmd/server && go build -o build/bin/choreography-payment-service ./choreography-saga/payment-service/cmd/server && go build -o build/bin/choreography-inventory-service ./choreography-saga/inventory-service/cmd/server && go build -o build/bin/choreography-shipping-service ./choreography-saga/shipping-service/cmd/server && go build -o build/bin/orchestration-order-service ./orchestration-saga/order-service/cmd/server && go build -o build/bin/orchestration-payment-service ./orchestration-saga/payment-service/cmd/server && go build -o build/bin/orchestration-inventory-service ./orchestration-saga/inventory-service/cmd/server && go build -o build/bin/orchestration-shipping-service ./orchestration-saga/shipping-service/cmd/server)

test: tidy
	$(call RUN_GO_SHELL,go test $(DEFAULT_TEST_PACKAGES))

test-parity:
	$(call RUN_GO_SHELL,go test ./test/parity/...)

clean:
	rm -rf build $(CACHE_ROOT)

clean-docker: down
	@echo "Removing saga-pattern Docker images..."
	@docker images --format '{{.Repository}}:{{.Tag}}' | grep '^saga-pattern/' | xargs -r docker rmi -f 2>/dev/null || true
	@echo "Removing project Docker volumes..."
	@docker volume ls -q | grep -E '^(docker-local_|saga-dual-)' | xargs -r docker volume rm -f 2>/dev/null || true
	@echo "Removing dangling images and build cache..."
	@docker image prune -f 2>/dev/null || true
	@docker builder prune -f 2>/dev/null || true
	@echo "Docker cleanup complete."

clean-all: clean clean-docker

up-infra:
	./docker-local/local-runner.sh start-infra

up-observability:
	./docker-local/local-runner.sh start-infra
	./docker-local/local-runner.sh start-observability

up-choreography:
	./docker-local/local-runner.sh start-infra
	./docker-local/local-runner.sh start-observability
	./docker-local/local-runner.sh start-choreography

up-orchestration:
	./docker-local/local-runner.sh start-infra
	./docker-local/local-runner.sh start-observability
	./docker-local/local-runner.sh start-orchestration

up-dual-local:
	./docker-local/local-runner.sh start-dual-local

status-dual-local:
	./docker-local/local-runner.sh status-dual-local

down-dual-local:
	./docker-local/local-runner.sh stop-dual-local

down:
	./docker-local/local-runner.sh stop-all

smoke-choreography: smoke-choreography-go

smoke-choreography-go:
	@docker compose -p saga-dual-choreography -f "docker-local/docker-compose.choreography.local.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -p saga-dual-orchestration -f "docker-local/docker-compose.orchestration.local.yml" --profile scale4 down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "docker-local/docker-compose.choreography.local.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "docker-local/docker-compose.infra.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@./docker-local/local-runner.sh start-infra
	@./docker-local/local-runner.sh start-choreography
	@for port in 8081 8082 8083 8084; do \
		for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45; do \
			status=$$(curl -sf "http://localhost:$$port/actuator/health" | jq -r '.status // empty' 2>/dev/null || true); \
			if [ "$$status" = "UP" ]; then \
				break; \
			fi; \
			sleep 2; \
			if [ $$attempt -eq 45 ]; then \
				printf '%s\n' "error: choreography service on port $$port did not become healthy" >&2; \
				exit 1; \
			fi; \
		done; \
	done
	@for attempt in 1 2 3 4 5 6; do \
		order_id=$$(curl -sf -X POST http://localhost:8081/api/orders \
			-H 'Content-Type: application/json' \
			-d '{"customerId":"WARMUP-CHOR-'"$$attempt"'","shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":15999000}]}' | jq -r '.orderId // empty'); \
		if [ -z "$$order_id" ]; then \
			sleep 3; \
			continue; \
		fi; \
		for poll in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
			status=$$(curl -sf http://localhost:8081/api/orders/$$order_id | jq -r '.status // empty' 2>/dev/null || true); \
			if [ "$$status" = "COMPLETED" ] || [ "$$status" = "CANCELLED" ] || [ "$$status" = "FAILED" ]; then \
				break 2; \
			fi; \
			sleep 1; \
		done; \
		if [ $$attempt -eq 6 ]; then \
			printf '%s\n' 'error: choreography warmup order never reached a terminal state' >&2; \
			exit 1; \
		fi; \
		sleep 3; \
	done
	@mkdir -p $(SMOKE_RESULTS_DIR)
	$(call RUN_GO_SHELL,PARITY_PATTERN=choreography PARITY_STACK=go PARITY_BASE_URL=http://localhost:8081 go test -json ./test/parity/... > $(SMOKE_RESULTS_DIR)/smoke-choreography.json; status=$$?; cat $(SMOKE_RESULTS_DIR)/smoke-choreography.json; exit $$status)

smoke-orchestration: smoke-orchestration-go

smoke-orchestration-go:
	@docker compose -p saga-dual-choreography -f "docker-local/docker-compose.choreography.local.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -p saga-dual-orchestration -f "docker-local/docker-compose.orchestration.local.yml" --profile scale4 down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "docker-local/docker-compose.orchestration.local.yml" --profile scale4 down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "docker-local/docker-compose.choreography.local.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "docker-local/docker-compose.infra.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@./docker-local/local-runner.sh start-infra
	@./docker-local/local-runner.sh start-orchestration
	@for port in 8091 8092 8093 8094; do \
		for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45; do \
			status=$$(curl -sf "http://localhost:$$port/actuator/health" | jq -r '.status // empty' 2>/dev/null || true); \
			if [ "$$status" = "UP" ]; then \
				break; \
			fi; \
			sleep 2; \
			if [ $$attempt -eq 15 ]; then \
				printf '%s\n' "error: orchestration service on port $$port did not become healthy" >&2; \
				exit 1; \
			fi; \
		done; \
	done
	@for attempt in 1 2 3 4 5 6; do \
		order_id=$$(curl -sf -X POST http://localhost:8091/api/orders \
			-H 'Content-Type: application/json' \
			-d '{"customerId":"WARMUP-ORCH-GO-'"$$attempt"'","totalAmount":15999000,"shippingAddress":"Jl. Ketintang Wiyata, Surabaya 60231","items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":15999000}]}' | jq -r '.orderId // empty'); \
		if [ -z "$$order_id" ]; then \
			sleep 3; \
			continue; \
		fi; \
		for poll in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
			status=$$(curl -sf http://localhost:8091/api/orders/$$order_id | jq -r '.status // empty' 2>/dev/null || true); \
			if [ "$$status" = "COMPLETED" ] || [ "$$status" = "CANCELLED" ] || [ "$$status" = "FAILED" ]; then \
				break 2; \
			fi; \
			if [ "$$status" != "" ] && [ "$$status" != "CREATED" ]; then \
				break 2; \
			fi; \
			sleep 1; \
		done; \
		if [ $$attempt -eq 6 ]; then \
			printf '%s\n' 'error: orchestration warmup order never progressed beyond CREATED' >&2; \
			exit 1; \
		fi; \
		sleep 3; \
	done
	@mkdir -p $(SMOKE_RESULTS_DIR)
	$(call RUN_GO_SHELL,PARITY_PATTERN=orchestration PARITY_STACK=go PARITY_BASE_URL=http://localhost:8091 go test -json ./test/parity/... > $(SMOKE_RESULTS_DIR)/smoke-orchestration.json; status=$$?; cat $(SMOKE_RESULTS_DIR)/smoke-orchestration.json; exit $$status)

k6-choreography-quick:
	bash load-testing/thesis/run-k6-thesis.sh --pattern choreography --scenario successful-order --profile quick

k6-orchestration-quick:
	bash load-testing/thesis/run-k6-thesis.sh --pattern orchestration --scenario successful-order --profile quick

thesis-compare-quick:
	bash load-testing/thesis/run-k6-thesis.sh --pattern choreography --scenario successful-order --profile quick
	bash load-testing/thesis/run-k6-thesis.sh --pattern orchestration --scenario successful-order --profile quick

thesis-compare-baseline:
	bash load-testing/thesis/run-k6-thesis.sh --pattern choreography --scenario successful-order --profile thesis-baseline
	bash load-testing/thesis/run-k6-thesis.sh --pattern orchestration --scenario successful-order --profile thesis-baseline

web-install:
	$(call RUN_NODE_SHELL,npm install)

web-dev:
	$(call RUN_NODE_SHELL,npm run dev)

web-build:
	$(call RUN_NODE_SHELL,npm run build)

web-lint:
	$(call RUN_NODE_SHELL,npm run lint)
