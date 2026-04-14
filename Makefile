GO_IMAGE ?= golang:1.25.0
MAVEN_IMAGE ?= maven:3.9.9-eclipse-temurin-17
CACHE_ROOT := .cache
GO_BUILD_CACHE := $(CACHE_ROOT)/go-build
GO_MOD_CACHE := $(CACHE_ROOT)/go-mod
MAVEN_CACHE := $(CACHE_ROOT)/m2
SMOKE_RESULTS_DIR := test/parity/results
COMMA := ,

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

COMPATIBILITY_TEST_PACKAGES := ./test/compatibility/...
THESIS_SURFACE_TEST_REGEX := TestCompatibilityMatrixComplete|TestGatlingSurfaceFixtures|TestRejectsMissingMetricOrTopicFixture|TestDocsMatchCompatibilityMatrix
DEFAULT_TEST_PACKAGES := ./common/... ./choreography-saga/... ./orchestration-framework/... ./orchestration-saga/... ./test/compatibility/...

.PHONY: help tidy build test test-parity clean up-infra up-choreography up-orchestration down smoke-choreography smoke-choreography-go smoke-orchestration smoke-orchestration-go gatling-choreography-quick gatling-orchestration-quick verify-thesis-surface verify-jenkins-benchmark

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

define RUN_MAVEN_SHELL
	@if command -v mvn >/dev/null 2>&1; then \
		/bin/sh -lc '$(1)'; \
	elif command -v docker >/dev/null 2>&1; then \
		mkdir -p $(MAVEN_CACHE); \
		docker run --rm \
			--network host \
			-u "$$(id -u):$$(id -g)" \
			-v "$(CURDIR):/workspace" \
			-w /workspace \
			-e MAVEN_CONFIG=/workspace/$(MAVEN_CACHE) \
			$(MAVEN_IMAGE) \
			/bin/sh -lc 'export PATH=/usr/share/maven/bin:/usr/local/bin:/usr/bin:/bin; $(1)'; \
	else \
		printf 'error: neither mvn nor docker is available\n' >&2; \
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
		'  up-infra                     Start the existing local infra stack via local runner' \
		'  up-choreography              Start infra plus the local choreography stack via local runner' \
		'  up-orchestration             Start infra plus the local orchestration stack via local runner' \
		'  down                         Stop all local stacks via local runner' \
		'  smoke-choreography           Alias for smoke-choreography-go' \
		'  smoke-choreography-go        Run the parity smoke harness against the Go choreography stack' \
		'  smoke-orchestration          Alias for smoke-orchestration-go' \
		'  smoke-orchestration-go       Run the parity smoke harness against the Go orchestration stack' \
		'  verify-thesis-surface        Verify docs, metrics, and commands against the frozen compatibility matrix' \
		'  verify-jenkins-benchmark     Verify Jenkins benchmark parameters and structure expectations' \
		'  gatling-choreography-quick   Run the existing quick Gatling choreography suite locally' \
		'  gatling-orchestration-quick  Run the existing quick Gatling orchestration suite locally'

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

up-infra:
	./local-testing/local-runner.sh start-infra

up-choreography:
	./local-testing/local-runner.sh start-infra
	./local-testing/local-runner.sh start-choreography

up-orchestration:
	./local-testing/local-runner.sh start-infra
	./local-testing/local-runner.sh start-orchestration

down:
	./local-testing/local-runner.sh stop-all

smoke-choreography: smoke-choreography-go

smoke-choreography-go:
	@docker compose -f "local-testing/docker-compose.choreography.local.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "local-testing/docker-compose.infra.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "local-testing/docker-compose.infra.yml" up -d
	@docker compose -f "local-testing/docker-compose.choreography.local.yml" up -d --build
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
			-d '{"customerId":"WARMUP-CHOR-'"$$attempt"'","shippingAddress":"123 Warmup Street","items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":999.99}]}' | jq -r '.orderId // empty'); \
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
	@docker compose -f "local-testing/docker-compose.orchestration.local.yml" --profile scale4 down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "local-testing/docker-compose.choreography.local.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "local-testing/docker-compose.infra.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f "local-testing/docker-compose.infra.yml" up -d
	@docker compose -f "local-testing/docker-compose.orchestration.local.yml" up -d --build
	@for port in 8085 8091 8092 8093; do \
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
		order_id=$$(curl -sf -X POST http://localhost:8085/api/orders \
			-H 'Content-Type: application/json' \
			-d '{"customerId":"WARMUP-ORCH-GO-'"$$attempt"'","totalAmount":999.99,"shippingAddress":"123 Warmup Street","items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":999.99}]}' | jq -r '.orderId // empty'); \
		if [ -z "$$order_id" ]; then \
			sleep 3; \
			continue; \
		fi; \
		for poll in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
			status=$$(curl -sf http://localhost:8085/api/orders/$$order_id | jq -r '.status // empty' 2>/dev/null || true); \
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
	$(call RUN_GO_SHELL,PARITY_PATTERN=orchestration PARITY_STACK=go PARITY_BASE_URL=http://localhost:8085 go test -json ./test/parity/... > $(SMOKE_RESULTS_DIR)/smoke-orchestration.json; status=$$?; cat $(SMOKE_RESULTS_DIR)/smoke-orchestration.json; exit $$status)

gatling-choreography-quick:
	$(call RUN_MAVEN_SHELL,mvn -f load-testing/gatling/pom.xml -Pchoreography$(COMMA)quick -Dgatling.simulationClass=simulations.HappyPathSimulation gatling:test)

gatling-orchestration-quick:
	$(call RUN_MAVEN_SHELL,mvn -f load-testing/gatling/pom.xml -Porchestration$(COMMA)quick -Dgatling.simulationClass=simulations.HappyPathSimulation gatling:test)

verify-thesis-surface:
	$(call RUN_GO_SHELL,go test $(COMPATIBILITY_TEST_PACKAGES) -run "$(THESIS_SURFACE_TEST_REGEX)")

verify-jenkins-benchmark:
	bash scripts/ci/verify-jenkins-benchmark.sh
