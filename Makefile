# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

.PHONY: build release run test check header fmt fmt-check clippy actionlint static clean qa ci-local ci-local-stop regress regress-setup regress-update regress-ci regress-unit bench bench-ci bench-check bench-bless bench-setup bench-clean bench-ping python-udf-setup python-udf-unit python-udf-regress python-udf-startup-smoke python-udf-ci

# Build the project
build:
	cargo build

# Build release version
release:
	cargo build --release

# Run the server
run:
	RUST_BACKTRACE=1 RUST_LOG=info cargo run -p paro-server --bin parod -- --listen $(PARO_HOST):$(PARO_PORT)

# Run all tests
test:
	cargo test --workspace --locked

# Check compilation without building
check:
	cargo check

# Validate repository-wide license headers
header:
	python3 tools/ci/check_headers.py

# Format code
fmt:
	cargo fmt --all

# Check formatting without modifying files
fmt-check:
	cargo fmt --all --check

# Run clippy linter
clippy:
	cargo clippy --workspace --all-targets --locked -- -D warnings

# Lint GitHub Actions workflows when actionlint is installed locally
actionlint:
	@if command -v actionlint >/dev/null 2>&1; then \
		actionlint; \
	else \
		echo "actionlint not found, skipping"; \
	fi

# Run the static checks used at the front of CI
static:
	@echo "══════ [1/4] header ══════"
	$(MAKE) header
	@echo "══════ [2/4] rustfmt ══════"
	$(MAKE) fmt-check
	@echo "══════ [3/4] clippy ══════"
	$(MAKE) clippy
	@echo "══════ [4/4] actionlint ══════"
	$(MAKE) actionlint

# Clean build artifacts
clean:
	cargo clean

# Run all quality checks
qa: static test

# ── Local CI (mirrors GitHub Actions pipeline) ───────────────
PARO_HOST   ?= 127.0.0.1
PARO_PORT   ?= 6432
CI_HOST     ?= 127.0.0.1
CI_PORT     ?= 6432
CI_DATA_DIR ?= .ci/parod-data
CI_LOG      ?= .ci/parod.log
CI_PID      ?= .ci/parod.pid
PYTHON3     ?= python3
PYTHON_UDF_FILTER ?= python_udf
PYTHON_UDF_SMOKE_HOST ?= 127.0.0.1
PYTHON_UDF_SMOKE_PORT ?= 6544
PYTHON_UDF_SMOKE_DATA_DIR ?= .ci/python-udf-startup-data
PYTHON_UDF_SMOKE_LOG ?= .ci/python-udf-startup.log
PYTHON_UDF_SMOKE_PID ?= .ci/python-udf-startup.pid
PYTHON_UDF_SMOKE_CLIENT ?= regress/.venv/bin/python3

ci-local: ## Run the full CI pipeline locally (static → build → test → regress unit → startup smoke → regress → benchmark)
	@$(MAKE) static
	@echo "══════ [1/6] build (release) ══════"
	cargo build --release -p paro-server --bin parod --locked
	@echo "══════ [2/6] unit tests ══════"
	cargo test --workspace --locked
	@echo "══════ [3/6] regress harness unit ══════"
	$(MAKE) -C regress unit
	@echo "══════ [4/6] python runtime startup smoke ══════"
	$(MAKE) python-udf-startup-smoke
	@echo "══════ starting parod for regress + benchmark ══════"
	@set -e; \
	cleanup() { \
		if [ -f "$(CI_PID)" ]; then \
			kill "$$(cat "$(CI_PID)")" 2>/dev/null || true; \
			wait "$$(cat "$(CI_PID)")" 2>/dev/null || true; \
			rm -f "$(CI_PID)"; \
		fi; \
	}; \
	trap cleanup EXIT INT TERM; \
	mkdir -p .ci; \
	rm -rf "$(CI_DATA_DIR)"; \
	mkdir -p "$(CI_DATA_DIR)"; \
	./target/release/parod --listen $(CI_HOST):$(CI_PORT) --data-dir "$(CI_DATA_DIR)" > "$(CI_LOG)" 2>&1 & echo $$! > "$(CI_PID)"; \
	for i in $$(seq 1 60); do \
		if python3 -c "import socket; socket.create_connection(('$(CI_HOST)', $(CI_PORT)), timeout=1).close()" 2>/dev/null; then \
			echo "parod is ready on $(CI_HOST):$(CI_PORT)"; \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "FAIL: parod did not start"; \
			cat "$(CI_LOG)"; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	echo "══════ [5/6] regression ══════"; \
	PARO_HOST=$(CI_HOST) PARO_PORT=$(CI_PORT) $(MAKE) -C regress ci; \
	echo "══════ [6/6] benchmark ══════"; \
	PARO_HOST=$(CI_HOST) PARO_PORT=$(CI_PORT) $(MAKE) -C benchmark ci; \
	trap - EXIT INT TERM; \
	cleanup; \
	echo ""; \
	echo "══════ ALL CI CHECKS PASSED ══════"

ci-local-stop: ## Stop any leftover ci-local parod process
	@if [ -f $(CI_PID) ]; then kill "$$(cat $(CI_PID))" 2>/dev/null || true; rm -f $(CI_PID); echo "Stopped."; \
	else echo "No ci-local parod running."; fi

# ── SQL Regression (proxy into regress/) ─────────────────────
regress: ## Run SQL regression suite
	@$(MAKE) -C regress check $(if $(FILE),FILE=$(FILE))

regress-setup: ## Install regress Python deps
	@$(MAKE) -C regress setup

regress-update: ## Regenerate .result baselines
	@$(MAKE) -C regress update $(if $(FILE),FILE=$(FILE))

regress-ci: ## CI mode regression
	@PARO_HOST=$(PARO_HOST) PARO_PORT=$(PARO_PORT) $(MAKE) -C regress ci

regress-unit: ## Run harness unit tests
	@$(MAKE) -C regress unit

# ── Python UDF (project-level proxy + smoke) ─────────────────
python-udf-setup: ## Install regress dependencies used by Python UDF regress and smoke flows
	@$(MAKE) -C regress setup

python-udf-unit: ## Run ABI/runtime Rust tests plus Python worker and SDK unit tests
	cargo test -p paro-external-abi --test conformance --locked
	cargo test -p paro-external-runtime --test runtime --locked
	PYTHONPATH=python/paro_udf/src:runtimes/python-worker/src $(PYTHON3) -m unittest discover -s runtimes/python-worker/tests -p 'test_*.py' -v
	PYTHONPATH=python/paro_udf/src:runtimes/python-worker/src $(PYTHON3) -m unittest discover -s python/paro_udf/tests -p 'test_*.py' -v

python-udf-regress: ## Run real-worker SQL regression cases under regress/cases/python_udf/
	@$(MAKE) python-udf-setup
	@PARO_HOST=$(PARO_HOST) PARO_PORT=$(PARO_PORT) PARO_USER=paro $(MAKE) -C regress check FILE=$(PYTHON_UDF_FILTER)

python-udf-startup-smoke: ## Verify parod starts and serves ordinary SQL when Python runtime is disabled
	@$(MAKE) python-udf-setup
	@set -e; \
	cleanup() { \
		if [ -f "$(PYTHON_UDF_SMOKE_PID)" ]; then \
			kill "$$(cat "$(PYTHON_UDF_SMOKE_PID)")" 2>/dev/null || true; \
			wait "$$(cat "$(PYTHON_UDF_SMOKE_PID)")" 2>/dev/null || true; \
			rm -f "$(PYTHON_UDF_SMOKE_PID)"; \
		fi; \
	}; \
	trap cleanup EXIT INT TERM; \
	mkdir -p .ci; \
	rm -rf "$(PYTHON_UDF_SMOKE_DATA_DIR)"; \
	mkdir -p "$(PYTHON_UDF_SMOKE_DATA_DIR)"; \
	cargo build --release -p paro-server --bin parod --locked; \
	PARO_PYTHON_RUNTIME_DISABLED=1 ./target/release/parod --listen $(PYTHON_UDF_SMOKE_HOST):$(PYTHON_UDF_SMOKE_PORT) --data-dir "$(PYTHON_UDF_SMOKE_DATA_DIR)" > "$(PYTHON_UDF_SMOKE_LOG)" 2>&1 & echo $$! > "$(PYTHON_UDF_SMOKE_PID)"; \
	for i in $$(seq 1 60); do \
		if $(PYTHON3) -c "import socket; socket.create_connection(('$(PYTHON_UDF_SMOKE_HOST)', $(PYTHON_UDF_SMOKE_PORT)), timeout=1).close()" 2>/dev/null; then \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "FAIL: parod did not start for python startup smoke"; \
			cat "$(PYTHON_UDF_SMOKE_LOG)"; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	"$(PYTHON_UDF_SMOKE_CLIENT)" tools/ci/python_runtime_startup_smoke.py --host $(PYTHON_UDF_SMOKE_HOST) --port $(PYTHON_UDF_SMOKE_PORT); \
	trap - EXIT INT TERM; \
	cleanup

python-udf-ci: ## Run Python UDF unit tests, startup smoke, and dedicated SQL regress
	@$(MAKE) python-udf-unit
	@$(MAKE) python-udf-startup-smoke
	@set -e; \
	cleanup() { \
		if [ -f "$(CI_PID)" ]; then \
			kill "$$(cat "$(CI_PID)")" 2>/dev/null || true; \
			wait "$$(cat "$(CI_PID)")" 2>/dev/null || true; \
			rm -f "$(CI_PID)"; \
		fi; \
	}; \
	trap cleanup EXIT INT TERM; \
	mkdir -p .ci; \
	rm -rf "$(CI_DATA_DIR)"; \
	mkdir -p "$(CI_DATA_DIR)"; \
	cargo build --release -p paro-server --bin parod --locked; \
	./target/release/parod --listen $(CI_HOST):$(CI_PORT) --data-dir "$(CI_DATA_DIR)" > "$(CI_LOG)" 2>&1 & echo $$! > "$(CI_PID)"; \
	for i in $$(seq 1 60); do \
		if $(PYTHON3) -c "import socket; socket.create_connection(('$(CI_HOST)', $(CI_PORT)), timeout=1).close()" 2>/dev/null; then \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "FAIL: parod did not start"; \
			cat "$(CI_LOG)"; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	PARO_HOST=$(CI_HOST) PARO_PORT=$(CI_PORT) PARO_USER=paro $(MAKE) -C regress ci FILE=$(PYTHON_UDF_FILTER); \
	trap - EXIT INT TERM; \
	cleanup

# ── Benchmark (proxy into benchmark/) ────────────────────────
bench: ## Run benchmark (WORKLOAD= FILTER= SUITE= PARAMS= PID=)
	@$(MAKE) -C benchmark run \
		$(if $(SUITE),SUITE=$(SUITE)) \
		$(if $(WORKLOAD),WORKLOAD=$(WORKLOAD)) \
		$(if $(FILTER),FILTER=$(FILTER)) \
		$(if $(PARAMS),PARAMS="$(PARAMS)") \
		$(if $(PID),PID=$(PID))

bench-ci: ## Run benchmark CI suite (default PARO_HOST/PARO_PORT = 127.0.0.1:6432)
	@PARO_HOST=$(PARO_HOST) PARO_PORT=$(PARO_PORT) $(MAKE) -C benchmark ci

bench-check: ## Compare benchmark against baseline (BASELINE=path)
	@$(MAKE) -C benchmark check \
		$(if $(BASELINE),BASELINE=$(abspath $(BASELINE))) \
		$(if $(SUITE),SUITE=$(SUITE)) \
		$(if $(WORKLOAD),WORKLOAD=$(WORKLOAD)) \
		$(if $(FILTER),FILTER=$(FILTER)) \
		$(if $(PARAMS),PARAMS="$(PARAMS)") \
		$(if $(PID),PID=$(PID))

bench-bless: ## Update benchmark baseline (BASELINE=path)
	@$(MAKE) -C benchmark bless \
		$(if $(BASELINE),BASELINE=$(abspath $(BASELINE))) \
		$(if $(SUITE),SUITE=$(SUITE)) \
		$(if $(WORKLOAD),WORKLOAD=$(WORKLOAD)) \
		$(if $(FILTER),FILTER=$(FILTER)) \
		$(if $(PARAMS),PARAMS="$(PARAMS)") \
		$(if $(PID),PID=$(PID))

bench-setup: ## Install benchmark Python dependencies
	@$(MAKE) -C benchmark setup

bench-clean: ## Remove benchmark reports and venv
	@$(MAKE) -C benchmark distclean

bench-ping: ## Verify benchmark can connect to Paro
	@$(MAKE) -C benchmark ping
