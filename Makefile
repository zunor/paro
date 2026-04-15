.PHONY: build release run test check fmt clippy clean qa ci-local ci-local-stop regress regress-setup regress-update regress-ci regress-unit bench bench-ci bench-check bench-bless bench-setup bench-clean bench-ping

# Build the project
build:
	cargo build

# Build release version
release:
	cargo build --release

# Run the server
run:
	RUST_BACKTRACE=1 RUST_LOG=info cargo run -p paro-server --bin parod

# Run all tests
test:
	cargo test --workspace --locked

# Check compilation without building
check:
	cargo check

# Format code
fmt:
	cargo fmt --all

# Run clippy linter
clippy:
	cargo clippy --workspace --all-targets --locked -- -D warnings

# Clean build artifacts
clean:
	cargo clean

# Run all quality checks
qa: fmt clippy test

# ── Local CI (mirrors GitHub Actions pipeline) ───────────────
PARO_HOST   ?= 127.0.0.1
PARO_PORT   ?= 6432
CI_HOST     ?= 127.0.0.1
CI_PORT     ?= 6432
CI_DATA_DIR ?= .ci/parod-data
CI_LOG      ?= .ci/parod.log
CI_PID      ?= .ci/parod.pid

ci-local: ## Run the full CI pipeline locally (fmt → clippy → build → test → regress unit → regress → benchmark)
	@echo "══════ [1/7] rustfmt ══════"
	cargo fmt --all --check
	@echo "══════ [2/7] clippy ══════"
	cargo clippy --workspace --all-targets --locked -- -D warnings
	@echo "══════ [3/7] build (release) ══════"
	cargo build --release -p paro-server --bin parod --locked
	@echo "══════ [4/7] unit tests ══════"
	cargo test --workspace --locked
	@echo "══════ [5/7] regress harness unit ══════"
	$(MAKE) -C regress unit
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
	echo "══════ [6/7] regression ══════"; \
	PARO_HOST=$(CI_HOST) PARO_PORT=$(CI_PORT) $(MAKE) -C regress ci; \
	echo "══════ [7/7] benchmark ══════"; \
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
