# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

VENV_DIR   := .venv
PYTHON_SYS ?= python3

ifneq (,$(wildcard $(VENV_DIR)/bin/python3))
  PYTHON := $(VENV_DIR)/bin/python3
else
  PYTHON := $(PYTHON_SYS)
endif

RUNNER := $(PYTHON) runner.py

WORKLOAD ?=
FILTER   ?=
SUITE    ?=
BASELINE ?=
POLICY   ?=
PARAMS   ?=
PID      ?=
GATE     ?= operator-runtime
INCLUDE_SOURCE ?=
BLESS_RUNS ?=
POLICY_EVOLUTION ?=
ARCHIVE ?=
ARCHIVE_CACHE ?=
RUN_ID ?=
PLATFORM ?= auto
POLICY_VERSION ?= auto
QUORUM_RETRIES ?=
AGAINST ?=

PARO_BENCH_THREAD_COUNT ?= 1
PARO_BENCH_QUERY_MEMORY_CAP ?= 256MB
PARO_BENCH_TEMP_DIRECTORY ?= /tmp/paro_benchmark_operator_runtime
PARO_BENCH_DATA_SCALE ?= checked-in
PARO_BENCH_RANDOM_SEED ?= 20260525
PARO_BENCH_SINGLE_TASK_FAST_PATH ?= current-single-active-driver
PARO_BENCH_ALLOC_AUDIT ?= 1
PARO_ALLOC_AUDIT ?= $(PARO_BENCH_ALLOC_AUDIT)

export PARO_BENCH_THREAD_COUNT
export PARO_BENCH_QUERY_MEMORY_CAP
export PARO_BENCH_TEMP_DIRECTORY
export PARO_BENCH_DATA_SCALE
export PARO_BENCH_RANDOM_SEED
export PARO_BENCH_SINGLE_TASK_FAST_PATH
export PARO_BENCH_ALLOC_AUDIT
export PARO_ALLOC_AUDIT

.DEFAULT_GOAL := help

.PHONY: setup run check bless calibrate bisect archive-manifest archive-recompute-calibration archive-finalize-calibration ci test ping clean clean-venv distclean help

setup: ## Create .venv and install benchmark dependencies
	@$(PYTHON_SYS) -m venv $(VENV_DIR)
	@$(VENV_DIR)/bin/pip install --upgrade pip -q
	@$(VENV_DIR)/bin/pip install -r requirements.txt -q
	@echo "Benchmark dependencies installed in $(VENV_DIR)"

run: ## Run benchmark (WORKLOAD= FILTER= SUITE= PARAMS="k=v a=b")
	$(RUNNER) run \
		$(if $(SUITE),--suite $(SUITE)) \
		$(if $(WORKLOAD),--workload $(WORKLOAD)) \
		$(if $(FILTER),--filter "$(FILTER)") \
		$(foreach p,$(PARAMS),--param $(p)) \
		$(if $(PID),--pid $(PID))

check: ## Run performance gate (GATE= INCLUDE_SOURCE= BASELINE= PID=)
	$(RUNNER) gate check \
		--gate $(GATE) \
		$(if $(BASELINE),--baseline $(BASELINE)) \
		$(if $(POLICY),--policy $(POLICY)) \
		$(if $(PID),--pid $(PID)) \
		$(if $(INCLUDE_SOURCE),--include-source $(INCLUDE_SOURCE)) \
		$(if $(ARCHIVE),--archive $(ARCHIVE)) \
		$(if $(ARCHIVE_CACHE),--archive-cache $(ARCHIVE_CACHE)) \
		$(if $(QUORUM_RETRIES),--quorum-retries $(QUORUM_RETRIES))

bless: ## Update gate baseline (GATE= BASELINE= PID=)
	$(RUNNER) gate bless \
		--gate $(GATE) \
		$(if $(BASELINE),--baseline $(BASELINE)) \
		$(if $(POLICY),--policy $(POLICY)) \
		$(if $(PID),--pid $(PID)) \
		$(if $(INCLUDE_SOURCE),--include-source $(INCLUDE_SOURCE)) \
		$(if $(BLESS_RUNS),--bless-runs $(BLESS_RUNS)) \
		$(if $(POLICY_EVOLUTION),--policy-evolution)

calibrate: ## Append clean gate observations to archive (GATE= ARCHIVE= RUN_ID=)
	$(RUNNER) gate calibrate \
		--gate $(GATE) \
		$(if $(BASELINE),--baseline $(BASELINE)) \
		$(if $(POLICY),--policy $(POLICY)) \
		$(if $(PID),--pid $(PID)) \
		$(if $(INCLUDE_SOURCE),--include-source $(INCLUDE_SOURCE)) \
		$(if $(ARCHIVE),--archive $(ARCHIVE)) \
		$(if $(ARCHIVE_CACHE),--archive-cache $(ARCHIVE_CACHE)) \
		$(if $(RUN_ID),--run-id $(RUN_ID))

bisect: ## Compare this checkout against archived gate result (GATE= AGAINST=<sha> ARCHIVE=)
	$(RUNNER) gate bisect \
		--gate $(GATE) \
		--against $(AGAINST) \
		$(if $(POLICY),--policy $(POLICY)) \
		$(if $(PID),--pid $(PID)) \
		$(if $(INCLUDE_SOURCE),--include-source $(INCLUDE_SOURCE)) \
		$(if $(ARCHIVE),--archive $(ARCHIVE)) \
		$(if $(ARCHIVE_CACHE),--archive-cache $(ARCHIVE_CACHE))

archive-manifest: ## Rebuild archive manifest from listing (ARCHIVE= GATE= PLATFORM= POLICY_VERSION=)
	$(PYTHON) -m harness.archive.manifest rebuild \
		--archive $(ARCHIVE) \
		--gate $(GATE) \
		--platform $(PLATFORM) \
		--policy-version $(POLICY_VERSION) \
		$(if $(POLICY),--policy $(POLICY)) \
		$(if $(ARCHIVE_CACHE),--cache $(ARCHIVE_CACHE))

archive-recompute-calibration: ## Recompute calibration from archived results (ARCHIVE= GATE= PLATFORM= POLICY=)
	$(PYTHON) -m harness.archive.calibration recompute \
		--archive $(ARCHIVE) \
		--policy $(if $(POLICY),$(POLICY),policies/$(GATE).toml) \
		--gate $(GATE) \
		--platform $(PLATFORM) \
		$(if $(ARCHIVE_CACHE),--cache $(ARCHIVE_CACHE))

archive-finalize-calibration: ## Rebuild manifest, recompute rolling calibration, and rebuild manifest again
	$(PYTHON) -m harness.archive.finalize \
		--archive $(ARCHIVE) \
		--gate $(GATE) \
		--platform $(PLATFORM) \
		--policy-version $(POLICY_VERSION) \
		$(if $(POLICY),--policy $(POLICY)) \
		$(if $(ARCHIVE_CACHE),--cache $(ARCHIVE_CACHE))

ci: ## Run CI benchmark suite (default: SUITE=pr-smoke)
	$(RUNNER) run \
		--suite $(if $(SUITE),$(SUITE),pr-smoke) \
		$(foreach p,$(PARAMS),--param $(p)) \
		$(if $(PID),--pid $(PID))

test: ## Run benchmark harness unit tests
	PYTHONPATH=.. $(PYTHON) -m unittest discover -s tests -p 'test_*.py' -v

ping: ## Verify Paro connectivity from config.toml
	@$(PYTHON) -c "\
import os, tomllib, pathlib, psycopg; \
cfg = tomllib.loads(pathlib.Path('config.toml').read_text()); \
c = cfg.get('connection', {}); \
conn = psycopg.connect( \
    host=os.getenv('PARO_HOST', c.get('host','localhost')), \
    port=int(os.getenv('PARO_PORT', c.get('port',6432))), \
    dbname=os.getenv('PARO_DATABASE', c.get('database','postgres')), \
    user=os.getenv('PARO_USER', c.get('user','postgres')), \
    password=os.getenv('PARO_PASSWORD', c.get('password','')) \
); \
print('OK'); \
conn.close()"

clean: ## Remove reports and Python cache files
	rm -rf report/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

clean-venv: ## Remove local virtual environment
	rm -rf $(VENV_DIR)

distclean: clean clean-venv ## Remove reports and dependencies

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "%-16s %s\n", $$1, $$2}'
