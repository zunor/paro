# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

# ──────────────────────────────────────────────────────────────
#  Paro SQL Regression Test Suite
# ──────────────────────────────────────────────────────────────
#
#  Quick start:
#    make setup          install Python deps into .venv
#    make check          run all regression cases
#    make check FILE=dml run cases matching "dml"
#    make update         regenerate all .result baselines
#
#  Configuration priority:  CLI flags  >  env vars  >  config.toml
# ──────────────────────────────────────────────────────────────

# ── Python / venv ────────────────────────────────────────────
VENV_DIR   := .venv
PYTHON_SYS ?= python3

# If .venv exists, use it; otherwise fall back to system python.
ifneq (,$(wildcard $(VENV_DIR)/bin/python3))
  PYTHON := $(VENV_DIR)/bin/python3
else
  PYTHON := $(PYTHON_SYS)
endif

RUNNER := $(PYTHON) runner.py

# ── Dependencies ─────────────────────────────────────────────
REQUIREMENTS := requirements.txt

# ── User-tunable knobs ───────────────────────────────────────
FILE  ?=
JOBS  ?= 4

# ── Colours (disabled when piped / NO_COLOR is set) ──────────
ifdef NO_COLOR
  _C := _E :=
else
  _C = \033[36m
  _Y = \033[33m
  _E = \033[0m
endif

# ═══════════════════════════════════════════════════════════════
#  Targets
# ═══════════════════════════════════════════════════════════════

.DEFAULT_GOAL := help

# ── Setup ────────────────────────────────────────────────────
.PHONY: setup

setup: ## Create .venv and install dependencies
	@echo "Creating virtual environment in $(VENV_DIR) …"
	@$(PYTHON_SYS) -m venv $(VENV_DIR)
	@$(VENV_DIR)/bin/pip install --upgrade pip -q
	@$(VENV_DIR)/bin/pip install -r $(REQUIREMENTS) -q
	@echo "Done.  Activate with:  source $(VENV_DIR)/bin/activate"

# ── Run Tests ────────────────────────────────────────────────
.PHONY: check test

check: ## Run regression cases (FILE=<pattern> to filter)
ifdef FILE
	$(RUNNER) --filter "$(FILE)"
else
	$(RUNNER)
endif

test: check ## Alias for 'check'

# ── Update Baselines ─────────────────────────────────────────
.PHONY: update

update: ## Regenerate .result baselines (FILE=<pattern> to scope)
ifdef FILE
	$(RUNNER) --update --filter "$(FILE)"
else
	$(RUNNER) --update
endif

# ── Parallel / CI ────────────────────────────────────────────
.PHONY: check-parallel ci

check-parallel: ## Run with --jobs (JOBS=N, default 4)
	$(RUNNER) --jobs $(JOBS)

ci: ## CI mode: compare-only, emit *.result.actual
	PARO_UPDATE=0 PARO_WRITE_ACTUAL=1 $(RUNNER) --verbose

# ── Unit Tests ───────────────────────────────────────────────
.PHONY: unit

unit: ## Run harness unit tests
	$(PYTHON) -m unittest discover -s unit -p 'test_*.py' -v

# ── Utilities ────────────────────────────────────────────────
.PHONY: ping clean help

ping: ## Verify Paro server connectivity
	@$(PYTHON) -c "\
	import tomllib, pathlib, sys;\
	cfg = tomllib.loads(pathlib.Path('config.toml').read_text());\
	c = cfg.get('connection', {});\
	h, p, d = c.get('host','localhost'), c.get('port',6432), c.get('database','postgres');\
	print(f'Connecting to {h}:{p}/{d} …');\
	import psycopg;\
	conn = psycopg.connect(host=h, port=p, dbname=d, user=c.get('user','postgres'), password=c.get('password',''));\
	print('OK – server is reachable');\
	conn.close()" 2>&1 || (echo "FAIL – cannot reach Paro server" && exit 1)

clean: ## Remove .venv, __pycache__, and report artifacts
	rm -rf $(VENV_DIR) report/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# ── Help ─────────────────────────────────────────────────────
help: ## Show this help
	@printf "\n  $(_C)Paro Regression Test Suite$(_E)\n\n"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(_C)%-18s$(_E) %s\n", $$1, $$2}'
	@printf "\n  $(_Y)Variables:$(_E)\n"
	@printf "  $(_C)%-18s$(_E) %s\n" "FILE=<pattern>"  "Filter cases by substring"
	@printf "  $(_C)%-18s$(_E) %s\n" "JOBS=N"          "Worker count for check-parallel (default: 4)"
	@printf "  $(_C)%-18s$(_E) %s\n" "PYTHON_SYS=…"   "Override system Python binary"
	@printf "\n"
