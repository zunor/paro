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
PARAMS   ?=
PID      ?=

.DEFAULT_GOAL := help

.PHONY: setup run check bless ci ping clean clean-venv distclean help

setup: ## Create .venv and install benchmark dependencies
	@$(PYTHON_SYS) -m venv $(VENV_DIR)
	@$(VENV_DIR)/bin/pip install --upgrade pip -q
	@$(VENV_DIR)/bin/pip install -r requirements.txt -q
	@echo "Benchmark dependencies installed in $(VENV_DIR)"

run: ## Run benchmark (WORKLOAD= FILTER= SUITE= PARAMS="k=v a=b")
	$(RUNNER) \
		$(if $(SUITE),--suite $(SUITE)) \
		$(if $(WORKLOAD),--workload $(WORKLOAD)) \
		$(if $(FILTER),--filter "$(FILTER)") \
		$(foreach p,$(PARAMS),--param $(p)) \
		$(if $(PID),--pid $(PID))

check: ## Run benchmark and compare against baseline (BASELINE=path)
	@test -n "$(BASELINE)" || (echo "BASELINE is required" && exit 2)
	$(RUNNER) \
		$(if $(SUITE),--suite $(SUITE)) \
		$(if $(WORKLOAD),--workload $(WORKLOAD)) \
		$(if $(FILTER),--filter "$(FILTER)") \
		$(foreach p,$(PARAMS),--param $(p)) \
		$(if $(PID),--pid $(PID)) \
		--check $(BASELINE)

bless: ## Update baseline from current run (BASELINE=path)
	@test -n "$(BASELINE)" || (echo "BASELINE is required" && exit 2)
	$(RUNNER) \
		$(if $(SUITE),--suite $(SUITE)) \
		$(if $(WORKLOAD),--workload $(WORKLOAD)) \
		$(if $(FILTER),--filter "$(FILTER)") \
		$(foreach p,$(PARAMS),--param $(p)) \
		$(if $(PID),--pid $(PID)) \
		--bless $(BASELINE)

ci: ## Run CI benchmark suite (default: SUITE=pr-smoke)
	$(RUNNER) \
		--suite $(if $(SUITE),$(SUITE),pr-smoke) \
		$(foreach p,$(PARAMS),--param $(p)) \
		$(if $(PID),--pid $(PID))

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
