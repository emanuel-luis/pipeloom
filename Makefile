# Developer entry point. Needs bash + make (Linux, macOS or WSL) and uv.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

.PHONY: help install lint format typecheck test spark-test airflow-test examples-data examples-test build docs docs-serve \
	d2-install diagrams diagrams-check diagrams-watch

help: ## list the targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "} {printf "  %-16s %s\n", $$1, $$2}'

install: ## install every dependency group into the uv environment
	uv sync --all-groups --all-packages

lint: ## ruff check, format check and import-linter contracts
	uv run ruff check .
	uv run ruff format --check .
	uv run lint-imports

format: ## apply ruff fixes and formatting
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## mypy --strict on src and the plugin sources
	uv run mypy --strict src plugins/pipeloom-polars/src plugins/pipeloom-pandas/src \
		plugins/pipeloom-spark/src

test: ## pytest, core and plugins
	uv run pytest -q

spark-test: ## print the WSL recipe for the Spark tests (Spark never runs in Windows Python)
	@echo "Spark tests need Linux and Java 17 or 21. On Windows, run them in WSL:"
	@echo "  wsl -d Ubuntu -e bash /mnt/d/<path>/pipeloom/scripts/wsl-spark-tests.sh classic"
	@echo "  wsl -d Ubuntu -e bash /mnt/d/<path>/pipeloom/scripts/wsl-spark-tests.sh connect"
	@echo "Extra arguments go to pytest; PIPELOOM_WSL_VENV picks the virtualenv (default ~/.venvs/pipeloom-spark)."
	@echo "On Linux or macOS: bash scripts/wsl-spark-tests.sh classic"

airflow-test: ## print the recipe for the Airflow tests (Airflow does not run on Windows)
	@echo "Airflow tests need Linux or macOS. On Windows, run them in WSL:"
	@echo "  wsl -d Ubuntu -e bash /mnt/d/<path>/pipeloom/scripts/wsl-airflow-tests.sh"
	@echo "The script makes a throwaway AIRFLOW_HOME with a sqlite database and removes it after."
	@echo "Extra arguments go to pytest; PIPELOOM_WSL_VENV picks the virtualenv (default ~/.venvs/pipeloom-airflow);"
	@echo "PIPELOOM_AIRFLOW_PIN=3.2.2 with its own PIPELOOM_WSL_VENV tests the floor."
	@echo "On Linux or macOS: bash scripts/wsl-airflow-tests.sh"

examples-data: ## generate the TPC-H parquet files for the examples (DuckDB, scale factor 0.01)
	uv run examples/tpch_data.py

examples-test: ## polars vs pandas parity test on the generated data (run examples-data first)
	PIPELOOM_TEST_EXAMPLES=1 uv run pytest -q tests/examples

build: ## build every distribution (core and plugins) into dist/
	uv build --all-packages

docs: ## build the docs site, failing on any warning
	uv run --all-packages --group docs mkdocs build --strict

docs-serve: ## serve the docs site with live reload
	uv run --all-packages --group docs mkdocs serve

# Diagrams: docs/diagrams/<name>.d2 rendered to <name>.light.svg and <name>.dark.svg (committed),
# one theme per file so the docs follow the site's palette toggle, not the OS. One pinned D2
# everywhere. Layout engine, theme and padding live in docs/diagrams/_theme.d2 (d2-config); the
# dark render copies the sources to a temp dir with _theme-dark.d2 in place of _theme.d2. d2 runs
# in a clean environment: D2_* and SCALE variables would override that config. --scale 1 is not a
# d2-config key; it writes the natural width and height, so GitHub never upscales a small diagram.
D2_VERSION := 0.9.0
# sha256 of d2-v$(D2_VERSION)-linux-amd64.tar.gz, from the release's SHA256SUMS
D2_SHA256 := 5669ddc46b99e942cc96078f4a4e36d5e62103348f4c05179ede27802fdd87a9
D2_HOME ?= $(HOME)/.local/share/d2/v$(D2_VERSION)
D2 ?= $(D2_HOME)/bin/d2
D2_RUN = env -i PATH="$$PATH" HOME="$$HOME" "$(D2)" --scale 1
DIAGRAMS := $(filter-out docs/diagrams/_%,$(wildcard docs/diagrams/*.d2))
D2_CHECK_VERSION = v="$$($(D2_RUN) --version 2>/dev/null || true)"; [ "$$v" = "v$(D2_VERSION)" ] || { \
	echo "need D2 v$(D2_VERSION) at $(D2), found '$${v:-nothing}': run make d2-install" >&2; exit 1; }
# Render every diagram in both themes into $$out, using $$tmp as scratch (both set by the caller).
D2_RENDER = for t in light dark; do mkdir -p "$$tmp/$$t"; cp docs/diagrams/*.d2 "$$tmp/$$t/"; done; \
	cp docs/diagrams/_theme-dark.d2 "$$tmp/dark/_theme.d2"; \
	for f in $(DIAGRAMS); do n="$$(basename "$${f%.d2}")"; \
		for t in light dark; do $(D2_RUN) "$$tmp/$$t/$$n.d2" "$$out/$$n.$$t.svg"; done; done

d2-install: ## install the pinned D2 (Linux x86_64) into D2_HOME, checksum verified
	@if [ -x "$(D2_HOME)/bin/d2" ]; then echo "D2 v$(D2_VERSION) already in $(D2_HOME)"; exit 0; fi; \
	[ "$$(uname -sm)" = "Linux x86_64" ] || { echo "d2-install covers Linux x86_64 only;" \
		"install D2 v$(D2_VERSION) yourself and pass D2=/path/to/d2" >&2; exit 1; }; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSLo "$$tmp/d2.tar.gz" \
		"https://github.com/terrastruct/d2/releases/download/v$(D2_VERSION)/d2-v$(D2_VERSION)-linux-amd64.tar.gz"; \
	echo "$(D2_SHA256)  $$tmp/d2.tar.gz" | sha256sum --check --strict; \
	tar -xzf "$$tmp/d2.tar.gz" -C "$$tmp"; \
	mkdir -p "$(D2_HOME)/bin"; \
	install -m 0755 "$$tmp/d2-v$(D2_VERSION)/bin/d2" "$(D2_HOME)/bin/d2"; \
	"$(D2_HOME)/bin/d2" --version

diagrams: ## render docs/diagrams/*.d2 to light and dark SVGs with the pinned D2 (run make d2-install once)
	@$(D2_CHECK_VERSION)
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; out="$$tmp/out"; mkdir "$$out"; $(D2_RENDER); \
	rm -f docs/diagrams/*.svg; cp "$$out"/*.svg docs/diagrams/

diagrams-check: ## render to a temp dir, fail if a committed SVG is stale, missing or has no source
	@$(D2_CHECK_VERSION)
	@shopt -s nullglob; tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; out="$$tmp/out"; mkdir "$$out"; \
	$(D2_RENDER); rc=0; \
	for s in "$$out"/*.svg; do c="docs/diagrams/$$(basename "$$s")"; \
		cmp -s "$$s" "$$c" || { echo "$$c is stale or missing" >&2; rc=1; }; done; \
	for c in docs/diagrams/*.svg; do [ -f "$$out/$$(basename "$$c")" ] || { \
		echo "$$c has no .d2 source (expected <name>.light.svg and <name>.dark.svg)" >&2; rc=1; }; done; \
	[ "$$rc" = 0 ] || { echo "run make diagrams and commit the SVGs" >&2; exit 1; }

diagrams-watch: ## live preview (light theme) in the browser while editing: make diagrams-watch NAME=context
	@# NAME only from the command line: WSL exports NAME (the hostname) into every shell.
	@[ "$(origin NAME)" = "command line" ] && [ -f "docs/diagrams/$(NAME).d2" ] || { \
		echo "usage: make diagrams-watch NAME=<one of: $(basename $(notdir $(DIAGRAMS)))>" >&2; exit 1; }
	$(D2_RUN) --watch --browser "$${BROWSER:-0}" docs/diagrams/$(NAME).d2 docs/diagrams/$(NAME).light.svg
