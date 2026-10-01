# Common commands. Run `make setup` once, then the other targets use the .venv it creates.
# Override the Python used to create the venv with e.g. `make setup BOOTSTRAP_PYTHON=python3.11`.

ifeq ($(OS),Windows_NT)
    BOOTSTRAP_PYTHON ?= py -3.11
    PYTHON := .venv/Scripts/python.exe
else
    BOOTSTRAP_PYTHON ?= python3
    PYTHON := .venv/bin/python
endif

.PHONY: setup download run benchmark test lint format

setup:  ## Create the virtualenv and install dependencies
	$(BOOTSTRAP_PYTHON) -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

download:  ## Fetch 12 months of 2024 yellow taxi data + zone lookup into data/raw/
	$(PYTHON) -m scripts.download_data

run:  ## Run the pipeline end to end
	$(PYTHON) -m src.pipeline

benchmark:  ## Run the performance experiments
	$(PYTHON) -m src.benchmark

test:  ## Run unit tests (tiny in-memory data, no downloads needed)
	$(PYTHON) -m pytest

lint:  ## Check code style and common bugs
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:  ## Auto-fix formatting and fixable lint issues
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .
