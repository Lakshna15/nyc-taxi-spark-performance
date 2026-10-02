# Common commands. Run `make setup` once, then the other targets use the .venv it creates.
# Override the Python used to create the venv with e.g. `make setup BOOTSTRAP_PYTHON=python3.11`.

ifeq ($(OS),Windows_NT)
    BOOTSTRAP_PYTHON ?= py -3.11
    PYTHON := .venv/Scripts/python.exe
else
    BOOTSTRAP_PYTHON ?= python3
    PYTHON := .venv/bin/python
endif

IMAGE := nyc-taxi-spark
# Data lives in a Docker volume rather than a folder shared with the host: Docker Desktop's
# host file sharing is slow enough to distort benchmark timings.
DATA_VOLUME := nyc-taxi-data
DOCKER_RUN := docker run --rm -i -p 4040:4040 -v "$(CURDIR):/app" -v $(DATA_VOLUME):/app/data $(IMAGE)

.PHONY: setup download run benchmark results-table test lint format \
	docker-build docker-download docker-run docker-benchmark docker-test docker-export

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

results-table:  ## Print the latest benchmark results as a Markdown table (for the README)
	$(PYTHON) -m scripts.results_table

test:  ## Run unit tests (tiny in-memory data, no downloads needed)
	$(PYTHON) -m pytest

lint:  ## Check code style and common bugs
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:  ## Auto-fix formatting and fixable lint issues
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

# --- Docker: same commands inside Linux (no Java/Python/winutils needed on the host) ---

docker-build:  ## Build the image (Python 3.11 + Java 17 + requirements)
	docker build -t $(IMAGE) .

docker-download:  ## Download the data into the Docker volume
	$(DOCKER_RUN) python -m scripts.download_data

docker-run:  ## Run the pipeline in Docker (Spark UI at http://localhost:4040 while running)
	$(DOCKER_RUN) python -m src.pipeline

docker-benchmark:  ## Run experiments in Docker, e.g. make docker-benchmark ARGS="file_format"
	$(DOCKER_RUN) python -m src.benchmark $(ARGS)

docker-test:  ## Run the unit tests in Docker
	$(DOCKER_RUN) python -m pytest

docker-export:  ## Copy the pipeline's output tables from the volume to ./data/output
	docker run --rm -v $(DATA_VOLUME):/vol -v "$(CURDIR)/data:/host" $(IMAGE) cp -r /vol/output /host/
