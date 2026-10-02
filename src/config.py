"""Central place for paths and dataset settings, so other modules don't hard-code them."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Allow the data directory to be moved (e.g. to a bigger disk) without code changes.
DATA_DIR = Path(os.environ.get("NYC_TAXI_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
OUTPUT_DIR = DATA_DIR / "output"
# Copies of the data in other layouts/formats, created by the benchmark experiments
BENCH_DIR = DATA_DIR / "bench"

RESULTS_DIR = PROJECT_ROOT / "results"
RESULTS_FILE = RESULTS_DIR / "benchmark_results.csv"
# Query plans and Spark metrics captured by the experiments
PLANS_DIR = PROJECT_ROOT / "docs" / "plans"

# 2024 has a stable schema all year; 2025 adds a congestion-fee column mid-stream.
YEAR = 2024

ZONE_LOOKUP_FILE = RAW_DIR / "taxi_zone_lookup.csv"


def trip_file(year: int, month: int) -> Path:
    """Local path of one month of trips, e.g. data/raw/yellow_tripdata_2024-01.parquet."""
    return RAW_DIR / f"yellow_tripdata_{year}-{month:02d}.parquet"
