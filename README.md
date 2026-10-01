# nyc-taxi-spark-performance

Processes a year of NYC TLC Yellow Taxi trip data with PySpark and measures how common
Spark optimizations affect performance.

> Work in progress. The full write-up, architecture diagram, and benchmark results come
> in a later phase. All numbers will come from real runs.

## Requirements

- Python 3.10–3.12 (PySpark 3.5 does not support newer versions)
- Java 17
- GNU make

## Quick start

```bash
make setup      # create .venv and install dependencies
make download   # ~650 MB of Parquet into data/raw/ (already-downloaded files are skipped)
make run        # Phase 1: print the row count of one month
make test
```
