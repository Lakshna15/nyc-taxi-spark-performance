"""Run the pipeline end to end.

Phase 1: only the ingest step exists, so this proves Spark can start and read one month.
Later phases add cleaning, transformations, and writing outputs.
"""

import argparse
import time

from src import config
from src.ingest import read_trips_month, read_zones
from src.spark_session import build_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test: count one month of trips.")
    parser.add_argument("--month", type=int, default=1, choices=range(1, 13))
    args = parser.parse_args()

    spark = build_spark()
    try:
        print(f"Spark {spark.version} running with master={spark.sparkContext.master}")

        # Nothing is read yet: Spark only records *how* to read the file (lazy evaluation).
        trips = read_trips_month(spark, config.YEAR, args.month)
        trips.printSchema()  # schema comes from the Parquet footer, no full scan needed

        # count() is an "action", so this is the moment Spark actually does work.
        start = time.perf_counter()
        n_trips = trips.count()
        elapsed = time.perf_counter() - start

        print(f"Trips in {config.YEAR}-{args.month:02d}: {n_trips:,} (counted in {elapsed:.1f}s)")
        print(f"Partitions Spark split the file into: {trips.rdd.getNumPartitions()}")
        print(f"Taxi zones in lookup table: {read_zones(spark).count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
