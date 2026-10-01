"""Build a SparkSession configured for local-mode runs of this project."""

import os
import sys

from pyspark.sql import SparkSession


def build_spark(
    app_name: str = "nyc-taxi-spark-performance",
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create (or reuse) a SparkSession.

    master defaults to local[*]: run Spark inside this one process, using every CPU core.
    extra_conf lets callers (tests, benchmarks) override settings without editing this file.

    Note: getOrCreate() returns the already-running session if there is one, and some
    settings (like driver memory) only take effect when the JVM first starts. Call
    spark.stop() before building a session with different startup settings.
    """
    # Spark starts separate Python worker processes for some operations. Point them at the
    # interpreter running this code; otherwise, on Windows, Spark looks for "python3",
    # which usually doesn't exist and causes "Python worker failed to connect back".
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or os.environ.get("SPARK_MASTER", "local[*]"))
        # In local mode the driver *is* the whole cluster, so this is all the memory Spark
        # gets. The 1g default is too small for a year of taxi data.
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        # Pin the session time zone so hour/day calculations don't depend on the
        # machine's local time zone setting (e.g. CI runners use UTC).
        .config("spark.sql.session.timeZone", "UTC")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    # Spark's INFO logging is extremely chatty; WARN keeps the console readable.
    spark.sparkContext.setLogLevel("WARN")
    return spark
