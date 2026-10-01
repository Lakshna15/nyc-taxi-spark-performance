import pytest

from src.spark_session import build_spark


@pytest.fixture(scope="session")
def spark():
    """One SparkSession shared by all tests: starting the JVM takes seconds, so do it once."""
    spark = build_spark(
        app_name="tests",
        master="local[2]",
        extra_conf={
            # Test data is a handful of rows; the default of 200 shuffle partitions would
            # create 200 near-empty tasks for every groupBy/join and slow tests down.
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
        },
    )
    yield spark
    spark.stop()
