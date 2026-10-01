"""Read raw input files into Spark DataFrames."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src import config

# The zone lookup is tiny, so we spell out its schema instead of asking Spark to guess.
ZONE_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), nullable=False),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),
        StructField("service_zone", StringType()),
    ]
)


def _require(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run `make download` first.")
    # Forward slashes work for Spark on both Windows and Linux.
    return path.as_posix()


def read_trips_month(spark: SparkSession, year: int, month: int) -> DataFrame:
    """Read one month of yellow taxi trips. Parquet files carry their own schema."""
    return spark.read.parquet(_require(config.trip_file(year, month)))


def read_zones(spark: SparkSession) -> DataFrame:
    """Read the taxi zone lookup (LocationID -> Borough, Zone).

    An explicit schema avoids inferSchema, which costs an extra pass over the file, and
    guarantees LocationID is an integer so it joins cleanly with the trip table.
    """
    return spark.read.csv(_require(config.ZONE_LOOKUP_FILE), header=True, schema=ZONE_SCHEMA)
