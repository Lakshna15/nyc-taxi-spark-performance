from datetime import datetime

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from src.clean import add_derived_columns, clean_trips, cleaning_report

YEAR = 2024

# Only the columns cleaning needs, with the same types as the real TLC files
# (the timestamps in the Parquet files are read as timestamp_ntz).
TRIP_SCHEMA = StructType(
    [
        StructField("trip_id", StringType()),  # test-only label so assertions can name rows
        StructField("tpep_pickup_datetime", TimestampNTZType()),
        StructField("tpep_dropoff_datetime", TimestampNTZType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
    ]
)


def trip(trip_id: str, **overrides) -> dict:
    """A valid trip (Friday 2024-03-15, 08:00-08:30). Override fields to break one rule."""
    row = {
        "trip_id": trip_id,
        "tpep_pickup_datetime": datetime(2024, 3, 15, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 3, 15, 8, 30),
        "PULocationID": 161,
        "DOLocationID": 236,
        "trip_distance": 3.2,
        "fare_amount": 20.0,
        "tip_amount": 4.0,
        "total_amount": 28.5,
    }
    row.update(overrides)
    return row


def make_df(spark, rows):
    return spark.createDataFrame(
        [tuple(r[field.name] for field in TRIP_SCHEMA.fields) for r in rows], TRIP_SCHEMA
    )


def kept_ids(spark, rows) -> list[str]:
    cleaned = clean_trips(make_df(spark, rows), YEAR)
    return sorted(r.trip_id for r in cleaned.select("trip_id").collect())


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"fare_amount": 0.0}, id="zero_fare"),
        pytest.param({"fare_amount": -5.0}, id="negative_fare"),
        pytest.param({"fare_amount": None}, id="null_fare"),
        pytest.param({"total_amount": 0.0}, id="zero_total"),
        pytest.param({"trip_distance": 0.0}, id="zero_distance"),
        pytest.param({"trip_distance": 150.0}, id="huge_distance"),
        pytest.param({"tpep_dropoff_datetime": datetime(2024, 3, 15, 7, 59)}, id="dropoff_first"),
        pytest.param({"tpep_dropoff_datetime": datetime(2024, 3, 15, 8, 0)}, id="zero_duration"),
        pytest.param({"tpep_dropoff_datetime": datetime(2024, 3, 15, 14, 1)}, id="over_6_hours"),
        pytest.param({"PULocationID": None}, id="null_pickup_location"),
        pytest.param({"DOLocationID": None}, id="null_dropoff_location"),
        pytest.param(
            {
                "tpep_pickup_datetime": datetime(2023, 12, 31, 23, 50),
                "tpep_dropoff_datetime": datetime(2024, 1, 1, 0, 10),
            },
            id="pickup_in_previous_year",
        ),
        pytest.param(
            {
                "tpep_pickup_datetime": datetime(2025, 1, 1, 0, 0),
                "tpep_dropoff_datetime": datetime(2025, 1, 1, 0, 20),
            },
            id="pickup_in_next_year",
        ),
    ],
)
def test_invalid_trip_is_removed(spark, overrides):
    assert kept_ids(spark, [trip("good"), trip("bad", **overrides)]) == ["good"]


def test_boundary_values_are_kept(spark):
    rows = [
        trip("exactly_6_hours", tpep_dropoff_datetime=datetime(2024, 3, 15, 14, 0)),
        trip("exactly_100_miles", trip_distance=100.0),
        trip(
            "first_minute_of_year",
            tpep_pickup_datetime=datetime(2024, 1, 1, 0, 0),
            tpep_dropoff_datetime=datetime(2024, 1, 1, 0, 15),
        ),
        # Pickup decides the year, so a trip that ends in the next year still counts.
        trip(
            "ends_in_next_year",
            tpep_pickup_datetime=datetime(2024, 12, 31, 23, 55),
            tpep_dropoff_datetime=datetime(2025, 1, 1, 0, 10),
        ),
    ]
    assert kept_ids(spark, rows) == sorted(r["trip_id"] for r in rows)


def test_report_charges_each_row_to_the_first_rule_it_fails(spark):
    rows = [
        trip("good_1"),
        trip("good_2"),
        trip("bad_fare", fare_amount=0.0),
        # Fails both fare and distance: counted once, under the fare rule (it comes first).
        trip("bad_fare_and_distance", fare_amount=0.0, trip_distance=0.0),
        trip("bad_distance", trip_distance=500.0),
        trip("null_location", PULocationID=None),
        trip(
            "wrong_year",
            tpep_pickup_datetime=datetime(2023, 6, 1, 9, 0),
            tpep_dropoff_datetime=datetime(2023, 6, 1, 9, 20),
        ),
    ]
    df = make_df(spark, rows)

    report = cleaning_report(df, YEAR)

    assert report == {
        "input_rows": 7,
        "positive_fare_and_total": 2,
        "realistic_distance": 1,
        "dropoff_after_pickup": 0,
        "duration_at_most_6h": 0,
        "location_ids_present": 1,
        "pickup_in_year": 1,
        "output_rows": 2,
    }
    # The report must agree with what clean_trips actually keeps.
    assert clean_trips(df, YEAR).count() == report["output_rows"]


def test_report_on_empty_dataframe(spark):
    report = cleaning_report(make_df(spark, []), YEAR)
    assert report["input_rows"] == 0
    assert report["output_rows"] == 0


def test_derived_columns(spark):
    row = add_derived_columns(make_df(spark, [trip("t")])).first()

    assert row.trip_duration_minutes == 30.0
    assert row.pickup_hour == 8
    assert row.pickup_day_of_week == 6  # Spark: 1 = Sunday, so Friday = 6
    assert row.tip_percentage == pytest.approx(20.0)  # $4 tip on a $20 fare


def test_duration_across_midnight(spark):
    overnight = trip(
        "overnight",
        tpep_pickup_datetime=datetime(2024, 3, 15, 23, 50),
        tpep_dropoff_datetime=datetime(2024, 3, 16, 0, 20),
    )
    row = add_derived_columns(make_df(spark, [overnight])).first()

    assert row.trip_duration_minutes == 30.0
    assert row.pickup_hour == 23  # hour comes from pickup, not dropoff
