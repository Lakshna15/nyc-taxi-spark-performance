from datetime import datetime

import pytest

from src.clean import add_derived_columns
from src.transform import (
    build_output_tables,
    join_zones,
    revenue_by_zone_month,
    tip_by_payment_type,
    top_zone_pairs,
    trips_by_hour_and_weekday,
)
from tests.factories import make_trips, make_zones, trip


def enriched(spark, rows):
    """Run tiny trips through the same steps the pipeline uses before aggregating."""
    return join_zones(add_derived_columns(make_trips(spark, rows)), make_zones(spark))


def test_join_zones_names_both_ends_of_the_trip(spark):
    row = enriched(spark, [trip("t", PULocationID=132, DOLocationID=236)]).first()

    assert (row.pickup_borough, row.pickup_zone) == ("Queens", "JFK Airport")
    assert (row.dropoff_borough, row.dropoff_zone) == ("Manhattan", "Upper East Side North")


def test_join_zones_keeps_trips_with_unknown_location(spark):
    df = enriched(spark, [trip("known"), trip("unknown", PULocationID=999)])

    assert df.count() == 2  # left join: no trip is lost
    unknown = df.filter("trip_id = 'unknown'").first()
    assert unknown.pickup_zone is None
    assert unknown.dropoff_zone == "Upper East Side North"


def test_revenue_by_zone_month(spark):
    rows = [
        trip("a", total_amount=10.0),
        trip("b", total_amount=15.5),  # same zone, same month as "a"
        trip(
            "c",
            total_amount=20.0,
            tpep_pickup_datetime=datetime(2024, 4, 2, 9, 0),
            tpep_dropoff_datetime=datetime(2024, 4, 2, 9, 20),
        ),
        trip("d", total_amount=40.0, PULocationID=132),
    ]
    result = {
        (r.pickup_month, r.pickup_zone): (r.revenue, r.trip_count)
        for r in revenue_by_zone_month(enriched(spark, rows)).collect()
    }

    assert result == {
        ("2024-03", "Midtown Center"): (25.5, 2),
        ("2024-04", "Midtown Center"): (20.0, 1),
        ("2024-03", "JFK Airport"): (40.0, 1),
    }


def test_trips_by_hour_and_weekday(spark):
    friday_8am_10min = trip(
        "a",
        tpep_pickup_datetime=datetime(2024, 3, 15, 8, 0),
        tpep_dropoff_datetime=datetime(2024, 3, 15, 8, 10),
    )
    friday_8am_30min = trip(
        "b",
        tpep_pickup_datetime=datetime(2024, 3, 15, 8, 45),
        tpep_dropoff_datetime=datetime(2024, 3, 15, 9, 15),
    )
    sunday_11pm = trip(
        "c",
        tpep_pickup_datetime=datetime(2024, 3, 17, 23, 0),
        tpep_dropoff_datetime=datetime(2024, 3, 17, 23, 15),
    )
    rows = trips_by_hour_and_weekday(
        enriched(spark, [friday_8am_10min, friday_8am_30min, sunday_11pm])
    ).collect()

    # Ordered by day of week (Sunday = 1 first), then hour
    assert [tuple(r) for r in rows] == [
        (1, "Sun", 23, 1, 15.0),
        (6, "Fri", 8, 2, 20.0),
    ]


def test_tip_by_payment_type(spark):
    rows = [
        trip("card_1", payment_type=1, fare_amount=20.0, tip_amount=4.0),  # 20%
        trip("card_2", payment_type=1, fare_amount=10.0, tip_amount=3.0),  # 30%
        trip("card_3", payment_type=1, fare_amount=10.0, tip_amount=1.0),  # 10%
        trip("cash", payment_type=2, fare_amount=15.0, tip_amount=0.0),  # cash tips not recorded
    ]
    result = {r.payment_type_name: r for r in tip_by_payment_type(enriched(spark, rows)).collect()}

    assert result["Credit card"].trip_count == 3
    assert result["Credit card"].avg_tip_percentage == pytest.approx(20.0)
    assert result["Credit card"].median_tip_percentage == pytest.approx(20.0)
    assert result["Cash"].avg_tip_percentage == 0.0


def test_top_zone_pairs_orders_by_count_and_limits(spark):
    rows = (
        [trip(f"mid_to_ues_{i}", PULocationID=161, DOLocationID=236) for i in range(3)]
        + [trip(f"jfk_to_mid_{i}", PULocationID=132, DOLocationID=161) for i in range(2)]
        + [trip("ues_to_ues", PULocationID=236, DOLocationID=237)]
    )
    result = top_zone_pairs(enriched(spark, rows), n=2).collect()

    assert [(r.pickup_zone, r.dropoff_zone, r.trip_count) for r in result] == [
        ("Midtown Center", "Upper East Side North", 3),
        ("JFK Airport", "Midtown Center", 2),
    ]


def test_build_output_tables_has_all_four_outputs(spark):
    tables = build_output_tables(enriched(spark, [trip("t")]))

    assert set(tables) == {
        "revenue_by_zone_month",
        "trips_by_hour_and_weekday",
        "tip_by_payment_type",
        "top_zone_pairs",
    }
    assert all(df.count() == 1 for df in tables.values())
