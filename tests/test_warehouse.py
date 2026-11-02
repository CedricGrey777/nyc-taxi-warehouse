"""Reconciliation regressions against a real in-memory DuckDB, without Spark."""

import duckdb
import pytest

from taxi_warehouse.warehouse import DDL, _assert_month


@pytest.fixture
def warehouse():
    with duckdb.connect() as connection:
        connection.execute(DDL)
        connection.execute("INSERT INTO dim_zone VALUES (1, 'A', 'North', 'Test'), (2, 'B', 'South', 'Test'), (3, 'C', 'East', 'Test')")
        connection.execute("INSERT INTO dim_date (date_key, calendar_date) VALUES (20240101, DATE '2024-01-01'), (20240102, DATE '2024-01-02')")
        connection.execute("""INSERT INTO fact_trip_record
            (source_month, pickup_date, date_key, pu_zone_key, do_zone_key, trip_distance, total_amount)
            VALUES ('2024-01', '2024-01-01', 20240101, 1, 2, 1.0, 10.00),
                   ('2024-01', '2024-01-01', 20240101, 1, 2, 2.0, 15.00),
                   ('2024-01', '2024-01-02', 20240102, 2, 1, 1.0, 5.00)""")
        connection.execute("""INSERT INTO mart_daily_pickup_zone
            SELECT source_month, pickup_date, pu_zone_key, count(*), sum(total_amount), sum(trip_distance)
            FROM fact_trip_record GROUP BY source_month, pickup_date, pu_zone_key""")
        yield connection


def test_matching_daily_groups_pass(warehouse):
    assert _assert_month(warehouse, "2024-01", 3, "30.00")["passed"] is True


@pytest.mark.parametrize("change", [
    "UPDATE mart_daily_pickup_zone SET pickup_date = DATE '2024-01-03' WHERE pu_zone_key = 1",
    "UPDATE mart_daily_pickup_zone SET pu_zone_key = 3 WHERE pu_zone_key = 1",
    "UPDATE mart_daily_pickup_zone SET trip_record_count = trip_record_count + CASE WHEN pu_zone_key = 1 THEN 1 ELSE -1 END",
    "UPDATE mart_daily_pickup_zone SET recorded_charge_total = recorded_charge_total + CASE WHEN pu_zone_key = 1 THEN 1 ELSE -1 END",
])
def test_wrong_group_distribution_fails_despite_matching_month_totals(warehouse, change):
    warehouse.execute(change)
    with pytest.raises(ValueError, match="Daily pickup-zone reconciliation"):
        _assert_month(warehouse, "2024-01", 3, "30.00")


@pytest.mark.parametrize("change", [
    """DELETE FROM mart_daily_pickup_zone WHERE pu_zone_key = 1;
       UPDATE mart_daily_pickup_zone SET trip_record_count = 3, recorded_charge_total = 30, distance_total_miles = 4""",
    "INSERT INTO mart_daily_pickup_zone VALUES ('2024-01', DATE '2024-01-03', 3, 0, 0, 0)",
])
def test_missing_or_extra_group_fails_despite_matching_month_totals(warehouse, change):
    warehouse.execute(change)
    with pytest.raises(ValueError, match="Daily pickup-zone reconciliation"):
        _assert_month(warehouse, "2024-01", 3, "30.00")


@pytest.mark.parametrize("expected_distance, drift, should_pass", [
    (0.0, 1e-8, True),
    (0.0, 2e-8, False),
    (1_000_000.0, 0.001, True),
    (1_000_000.0, 0.002, False),
])
def test_distance_uses_absolute_and_relative_tolerance(warehouse, expected_distance, drift, should_pass):
    warehouse.execute("UPDATE fact_trip_record SET trip_distance = ? WHERE pu_zone_key = 1", [expected_distance / 2])
    warehouse.execute("UPDATE mart_daily_pickup_zone SET distance_total_miles = ? WHERE pu_zone_key = 1", [expected_distance + drift])
    if should_pass:
        assert _assert_month(warehouse, "2024-01", 3, "30.00")["passed"] is True
    else:
        with pytest.raises(ValueError, match="Daily pickup-zone reconciliation"):
            _assert_month(warehouse, "2024-01", 3, "30.00")


@pytest.mark.parametrize("table, column", [
    ("fact_trip_record", "trip_distance"),
    ("mart_daily_pickup_zone", "distance_total_miles"),
])
@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), float("-inf")])
def test_missing_or_nonfinite_distance_is_rejected(warehouse, table, column, value):
    warehouse.execute(f"UPDATE {table} SET {column} = ? WHERE pu_zone_key = 1", [value])
    with pytest.raises(ValueError, match="missing or non-finite distance"):
        _assert_month(warehouse, "2024-01", 3, "30.00")
