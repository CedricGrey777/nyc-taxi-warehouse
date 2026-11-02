"""One DuckDB transaction publishes a month, its marts, and its version pointer."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import duckdb

from .pipeline import CommitAcknowledgementLost, validate_month
from .transform import FACT_COLUMNS

# Distances are floating-point miles; counts and monetary amounts remain exact.
DISTANCE_ABS_TOLERANCE = 1e-8
DISTANCE_REL_TOLERANCE = 1e-9

DDL = """
CREATE TABLE IF NOT EXISTS dim_zone (
    zone_key INTEGER PRIMARY KEY, borough VARCHAR, zone VARCHAR, service_zone VARCHAR
);
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, calendar_date DATE UNIQUE, year INTEGER,
    month INTEGER, iso_weekday INTEGER, is_weekend BOOLEAN
);
CREATE TABLE IF NOT EXISTS fact_trip_record (
    source_month VARCHAR, pickup_at TIMESTAMP, dropoff_at TIMESTAMP,
    pickup_date DATE, date_key INTEGER, vendor_id INTEGER, pu_zone_key INTEGER,
    do_zone_key INTEGER, pu_location_id_raw INTEGER, do_location_id_raw INTEGER,
    trip_distance DOUBLE, total_amount DECIMAL(18,2), payment_type INTEGER, run_id VARCHAR
);
CREATE TABLE IF NOT EXISTS mart_daily_pickup_zone (
    source_month VARCHAR, pickup_date DATE, pu_zone_key INTEGER,
    trip_record_count BIGINT, recorded_charge_total DECIMAL(38,2), distance_total_miles DOUBLE,
    PRIMARY KEY (source_month, pickup_date, pu_zone_key)
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY, source_month VARCHAR, source_sha256 VARCHAR,
    zones_sha256 VARCHAR, accepted_path VARCHAR, quality_json VARCHAR,
    published_at TIMESTAMPTZ DEFAULT current_timestamp
);
CREATE TABLE IF NOT EXISTS published_months (
    source_month VARCHAR PRIMARY KEY, run_id VARCHAR
);
CREATE OR REPLACE VIEW daily_pickup_metrics AS
SELECT m.source_month, m.pickup_date, m.pu_zone_key, z.borough, z.zone,
       m.trip_record_count, m.recorded_charge_total,
       m.distance_total_miles / m.trip_record_count AS avg_trip_distance_miles
FROM mart_daily_pickup_zone m JOIN dim_zone z ON m.pu_zone_key = z.zone_key;
"""


def _files(directory: Path) -> list[str]:
    files = sorted(str(p) for p in directory.rglob("*.parquet"))
    if not files:
        raise ValueError(f"No Parquet output in {directory.name}")
    return files


def _assert_month(connection, month: str, expected_count: int, expected_amount: str) -> dict:
    fact = connection.execute(
        "SELECT count(*), coalesce(sum(total_amount), 0) FROM fact_trip_record WHERE source_month = ?",
        [month],
    ).fetchone()
    mart = connection.execute(
        "SELECT coalesce(sum(trip_record_count), 0), coalesce(sum(recorded_charge_total), 0) "
        "FROM mart_daily_pickup_zone WHERE source_month = ?", [month],
    ).fetchone()
    if fact != mart or fact != (expected_count, Decimal(expected_amount)):
        raise ValueError(f"Fact/mart/quality reconciliation failed for {month}")
    invalid_distance = connection.execute(
        """SELECT
        (SELECT count(*) FROM fact_trip_record
         WHERE source_month = ? AND (trip_distance IS NULL OR NOT isfinite(trip_distance))) +
        (SELECT count(*) FROM mart_daily_pickup_zone
         WHERE source_month = ? AND (distance_total_miles IS NULL OR NOT isfinite(distance_total_miles)))""",
        [month, month],
    ).fetchone()[0]
    if invalid_distance:
        raise ValueError(f"Fact/mart contains a missing or non-finite distance for {month}")
    mismatched_groups = connection.execute(
        """WITH fact_groups AS (
            SELECT pickup_date, pu_zone_key, count(*) AS record_count,
                   sum(total_amount) AS charge_total, sum(trip_distance) AS distance_total
            FROM fact_trip_record WHERE source_month = ?
            GROUP BY pickup_date, pu_zone_key
        ), mart_groups AS (
            SELECT pickup_date, pu_zone_key, trip_record_count AS record_count,
                   recorded_charge_total AS charge_total, distance_total_miles AS distance_total
            FROM mart_daily_pickup_zone WHERE source_month = ?
        )
        SELECT count(*) FROM fact_groups f FULL OUTER JOIN mart_groups m
          ON f.pickup_date = m.pickup_date AND f.pu_zone_key = m.pu_zone_key
        WHERE f.record_count IS NULL OR m.record_count IS NULL
           OR f.record_count <> m.record_count
           OR f.charge_total IS DISTINCT FROM m.charge_total
           OR f.distance_total IS NULL OR m.distance_total IS NULL
           OR NOT isfinite(f.distance_total) OR NOT isfinite(m.distance_total)
           OR abs(f.distance_total - m.distance_total) >
              ? + ? * greatest(abs(f.distance_total), abs(m.distance_total))""",
        [month, month, DISTANCE_ABS_TOLERANCE, DISTANCE_REL_TOLERANCE],
    ).fetchone()[0]
    if mismatched_groups:
        raise ValueError(f"Daily pickup-zone reconciliation failed for {month}: {mismatched_groups} groups")
    orphaned = connection.execute(
        """SELECT count(*) FROM fact_trip_record f
        LEFT JOIN dim_zone p ON f.pu_zone_key = p.zone_key
        LEFT JOIN dim_zone d ON f.do_zone_key = d.zone_key
        LEFT JOIN dim_date c ON f.date_key = c.date_key
        WHERE f.source_month = ? AND (p.zone_key IS NULL OR d.zone_key IS NULL OR c.date_key IS NULL)""",
        [month],
    ).fetchone()[0]
    if orphaned:
        raise ValueError("Fact contains unresolved dimension references")
    return {"month": month, "record_count": fact[0], "recorded_charge_total": str(fact[1]), "passed": True}


def publish(context: dict, report: dict, fail_at: str | None = None) -> None:
    month = validate_month(context["month"])
    root = Path(context["data_root"])
    run_dir = Path(context["run_dir"])
    connection = duckdb.connect(str(root / "warehouse.duckdb"), config={"memory_limit": "1GB", "threads": 2})
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(DDL)
        connection.execute(
            "CREATE TEMP TABLE incoming_zones AS SELECT * FROM read_parquet(?)", [_files(run_dir / "zones")]
        )
        if connection.execute("SELECT count(*) FROM dim_zone").fetchone()[0]:
            difference = connection.execute(
                """SELECT count(*) FROM (
                (SELECT * FROM dim_zone EXCEPT ALL SELECT * FROM incoming_zones)
                UNION ALL
                (SELECT * FROM incoming_zones EXCEPT ALL SELECT * FROM dim_zone))"""
            ).fetchone()[0]
            if difference:
                raise ValueError("Zone snapshot changed; v1 uses one fixed reference snapshot for all months")
        else:
            connection.execute("INSERT INTO dim_zone SELECT * FROM incoming_zones")
        connection.execute(
            """INSERT INTO dim_date
            SELECT cast(strftime(d, '%Y%m%d') AS INTEGER), cast(d AS DATE), year(d), month(d),
                   isodow(d), isodow(d) IN (6, 7)
            FROM range(DATE '2024-01-01', DATE '2024-04-01', INTERVAL 1 DAY) t(d)
            ON CONFLICT DO NOTHING"""
        )
        connection.execute("DELETE FROM fact_trip_record WHERE source_month = ?", [month])
        if fail_at == "after_delete":
            raise RuntimeError("Injected failure after_delete")
        columns = ", ".join(FACT_COLUMNS)
        connection.execute(
            f"INSERT INTO fact_trip_record ({columns}) SELECT {columns} "
            "FROM read_parquet(?, hive_partitioning = true)", [_files(run_dir / "accepted")],
        )
        # Do not accept stale, foreign-month output under a run directory.
        wrong = connection.execute(
            "SELECT count(*) FROM fact_trip_record WHERE run_id = ? AND source_month <> ?",
            [context["run_id"], month],
        ).fetchone()[0]
        if wrong:
            raise ValueError("Prepared records do not belong to the requested month")
        connection.execute("DELETE FROM mart_daily_pickup_zone WHERE source_month = ?", [month])
        connection.execute(
            """INSERT INTO mart_daily_pickup_zone
            SELECT source_month, pickup_date, pu_zone_key, count(*), sum(total_amount), sum(trip_distance)
            FROM fact_trip_record WHERE source_month = ? GROUP BY source_month, pickup_date, pu_zone_key""",
            [month],
        )
        _assert_month(connection, month, report["accepted_count"], report["accepted_amount_total"])
        connection.execute(
            """INSERT INTO pipeline_runs
            (run_id, source_month, source_sha256, zones_sha256, accepted_path, quality_json)
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (run_id) DO NOTHING""",
            [context["run_id"], month, context["source"]["sha256"], context["zones"]["sha256"],
             str(run_dir / "accepted"), json.dumps(report)],
        )
        connection.execute(
            "INSERT INTO published_months VALUES (?, ?) ON CONFLICT (source_month) DO UPDATE SET run_id = excluded.run_id",
            [month, context["run_id"]],
        )
        if fail_at == "before_commit":
            raise RuntimeError("Injected failure before_commit")
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    # Deliberately outside the transaction try/except: COMMIT already succeeded.
    if fail_at == "after_commit_response":
        raise CommitAcknowledgementLost("Injected lost response after a successful commit; check the published month")


def check_month(data_root: str | Path, month: str) -> dict:
    validate_month(month)
    database = Path(data_root).resolve() / "warehouse.duckdb"
    if not database.is_file():
        raise ValueError("No warehouse has been published")
    with duckdb.connect(str(database), read_only=True) as connection:
        row = connection.execute(
            """SELECT r.run_id, r.quality_json FROM published_months p
            JOIN pipeline_runs r ON p.run_id = r.run_id WHERE p.source_month = ?""", [month],
        ).fetchone()
        if not row:
            raise ValueError(f"Month {month} has not been published")
        report = json.loads(row[1])
        result = _assert_month(connection, month, report["accepted_count"], report["accepted_amount_total"])
        return {**result, "run_id": row[0], "quarantine_count": report["quarantine_count"]}
