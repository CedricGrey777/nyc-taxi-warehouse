"""Explicitly synthetic, deterministic tiny files; not observed TLC statistics."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from .pipeline import MONTHS, dump_json


def create_fixtures(output: str | Path) -> dict:
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    zones = root / "zones.csv"
    with zones.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        writer.writerows([(1, "Manhattan", "Test North", "Yellow Zone"), (2, "Queens", "Test East", "Boro Zone")])
    files = {}
    with duckdb.connect() as connection:
        connection.execute("""CREATE TABLE fixture (
            VendorID INTEGER, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
            PULocationID INTEGER, DOLocationID INTEGER, trip_distance DOUBLE, total_amount DOUBLE, payment_type INTEGER)""")
        for month in MONTHS:
            year, month_number = map(int, month.split("-"))
            stamp = datetime(year, month_number, 15, 12)
            base = (1, stamp, stamp + timedelta(minutes=10), 1, 2, 2.0, 12.50, 1)
            leap = datetime(2024, 2, 29, 18) if month == "2024-02" else stamp + timedelta(days=1)
            rows = [
                base, base,  # Identical public observations are intentionally retained.
                (1, leap, leap + timedelta(minutes=5), 999, None, 0.0, 0.0, 2),
                (1, None, stamp, 1, 2, 1.0, 5.0, 1),
                (1, stamp, stamp + timedelta(minutes=1), 1, 2, -1.0, -3.0, 1),
                (1, datetime(2023, 12, 31, 12), datetime(2023, 12, 31, 13), 1, 2, 1.0, 7.0, 1),
                (1, stamp, stamp - timedelta(minutes=1), 1, 2, 1.0, 4.0, 1),
                (1, stamp, stamp + timedelta(minutes=1), 1, 2, float("nan"), 2.0, 1),
                (1, stamp, stamp + timedelta(minutes=1), 1, 2, 1.0, None, 1),
            ]
            connection.execute("DELETE FROM fixture")
            connection.executemany("INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
            path = root / f"yellow_tripdata_{month}.parquet"
            connection.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(path)])
            files[month] = str(path)
    result = {"kind": "synthetic_fixture", "source_files": files, "zones": str(zones),
              "expected_per_month": {"raw": 9, "accepted": 3, "quarantine": 6, "amount": "25.00"}}
    dump_json(root / "fixture_manifest.json", result)
    return result

