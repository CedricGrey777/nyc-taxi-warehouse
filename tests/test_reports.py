import json
from decimal import Decimal

import duckdb
import pytest

from taxi_warehouse.warehouse import DDL
from tools.report import main, read_reports, render_markdown


@pytest.fixture
def report_database(tmp_path):
    with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as connection:
        connection.execute(DDL)
        connection.execute("INSERT INTO dim_zone VALUES (1, 'A', 'North', 'Test'), (2, 'B', 'South', 'Test')")
        connection.execute("""INSERT INTO dim_date (date_key, calendar_date, is_weekend)
            VALUES (20240101, '2024-01-01', false), (20240102, '2024-01-02', false), (20240106, '2024-01-06', true)""")
        connection.execute("""INSERT INTO mart_daily_pickup_zone VALUES
            ('2024-01', '2024-01-01', 1, 1, 10.00, 1.0),
            ('2024-01', '2024-01-01', 2, 9, 90.00, 90.0),
            ('2024-01', '2024-01-02', 1, 10, 100.00, 10.0),
            ('2024-01', '2024-01-06', 1, 2, 20.00, 8.0)""")
        connection.execute("INSERT INTO pipeline_runs (run_id, source_month) VALUES ('report-synthetic', '2024-01')")
        connection.execute("INSERT INTO published_months VALUES ('2024-01', 'report-synthetic')")
    return tmp_path


def test_analysis_queries_use_record_weighted_distance(report_database):
    reports = read_reports(report_database)["reports"]
    daily = reports["daily_trends"]
    assert len(daily) == 3
    assert daily[0]["trip_record_count"] == 10
    assert daily[0]["avg_trip_distance_miles"] == pytest.approx(9.1)
    assert sum(row["recorded_charge_total_usd"] for row in daily) == Decimal("220.00")
    zones = reports["top_pickup_zones"]
    assert [row["pu_zone_key"] for row in zones] == [1, 2]
    assert zones[0]["avg_trip_distance_miles"] == pytest.approx(19 / 13)
    weekday, weekend = reports["weekday_weekend"]
    assert weekday["day_type"] == "weekday"
    assert weekday["observed_days"] == 2
    assert weekday["avg_daily_record_count"] == 10
    assert weekday["avg_trip_distance_miles"] == pytest.approx(101 / 20)
    assert weekend["day_type"] == "weekend"
    assert weekend["avg_trip_distance_miles"] == 4


def test_report_cli_exports_aggregates_while_database_is_open_read_only(report_database, capsys):
    # DuckDB rejects a second connection with different read_only configuration.
    with duckdb.connect(str(report_database / "warehouse.duckdb"), read_only=True) as reader:
        assert main(["--data-root", str(report_database), "--format", "json"]) == 0
        report = json.loads(capsys.readouterr().out)
        assert set(report["reports"]) == {"daily_trends", "top_pickup_zones", "weekday_weekend"}
        assert report["published_months"][0]["run_id"] == "report-synthetic"
        assert reader.execute("SELECT sum(trip_record_count) FROM mart_daily_pickup_zone").fetchone()[0] == 22
    markdown = render_markdown(report)
    assert "## 日趋势" in markdown
    assert "## 上车地区 Top 10" in markdown
    assert "## 工作日与周末" in markdown
    assert "不是企业净收入" in markdown
