import json
from pathlib import Path

import duckdb
import pytest

from taxi_warehouse.fixtures import create_fixtures
from taxi_warehouse.pipeline import CommitAcknowledgementLost, dump_json, prepare_run, validate_run
from taxi_warehouse.transform import spark_session, transform
from taxi_warehouse.warehouse import check_month, publish

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    session = spark_session("NYCTaxiIntegrationTests")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def prepared(tmp_path_factory, spark):
    directory = tmp_path_factory.mktemp("taxi")
    fixture = create_fixtures(directory / "fixtures")
    root = directory / "warehouse"
    results = {}
    for month in ("2024-01", "2024-02", "2024-03"):
        context = prepare_run(month, root, fixture["source_files"][month], fixture["zones"])
        report = transform(context, spark=spark)
        dump_json(Path(context["run_dir"]) / "quality.json", report)
        context = validate_run(context)
        report = json.loads((Path(context["run_dir"]) / "quality.json").read_text())
        results[month] = (context, report)
    return root, fixture, results


def test_synthetic_quality_semantics(prepared):
    _, _, results = prepared
    for context, report in results.values():
        assert report["raw_count"] == 9
        assert report["accepted_count"] == 3
        assert report["quarantine_count"] == 6
        assert report["accepted_amount_total"] == "25.00"
        assert report["identical_source_extra_rows"] == 1
        assert report["unknown_pickup_count"] == 1
        assert report["unknown_dropoff_count"] == 1
        assert report["jvm_peak_rss_kib"] > 0
        assert sum(report["quarantine_reasons"].values()) == 6
        assert report["quarantine_reasons"]["negative_distance"] == 1
        assert "negative_amount" not in report["quarantine_reasons"]  # secondary reason, not another row
        with duckdb.connect() as connection:
            files = [str(p) for p in (Path(context["run_dir"]) / "quarantine").glob("*.parquet")]
            reasons = connection.execute("SELECT all_reasons FROM read_parquet(?) WHERE primary_reason='negative_distance'", [files]).fetchone()[0]
            assert reasons == "negative_distance,negative_amount"


def test_publish_backfill_rerun_and_rollback(prepared):
    root, _, results = prepared
    for month in ("2024-01", "2024-03"):
        publish(*results[month])
    january = check_month(root, "2024-01")
    march = check_month(root, "2024-03")
    publish(*results["2024-02"])
    assert check_month(root, "2024-01") == january
    assert check_month(root, "2024-03") == march
    publish(*results["2024-02"])
    assert check_month(root, "2024-02")["record_count"] == 3
    for fail_at in ("after_delete", "before_commit"):
        with pytest.raises(RuntimeError, match="Injected"):
            publish(*results["2024-02"], fail_at=fail_at)
        assert check_month(root, "2024-02")["record_count"] == 3
        assert check_month(root, "2024-01") == january
    with pytest.raises(CommitAcknowledgementLost, match="successful commit"):
        publish(*results["2024-02"], fail_at="after_commit_response")
    assert check_month(root, "2024-02")["run_id"] == results["2024-02"][0]["run_id"]
    publish(*results["2024-02"])
    assert check_month(root, "2024-02")["record_count"] == 3
    with duckdb.connect(str(root / "warehouse.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM fact_trip_record").fetchone()[0] == 9
        assert connection.execute("SELECT count(*) FROM dim_date").fetchone()[0] == 91
        assert connection.execute("SELECT count(*) FROM fact_trip_record WHERE pickup_date=DATE '2024-02-29'").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM fact_trip_record WHERE source_month='2024-01' AND total_amount=12.5").fetchone()[0] == 2


def test_duplicate_dimension_blocks_transform(tmp_path, prepared, spark):
    _, fixture, _ = prepared
    bad_zones = tmp_path / "zones.csv"
    text = Path(fixture["zones"]).read_text()
    bad_zones.write_text(text + "1,Manhattan,Duplicate,Yellow Zone\n")
    context = prepare_run("2024-01", tmp_path / "warehouse", fixture["source_files"]["2024-01"], str(bad_zones))
    with pytest.raises(ValueError, match="Duplicate zone"):
        transform(context, spark=spark)


def test_missing_column_blocks_transform(tmp_path, prepared, spark):
    _, fixture, _ = prepared
    bad_source = tmp_path / "missing.parquet"
    with duckdb.connect() as connection:
        connection.execute("COPY (SELECT 1 AS wrong_column) TO ? (FORMAT PARQUET)", [str(bad_source)])
    context = prepare_run("2024-01", tmp_path / "warehouse", str(bad_source), fixture["zones"])
    with pytest.raises(ValueError, match="Missing source columns"):
        transform(context, spark=spark)
