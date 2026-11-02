import json

from taxi_warehouse.benchmark import _write_benchmark_report


def test_repeated_experiments_keep_distinct_files_for_the_same_publication(tmp_path):
    publication = {"month": "2024-01", "run_id": "same-publication"}
    first = _write_benchmark_report(tmp_path, {**publication, "median_seconds": {"merge": 1.0}})
    first_path = next((tmp_path / "benchmarks").glob("*.json"))
    first_bytes = first_path.read_bytes()
    second = _write_benchmark_report(tmp_path, {**publication, "median_seconds": {"merge": 2.0}})

    assert first["experiment_id"] != second["experiment_id"]
    assert first["run_id"] == second["run_id"] == "same-publication"
    assert first_path.read_bytes() == first_bytes
    files = list((tmp_path / "benchmarks").glob("*.json"))
    assert len(files) == 2
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in files]
    assert {report["experiment_id"] for report in reports} == {first["experiment_id"], second["experiment_id"]}
    assert {report["median_seconds"]["merge"] for report in reports} == {1.0, 2.0}
