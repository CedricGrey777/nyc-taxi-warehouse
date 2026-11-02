"""Source snapshots and the four stages shared by CLI and Airflow."""

from __future__ import annotations

import hashlib
import json
import shutil
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

MONTHS = ("2024-01", "2024-02", "2024-03")
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


class CommitAcknowledgementLost(RuntimeError):
    """The database committed, but the caller did not receive a success response."""


def validate_month(month: str) -> str:
    if month not in MONTHS:
        raise ValueError("This dataset contract only supports 2024-01, 2024-02, 2024-03")
    return month


def month_range(start: str, end: str) -> list[str]:
    validate_month(start)
    validate_month(end)
    if start > end:
        raise ValueError("--from must not be after --to")
    return list(MONTHS[MONTHS.index(start) : MONTHS.index(end) + 1])


def dump_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _snapshot(root: Path, local: str | None, url: str, suffix: str) -> dict:
    """Keep immutable input versions; hashes identify files, never real trips."""
    root.mkdir(parents=True, exist_ok=True)
    if local:
        source = Path(local).resolve(strict=True)
        source_kind = "local_file"
        origin = str(source)
    else:
        source = root / ("download" + suffix)
        source_kind = "public_tlc"
        origin = url
        if not source.exists():
            temporary = root / ("download" + suffix + ".part")
            request = urllib.request.Request(url, headers={"User-Agent": "NYCTaxiWarehouse/0.1"})
            with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as out:
                shutil.copyfileobj(response, out)
            if suffix == ".parquet":
                import duckdb

                with duckdb.connect() as connection:
                    connection.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(temporary)])
            temporary.replace(source)
    checksum = digest(source)
    destination = root / (checksum + suffix)
    if not destination.exists():
        shutil.copyfile(source, destination)
    # Detect a corrupted local snapshot rather than silently consuming another version.
    if digest(destination) != checksum:
        raise ValueError("Input snapshot content does not match its source digest")
    return {
        "path": str(destination),
        "origin": origin,
        "kind": source_kind,
        "sha256": checksum,
        "bytes": destination.stat().st_size,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }


def prepare_run(
    month: str,
    data_root: str | Path,
    source: str | None = None,
    zones: str | None = None,
) -> dict:
    validate_month(month)
    root = Path(data_root).resolve()
    run_id = uuid.uuid4().hex
    run_dir = root / "runs" / month / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    context = {
        "run_id": run_id,
        "month": month,
        "data_root": str(root),
        "run_dir": str(run_dir),
        "status": "preparing",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    dump_json(run_dir / "run.json", context)
    try:
        context["source"] = _snapshot(
            root / "raw" / month,
            source,
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet",
            ".parquet",
        )
        context["zones"] = _snapshot(root / "raw" / "zones", zones, ZONES_URL, ".csv")
        context["status"] = "prepared"
        dump_json(run_dir / "run.json", context)
        return context
    except Exception as error:
        record_failure(context, error)
        raise


def record_failure(context: dict, error: Exception) -> None:
    status = "commit_ack_lost" if isinstance(error, CommitAcknowledgementLost) else "failed"
    failed = {**context, "status": status, "error": f"{type(error).__name__}: {error}"}
    dump_json(Path(context["run_dir"]) / "run.json", failed)


def transform_run(context: dict) -> dict:
    from .transform import transform

    try:
        report = transform(context)
        dump_json(Path(context["run_dir"]) / "quality.json", report)
        context = {**context, "status": "transformed"}
        dump_json(Path(context["run_dir"]) / "run.json", context)
        return context
    except Exception as error:
        record_failure(context, error)
        raise


def validate_run(context: dict) -> dict:
    from decimal import Decimal

    report = json.loads((Path(context["run_dir"]) / "quality.json").read_text(encoding="utf-8"))
    errors = []
    if report["raw_count"] != report["accepted_count"] + report["quarantine_count"]:
        errors.append("row count conservation failed")
    if report["accepted_count"] == 0:
        errors.append("no accepted records")
    if Decimal(report["parseable_amount_total"]) != (
        Decimal(report["accepted_amount_total"]) + Decimal(report["quarantine_amount_total"])
    ):
        errors.append("canonical amount conservation failed")
    if sum(report["quarantine_reasons"].values()) != report["quarantine_count"]:
        errors.append("quarantine primary reason count mismatch")
    report["passed"] = not errors
    report["errors"] = errors
    dump_json(Path(context["run_dir"]) / "quality.json", report)
    if errors:
        error = ValueError("; ".join(errors))
        record_failure(context, error)
        raise error
    return {**context, "status": "validated"}


def publish_run(context: dict, fail_at: str | None = None) -> dict:
    from .warehouse import publish

    try:
        report = json.loads((Path(context["run_dir"]) / "quality.json").read_text(encoding="utf-8"))
        if not report.get("passed"):
            raise ValueError("Cannot publish an unvalidated run")
        publish(context, report, fail_at=fail_at)
    except Exception as error:
        record_failure(context, error)
        raise
    context = {**context, "status": "published"}
    dump_json(Path(context["run_dir"]) / "run.json", context)
    return {**context, "quality": report}


def run_month(
    month: str,
    data_root: str | Path,
    source: str | None = None,
    zones: str | None = None,
    fail_at: str | None = None,
) -> dict:
    context = prepare_run(month, data_root, source, zones)
    try:
        if fail_at == "before_transform":
            raise RuntimeError("Injected failure before_transform")
        context = transform_run(context)
        context = validate_run(context)
        if fail_at == "after_quality":
            raise RuntimeError("Injected failure after_quality")
        return publish_run(context, fail_at)
    except Exception as error:
        record_failure(context, error)
        raise
