"""Controlled join comparison on one explicit published snapshot."""

from __future__ import annotations

import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .pipeline import dump_json, validate_month
from .transform import jvm_peak_rss_kib, spark_session


def _plan_metrics(plan) -> list[dict]:
    values = []
    metrics = plan.metrics().iterator()
    while metrics.hasNext():
        pair = metrics.next()
        values.append({"node": plan.nodeName(), "key": pair._1(), "value": int(pair._2().value())})
    children = plan.children().iterator()
    while children.hasNext():
        values.extend(_plan_metrics(children.next()))
    return values


def benchmark_month(data_root: str | Path, month: str, repetitions: int = 5) -> dict:
    from pyspark.sql import functions as F

    validate_month(month)
    if repetitions < 1:
        raise ValueError("At least one measured repetition is required")
    root = Path(data_root).resolve()
    with duckdb.connect(str(root / "warehouse.duckdb"), read_only=True) as connection:
        row = connection.execute(
            """SELECT r.run_id, r.accepted_path, r.source_sha256, r.quality_json
            FROM published_months p JOIN pipeline_runs r ON p.run_id=r.run_id WHERE p.source_month=?""",
            [month],
        ).fetchone()
    if row is None:
        raise ValueError("Publish the requested month before benchmarking")
    run_id, accepted_path, source_sha, quality_text = row
    spark = spark_session("NYCTaxiJoinBenchmark")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    times = {"merge": [], "broadcast": []}
    details = {}
    expected = None
    try:
        facts = spark.read.parquet(accepted_path)
        zones = spark.read.parquet(str(Path(accepted_path).parent / "zones"))

        def query(strategy):
            fact = facts.hint("merge") if strategy == "merge" else facts
            pickup = zones.hint(strategy).select(F.col("zone_key").alias("pu_key"), F.col("borough").alias("pu_borough"))
            dropoff = zones.hint(strategy).select(F.col("zone_key").alias("do_key"), F.col("borough").alias("do_borough"))
            return (fact.join(pickup, F.col("pu_zone_key") == F.col("pu_key"))
                    .join(dropoff, F.col("do_zone_key") == F.col("do_key"))
                    .groupBy("pickup_date", "pu_borough", "do_borough")
                    .agg(F.count("*").alias("record_count"), F.sum("total_amount").alias("charge_total")))

        # First pair is warm-up. Subsequent pairs alternate execution order.
        for iteration in range(repetitions + 1):
            order = ("merge", "broadcast") if iteration % 2 == 0 else ("broadcast", "merge")
            for strategy in order:
                frame = query(strategy)
                start = time.perf_counter()
                rows = frame.collect()
                elapsed = time.perf_counter() - start
                actual = sorted(json.dumps(record.asDict(), default=str, sort_keys=True) for record in rows)
                if expected is None:
                    expected = actual
                elif actual != expected:
                    raise ValueError("Join strategies returned different business results")
                plan = frame._jdf.queryExecution().executedPlan()
                plan_text = plan.toString()
                required_node = "BroadcastHashJoin" if strategy == "broadcast" else "SortMergeJoin"
                if required_node not in plan_text:
                    raise ValueError(f"Expected physical operator {required_node} was not observed")
                if iteration:
                    times[strategy].append(elapsed)
                    details[strategy] = {"plan": plan_text, "operator_metrics": _plan_metrics(plan)}
        report = {
            "month": month, "run_id": run_id, "source_sha256": source_sha,
            "input_records": json.loads(quality_text)["accepted_count"],
            "measured_at": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version, "master": spark.sparkContext.master,
            "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
            "jvm_peak_rss_kib": jvm_peak_rss_kib(spark),
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "aqe_enabled": False, "automatic_broadcast": False,
            "conditions": "warm JVM/OS cache; no DataFrame cache; excludes download and JVM startup",
            "repetitions": repetitions, "seconds": times,
            "median_seconds": {key: statistics.median(value) for key, value in times.items()},
            "outputs_equal": True, "output_groups": len(expected or []), "strategies": details,
            "interpretation": "Controlled single-machine experiment; no speedup or distributed claim assumed",
        }
        dump_json(root / "benchmarks" / f"{month}-{run_id}.json", report)
        return report
    finally:
        spark.stop()
