"""Actual PySpark processing; never invent or deduplicate a real trip ID."""

from __future__ import annotations

from pathlib import Path

REQUIRED = (
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
    "DOLocationID", "trip_distance", "total_amount", "payment_type",
)
FACT_COLUMNS = (
    "source_month", "pickup_at", "dropoff_at", "pickup_date", "date_key", "vendor_id",
    "pu_zone_key", "do_zone_key", "pu_location_id_raw", "do_location_id_raw",
    "trip_distance", "total_amount", "payment_type", "run_id",
)


def spark_session(name: str = "NYCTaxiWarehouse"):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]").appName(name)
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_zones(spark, path: str):
    from pyspark.sql import functions as F

    raw = spark.read.option("header", True).csv(path)
    required = {"LocationID", "Borough", "Zone", "service_zone"}
    if not required.issubset(raw.columns):
        raise ValueError("Taxi zone lookup is missing required columns")
    zones = raw.select(
        F.expr("try_cast(LocationID as int)").alias("zone_key"),
        F.col("Borough").alias("borough"), F.col("Zone").alias("zone"),
        F.col("service_zone"),
    )
    if zones.filter("zone_key IS NULL OR zone_key < 0").limit(1).count():
        raise ValueError("Invalid or reserved zone key")
    if zones.groupBy("zone_key").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate zone dimension key would multiply fact rows")
    if zones.limit(1).count() == 0:
        raise ValueError("Empty zone dimension")
    return zones


def _finite(column):
    from pyspark.sql import functions as F

    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) != float("inf"))


def jvm_peak_rss_kib(spark) -> int:
    """Linux high-water RSS for this JVM, not the sum of all service memory."""
    pid = spark._jvm.java.lang.ProcessHandle.current().pid()
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    raise RuntimeError("Linux JVM high-water memory measurement is unavailable")


def transform(context: dict, spark=None) -> dict:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    owned_session = spark is None
    spark = spark or spark_session()
    normalized = None
    try:
        raw = spark.read.parquet(context["source"]["path"])
        missing = set(REQUIRED) - set(raw.columns)
        if missing:
            raise ValueError("Missing source columns: " + ", ".join(sorted(missing)))
        zones = read_zones(spark, context["zones"]["path"])
        # Raw fields are retained in quarantine, alongside explicitly normalized values.
        normalized = raw.select(
            *[F.col(c).cast("string").alias("raw_" + c) for c in REQUIRED],
            F.expr("try_cast(tpep_pickup_datetime as timestamp_ntz)").alias("pickup_at"),
            F.expr("try_cast(tpep_dropoff_datetime as timestamp_ntz)").alias("dropoff_at"),
            F.expr("try_cast(VendorID as int)").alias("vendor_id"),
            F.expr("try_cast(PULocationID as int)").alias("pu_location_id_raw"),
            F.expr("try_cast(DOLocationID as int)").alias("do_location_id_raw"),
            F.expr("try_cast(trip_distance as double)").alias("trip_distance"),
            F.expr("try_cast(total_amount as decimal(18,2))").alias("total_amount"),
            F.expr("try_cast(payment_type as int)").alias("payment_type"),
        )
        normalized = (
            normalized.withColumn("pickup_date", F.to_date("pickup_at"))
            .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
            .withColumn("source_month", F.lit(context["month"]))
            .withColumn("run_id", F.lit(context["run_id"]))
        )
        rules = [
            (F.col("pickup_at").isNull(), "invalid_pickup", "validation_error"),
            (F.col("dropoff_at").isNull(), "invalid_dropoff", "validation_error"),
            (F.col("dropoff_at") <= F.col("pickup_at"), "invalid_time_order", "validation_error"),
            (~_finite(F.col("trip_distance")), "invalid_distance", "validation_error"),
            (F.col("total_amount").isNull(), "invalid_amount", "validation_error"),
            (F.date_format("pickup_at", "yyyy-MM") != context["month"], "outside_source_month", "business_exclusion"),
            (F.col("trip_distance") < 0, "negative_distance", "business_exclusion"),
            (F.col("total_amount") < 0, "negative_amount", "business_exclusion"),
        ]
        reason = F.lit(None).cast("string")
        reason_class = F.lit(None).cast("string")
        for condition, code, classification in reversed(rules):
            reason = F.when(condition, F.lit(code)).otherwise(reason)
            reason_class = F.when(condition, F.lit(classification)).otherwise(reason_class)
        normalized = (
            normalized.withColumn("primary_reason", reason)
            .withColumn("reason_class", reason_class)
            .withColumn("all_reasons", F.concat_ws(",", *[F.when(condition, F.lit(code)) for condition, code, _ in rules]))
            .persist(StorageLevel.DISK_ONLY)
        )
        accepted = normalized.filter("primary_reason IS NULL")
        quarantine = normalized.filter("primary_reason IS NOT NULL")
        metrics = normalized.agg(
            F.count("*").alias("raw_count"),
            F.sum(F.when(F.col("primary_reason").isNull(), 1).otherwise(0)).alias("accepted_count"),
            F.sum(F.when(F.col("primary_reason").isNotNull(), 1).otherwise(0)).alias("quarantine_count"),
            F.sum("total_amount").alias("parseable_amount_total"),
            F.sum(F.when(F.col("primary_reason").isNull(), F.col("total_amount"))).alias("accepted_amount_total"),
            F.sum(F.when(F.col("primary_reason").isNotNull(), F.col("total_amount"))).alias("quarantine_amount_total"),
            F.sum(F.when(F.col("total_amount").isNull(), 1).otherwise(0)).alias("unparseable_amount_count"),
        ).first().asDict()
        for key in ("raw_count", "accepted_count", "quarantine_count", "unparseable_amount_count"):
            metrics[key] = int(metrics[key] or 0)
        for key in ("parseable_amount_total", "accepted_amount_total", "quarantine_amount_total"):
            metrics[key] = str(metrics[key] or "0.00")
        # Observe equal public records; do not infer that they represent the same real trip.
        duplicate_groups = raw.groupBy(*raw.columns).count().filter("count > 1")
        duplicate_stats = duplicate_groups.agg(
            F.count("*").alias("identical_source_groups"),
            F.sum(F.col("count") - 1).alias("identical_source_extra_rows"),
        ).first().asDict()
        metrics.update({key: int(value or 0) for key, value in duplicate_stats.items()})
        metrics["quarantine_reasons"] = {
            row["primary_reason"]: row["count"] for row in quarantine.groupBy("primary_reason").count().collect()
        }
        keys = zones.select("zone_key")
        accepted = (
            accepted.join(F.broadcast(keys.withColumnRenamed("zone_key", "_pu_match")),
                          F.col("pu_location_id_raw") == F.col("_pu_match"), "left")
            .join(F.broadcast(keys.withColumnRenamed("zone_key", "_do_match")),
                  F.col("do_location_id_raw") == F.col("_do_match"), "left")
            .withColumn("pu_zone_key", F.coalesce("_pu_match", F.lit(-1)))
            .withColumn("do_zone_key", F.coalesce("_do_match", F.lit(-1)))
        )
        metrics["unknown_pickup_count"] = accepted.filter("pu_zone_key = -1").count()
        metrics["unknown_dropoff_count"] = accepted.filter("do_zone_key = -1").count()
        run_dir = Path(context["run_dir"])
        accepted.select(*FACT_COLUMNS).repartition(2).write.mode("overwrite").partitionBy("pickup_date").parquet(str(run_dir / "accepted"))
        quarantine.repartition(2).write.mode("overwrite").parquet(str(run_dir / "quarantine"))
        unknown = spark.createDataFrame([(-1, "Unknown", "Unknown", "Unknown")], zones.schema)
        zones.unionByName(unknown).coalesce(1).write.mode("overwrite").parquet(str(run_dir / "zones"))
        metrics.update({
            "jvm_peak_rss_kib": jvm_peak_rss_kib(spark),
            "month": context["month"], "run_id": context["run_id"], "passed": False,
            "record_grain": "source observation; identical records retained",
            "amount_policy": "DECIMAL(18,2); total_amount excludes unrecorded cash tips",
            "distance_unit": "mile",
        })
        return metrics
    finally:
        if normalized is not None:
            normalized.unpersist()
        if owned_session:
            spark.stop()
