"""Month-parameter DAG. Range backfill is provided by the CLI API client."""

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task

from taxi_warehouse.pipeline import MONTHS, prepare_run, publish_run, transform_run, validate_run

with DAG(
    dag_id="nyc_taxi_monthly",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={"month": Param("2024-01", type="string", enum=list(MONTHS))},
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["taxi", "monthly", "local-development"],
) as dag:
    @task
    def prepare():
        context = get_current_context()
        month = context["params"]["month"]
        root = os.environ.get("TAXI_DATA_ROOT", "/opt/taxi-data")
        fixture_root = os.environ.get("TAXI_FIXTURE_ROOT")
        source = f"{fixture_root}/yellow_tripdata_{month}.parquet" if fixture_root else None
        zones = f"{fixture_root}/zones.csv" if fixture_root else None
        return prepare_run(month, root, source, zones)

    @task
    def transform(context):
        return transform_run(context)

    @task
    def validate(context):
        return validate_run(context)

    @task
    def publish(context):
        result = publish_run(context)
        return {"month": result["month"], "run_id": result["run_id"], "status": result["status"]}

    publish(validate(transform(prepare())))

