"""Sequential parameterized DAG submissions, not calendar backfill."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from .pipeline import month_range


class AirflowClient:
    def __init__(self, base_url: str, token: str, dag_id: str = "nyc_taxi_monthly"):
        if not base_url or not token:
            raise ValueError("AIRFLOW_API_URL and AIRFLOW_API_TOKEN are required; tokens must stay in environment variables")
        if urllib.parse.urlparse(base_url).scheme not in ("http", "https"):
            raise ValueError("AIRFLOW_API_URL must be an HTTP(S) URL")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.dag_id = dag_id

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(payload).encode() if payload is not None else None,
            method=method,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            # Response bodies can contain credentials or infrastructure details.
            raise RuntimeError(f"Airflow API returned HTTP {error.code}; inspect the server logs") from None

    def trigger_and_wait(self, month: str, timeout: float = 1800, poll_seconds: float = 5) -> dict:
        if timeout <= 0 or poll_seconds <= 0:
            raise ValueError("Polling timeout and interval must be positive")
        run_id = f"monthly__{month}__{uuid.uuid4().hex}"
        dag_path = "/api/v2/dags/" + urllib.parse.quote(self.dag_id, safe="") + "/dagRuns"
        self._request("POST", dag_path, {"dag_run_id": run_id, "logical_date": None, "conf": {"month": month}})
        deadline = time.monotonic() + timeout
        run_path = dag_path + "/" + urllib.parse.quote(run_id, safe="")
        while True:
            result = self._request("GET", run_path)
            state = result["state"]
            if state == "success":
                return {"month": month, "dag_run_id": run_id, "state": state}
            if state == "failed":
                raise RuntimeError(f"Airflow run failed for {month}; later months were not submitted")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for {run_id}; inspect its state before resubmitting")
            time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))


def backfill(start: str, end: str, timeout: float = 1800, client: AirflowClient | None = None) -> list[dict]:
    months = month_range(start, end)
    client = client or AirflowClient(os.environ.get("AIRFLOW_API_URL", ""), os.environ.get("AIRFLOW_API_TOKEN", ""))
    results = []
    for month in months:
        results.append(client.trigger_and_wait(month, timeout=timeout))
    return results

