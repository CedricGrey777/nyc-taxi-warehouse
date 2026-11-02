import json

import pytest

from taxi_warehouse.airflow_client import AirflowClient, backfill
from taxi_warehouse.pipeline import dump_json, month_range, validate_month, validate_run


def test_month_contract_and_inclusive_range():
    assert month_range("2024-01", "2024-03") == ["2024-01", "2024-02", "2024-03"]
    with pytest.raises(ValueError):
        validate_month("2025-01")
    with pytest.raises(ValueError):
        month_range("2024-03", "2024-01")


def test_range_waits_in_order_and_stops_on_failure():
    class Client:
        months = []

        def trigger_and_wait(self, month, timeout):
            self.months.append(month)
            if month == "2024-02":
                raise RuntimeError("failed")
            return {"state": "success", "month": month}

    client = Client()
    with pytest.raises(RuntimeError):
        backfill("2024-01", "2024-03", client=client)
    assert client.months == ["2024-01", "2024-02"]


def test_api_submission_schema_and_poll(monkeypatch):
    client = AirflowClient("http://localhost:8080", "not-a-real-token")
    calls = []

    def request(method, path, payload=None):
        calls.append((method, path, payload))
        return {"state": "success"}

    monkeypatch.setattr(client, "_request", request)
    assert client.trigger_and_wait("2024-02")["state"] == "success"
    assert calls[0][2]["logical_date"] is None
    assert calls[0][2]["conf"] == {"month": "2024-02"}
    assert calls[1][0] == "GET"


def test_failed_api_run_does_not_report_success(monkeypatch):
    client = AirflowClient("http://localhost:8080", "not-a-real-token")
    monkeypatch.setattr(client, "_request", lambda *args: {"state": "failed"})
    with pytest.raises(RuntimeError, match="failed"):
        client.trigger_and_wait("2024-01")


def test_quality_gate_rejects_broken_conservation(tmp_path):
    context = {"run_dir": str(tmp_path), "run_id": "unit", "month": "2024-01"}
    dump_json(tmp_path / "quality.json", {
        "raw_count": 3, "accepted_count": 2, "quarantine_count": 0,
        "parseable_amount_total": "20.00", "accepted_amount_total": "20.00",
        "quarantine_amount_total": "0.00", "quarantine_reasons": {},
    })
    with pytest.raises(ValueError, match="row count"):
        validate_run(context)
    assert json.loads((tmp_path / "quality.json").read_text())["passed"] is False

