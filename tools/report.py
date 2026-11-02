"""Export three aggregate analyses from an existing warehouse, read-only."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

SQL_ROOT = Path(__file__).resolve().parents[1] / "sql"
REPORTS = (
    ("daily_trends", "日趋势"),
    ("top_pickup_zones", "上车地区 Top 10"),
    ("weekday_weekend", "工作日与周末"),
)
METRIC_NOTE = (
    "记录量指接纳的公开源记录；记录收费总额以美元计，不是企业净收入，"
    "不含未记录的现金小费。平均行程距离为总距离／总记录量，单位英里；"
    "日均记录量以有接纳记录的日期数为分母。"
)
COLUMN_LABELS = {
    "pickup_date": "上车日期",
    "pu_zone_key": "上车地区键",
    "borough": "行政区",
    "zone": "地区",
    "day_type": "日期类型",
    "observed_days": "有记录日期数",
    "trip_record_count": "记录量",
    "recorded_charge_total_usd": "记录收费总额（美元）",
    "avg_trip_distance_miles": "平均记录行程距离（英里）",
    "avg_daily_record_count": "日均记录量",
}


def _rows(connection, query: str) -> list[dict]:
    cursor = connection.execute(query)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def read_reports(data_root: str | Path) -> dict:
    database = Path(data_root) / "warehouse.duckdb"
    with duckdb.connect(str(database), read_only=True) as connection:
        published_months = _rows(connection, """
            SELECT p.source_month, p.run_id, r.published_at
            FROM published_months p JOIN pipeline_runs r ON p.run_id = r.run_id
            ORDER BY p.source_month
        """)
        reports = {
            name: _rows(connection, (SQL_ROOT / f"{name}.sql").read_text(encoding="utf-8"))
            for name, _ in REPORTS
        }
    return {"metric_note": METRIC_NOTE, "published_months": published_months, "reports": reports}


def _cell(value) -> str:
    if isinstance(value, float):
        text = f"{value:.6f}".rstrip("0").rstrip(".")
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def render_markdown(report: dict) -> str:
    lines = ["# NYC 出行运营指标", "", report["metric_note"], "", "已发布月份与来源运行：", ""]
    for month in report["published_months"]:
        lines.append(f"- {month['source_month']}：`{month['run_id']}`（{month['published_at']}）")
    for name, title in REPORTS:
        lines.extend(["", f"## {title}", ""])
        rows = report["reports"][name]
        if not rows:
            lines.append("暂无已发布的汇总记录。")
            continue
        columns = list(rows[0])
        lines.append("| " + " | ".join(COLUMN_LABELS[column] for column in columns) + " |")
        lines.append("| " + " | ".join("---" for _ in columns) + " |")
        lines.extend("| " + " | ".join(_cell(row[column]) for column in columns) + " |" for row in rows)
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    args = parser.parse_args(argv)
    try:
        report = read_reports(args.data_root)
    except (duckdb.Error, OSError) as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(render_markdown(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
