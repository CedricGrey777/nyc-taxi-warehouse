from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="2024 Q1 NYC TLC monthly warehouse")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run-month", help="Run one real local Spark/Parquet/DuckDB pipeline")
    run.add_argument("--month", required=True)
    run.add_argument("--data-root", default="data")
    run.add_argument("--source", help="Explicit local Parquet; otherwise acquire public TLC data")
    run.add_argument("--zones", help="Explicit local zone CSV; otherwise acquire TLC lookup")
    run.add_argument("--fail-at", choices=["before_transform", "after_quality", "after_delete", "before_commit", "after_commit_response"])
    back = commands.add_parser("backfill", help="Sequentially trigger parameterized Airflow DAG runs and wait")
    back.add_argument("--from", dest="start", required=True)
    back.add_argument("--to", dest="end", required=True)
    back.add_argument("--timeout", type=float, default=1800, help="Timeout per DAG run in seconds")
    check = commands.add_parser("check", help="Read-only published fact/mart reconciliation")
    check.add_argument("--month", required=True)
    check.add_argument("--data-root", default="data")
    fixture = commands.add_parser("fixtures", help="Write explicitly synthetic tiny fixtures, not public data")
    fixture.add_argument("--output", default="data/fixtures")
    benchmark = commands.add_parser("benchmark", help="Controlled Spark broadcast vs merge experiment")
    benchmark.add_argument("--data-root", default="data")
    benchmark.add_argument("--month", required=True)
    benchmark.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args(argv)
    try:
        if args.command == "run-month":
            from .pipeline import run_month
            result = run_month(args.month, args.data_root, args.source, args.zones, args.fail_at)
        elif args.command == "backfill":
            from .airflow_client import backfill
            result = backfill(args.start, args.end, args.timeout)
        elif args.command == "check":
            from .warehouse import check_month
            result = check_month(args.data_root, args.month)
        elif args.command == "fixtures":
            from .fixtures import create_fixtures
            result = create_fixtures(args.output)
        else:
            from .benchmark import benchmark_month
            result = benchmark_month(args.data_root, args.month, args.repetitions)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
