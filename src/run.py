"""Minimal pipeline entrypoint; applicants can extend or replace as desired."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample CLI for the pipeline.")
    parser.add_argument("--rawdir", type=Path, default=Path("data/raw"))
    parser.add_argument("--db", type=Path, default=Path("build/retail.duckdb"))
    parser.add_argument("--rebuild", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.rebuild and args.db.exists():
        args.db.unlink()

    args.db.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(args.db)) as conn:
        run_pipeline(conn=conn, rawdir=args.rawdir)


def run_pipeline(*, conn: duckdb.DuckDBPyConnection, rawdir: Path) -> None:
    """Placeholder for the applicant's implementation."""

    del conn, rawdir  # TODO: replace with real pipeline steps


if __name__ == "__main__":
    main()
