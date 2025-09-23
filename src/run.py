"""Retail data pipeline entrypoint for DuckDB-based analytics."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

from ingestion.retail_data import ingest_retail_data
from ingestion.fx_data import ingest_fx_data
from ingestion.holidays_data import ingest_holidays_data


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
    """Main pipeline orchestration."""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    logger.info("Starting retail data pipeline")
    logger.info(f"Raw data directory: {rawdir}")
    logger.info(f"Database: {conn}")

    try:
        # Phase 1: Data Ingestion
        logger.info("Phase 1: Ingesting raw data files")
        ingest_retail_data(conn, rawdir / "online_retail_II.xlsx")
        ingest_fx_data(conn, rawdir / "gbp.xml")
        ingest_holidays_data(conn, rawdir / "ukbankholidays-jul19.xls")

        # Phase 2: Build Dimensional Tables
        logger.info("Phase 2: Building dimensional tables")
        # TODO: create_dim_calendar(conn)
        # TODO: create_dim_product(conn)
        # TODO: create_dim_customer(conn)

        # Phase 3: Build Fact Tables
        logger.info("Phase 3: Building fact tables")
        # TODO: create_fct_sales(conn)
        # TODO: create_daily_fx_rates(conn)
        # TODO: create_fct_sales_eur(conn)

        # Phase 4: Build Aggregations
        logger.info("Phase 4: Building aggregation tables")
        # TODO: create_agg_country_day(conn)

        # Phase 5: Data Quality Validation
        logger.info("Phase 5: Running data quality checks")
        # TODO: validate_data_quality(conn)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
