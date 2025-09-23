"""Retail transaction data ingestion."""

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def ingest_retail_data(conn: duckdb.DuckDBPyConnection, excel_path: Path) -> None:
    """
    Load retail transaction data from Excel file into DuckDB.
    
    Combines all sheets into a single staging table. Handles column name 
    standardization and basic data type conversion.
    """
    logger.info(f"Loading retail data from {excel_path}")

    # Read sheets
    sheets = pd.read_excel(excel_path, sheet_name=None, engine='openpyxl')
    logger.info(f"Found {len(sheets)} sheets: {list(sheets.keys())}")

    # Combine all sheets into one DataFrame
    all_data = []
    for sheet_name, df in sheets.items():
        logger.info(f"Processing sheet '{sheet_name}': {len(df):,} rows")
        # Add sheet identifier if needed for debugging
        df['source_sheet'] = sheet_name
        all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"Combined dataset: {len(combined_df):,} total rows")

    # Standardize column names to match assignment specs
    column_mapping = {
        'Invoice': 'invoice_no',
        'StockCode': 'stock_code',
        'Description': 'description',
        'Quantity': 'qty',
        'InvoiceDate': 'invoice_ts',
        'Price': 'unit_price_gbp',
        'Customer ID': 'customer_id',
        'Country': 'country',
        'source_sheet': 'source_sheet'
    }

    combined_df = combined_df.rename(columns=column_mapping)
    logger.info(f"Standardized columns: {list(combined_df.columns)}")

    # Trim whitespace from string columns
    string_cols = ['invoice_no', 'stock_code', 'description', 'country']
    for col in string_cols:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].astype(str).str.strip()

    # Load into DuckDB staging table
    logger.info("Creating raw_retail_data staging table")
    conn.execute("DROP TABLE IF EXISTS raw_retail_data")
    conn.execute("""
        CREATE TABLE raw_retail_data AS 
        SELECT * FROM combined_df
    """)

    # Log some basic statistics
    row_count = conn.execute(
        "SELECT COUNT(*) FROM raw_retail_data").fetchone()[0]
    logger.info(
        f"Successfully loaded {row_count:,} rows into raw_retail_data table")

    cancellations = conn.execute("""
        SELECT COUNT(*) FROM raw_retail_data 
        WHERE invoice_no LIKE 'C%'
    """).fetchone()[0]

    null_customers = conn.execute("""
        SELECT COUNT(*) FROM raw_retail_data 
        WHERE customer_id IS NULL
    """).fetchone()[0]

    logger.info(f"Data quality preview:")
    logger.info(f"  - Cancellations (C* invoices): {cancellations:,}")
    logger.info(f"  - NULL customer IDs: {null_customers:,}")
