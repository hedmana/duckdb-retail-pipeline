"""
UK bank holidays data ingestion from Excel files.

Processes UK bank holiday dates for calendar dimension enrichment
and business day analysis in retail analytics.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def ingest_holidays_data(conn: duckdb.DuckDBPyConnection, excel_path: Path) -> None:
    """
    Load UK bank holidays data from Excel file into DuckDB.
    
    Cleans the holiday dates and creates a staging table for calendar dimension.
    """
    logger.info(f"Loading UK bank holidays from {excel_path}")

    # Read the Excel file
    df = pd.read_excel(excel_path, engine='xlrd')
    logger.info(
        f"Loaded holidays file: {df.shape[0]} rows, {df.shape[1]} columns")
    logger.info(f"Columns: {list(df.columns)}")

    # Get the holiday column (should be 'UK BANK HOLIDAYS')
    holiday_col = df.columns[0]  # First (and likely only) column
    logger.info(f"Using column '{holiday_col}' for holiday dates")

    # Clean the data - remove NaT/null values
    clean_holidays = df[holiday_col].dropna()
    logger.info(
        f"After removing nulls: {len(clean_holidays)} valid holiday dates")

    # Convert to date format and remove any time component
    clean_holidays = pd.to_datetime(clean_holidays).dt.date

    # Create a clean DataFrame
    holidays_df = pd.DataFrame({
        'holiday_date': clean_holidays
    }).drop_duplicates().sort_values('holiday_date').reset_index(drop=True)

    # Log date range
    min_date = holidays_df['holiday_date'].min()
    max_date = holidays_df['holiday_date'].max()
    logger.info(f"Holiday date range: {min_date} to {max_date}")
    logger.info(f"Total unique holidays: {len(holidays_df)}")

    # Filter to a reasonable range around your retail data (optional)
    # For now, keep all holidays as they might be useful for future analysis

    # Load into DuckDB staging table
    logger.info("Creating raw_uk_holidays staging table")
    conn.execute("DROP TABLE IF EXISTS raw_uk_holidays")
    conn.execute("""
        CREATE TABLE raw_uk_holidays AS 
        SELECT * FROM holidays_df
    """)

    # Verify data loaded
    row_count = conn.execute(
        "SELECT COUNT(*) FROM raw_uk_holidays").fetchone()[0]
    logger.info(
        f"Successfully loaded {row_count:,} holidays into raw_uk_holidays table")

    # Log some sample holidays around the retail data period (2009-2011)
    sample_holidays = conn.execute("""
        SELECT holiday_date 
        FROM raw_uk_holidays 
        WHERE holiday_date BETWEEN '2009-01-01' AND '2012-12-31'
        ORDER BY holiday_date
        LIMIT 10
    """).fetchall()

    if sample_holidays:
        logger.info("Sample holidays during retail period (2009-2012):")
        for (holiday_date,) in sample_holidays:
            logger.info(f"  {holiday_date}")

    # Count holidays in retail period
    retail_period_holidays = conn.execute("""
        SELECT COUNT(*) 
        FROM raw_uk_holidays 
        WHERE holiday_date BETWEEN '2009-12-01' AND '2011-12-31'
    """).fetchone()[0]

    logger.info(
        f"UK holidays during retail period (Dec 2009 - Dec 2011): {retail_period_holidays}")
