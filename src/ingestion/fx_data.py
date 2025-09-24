"""
GBP foreign exchange rate data ingestion from ECB XML.

Parses ECB SDMX XML format, extracts GBP/EUR exchange rates,
and creates staging tables with forward-filled rates for missing dates.
"""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def ingest_fx_data(conn: duckdb.DuckDBPyConnection, xml_path: Path) -> None:
    """
    Load GBP exchange rate data from ECB XML file into DuckDB.
    
    Parses ECB SDMX format XML and extracts daily GBP/EUR exchange rates.
    Creates a staging table with date and rate columns.
    """
    logger.info(f"Loading GBP FX data from {xml_path}")

    # Parse the XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Define ECB XML namespaces
    namespaces = {
        'message': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message',
        'ecb': 'http://www.ecb.europa.eu/vocabulary/stats/exr/1'
    }

    logger.info("Parsing ECB XML structure")

    # Find the dataset
    dataset = root.find('.//ecb:DataSet', namespaces)
    if dataset is None:
        raise ValueError("Could not find DataSet in XML file")

    # Find all currency series (should be just GBP)
    series = dataset.findall('.//ecb:Series', namespaces)
    logger.info(f"Found {len(series)} currency series")

    if not series:
        raise ValueError("No currency series found in XML file")

    # Extract data from the first (and likely only) series
    first_series = series[0]
    series_info = first_series.attrib
    logger.info(f"Series attributes: {series_info}")

    # Validate this is GBP data
    if series_info.get('CURRENCY') != 'GBP':
        raise ValueError(
            f"Expected GBP currency, got {series_info.get('CURRENCY')}")

    # Extract all observations (date + rate pairs)
    observations = first_series.findall('.//ecb:Obs', namespaces)
    logger.info(f"Found {len(observations)} exchange rate observations")

    if not observations:
        raise ValueError("No observations found in series")

    # Convert observations to list of dictionaries
    fx_data = []
    for obs in observations:
        date_str = obs.get('TIME_PERIOD')
        rate_str = obs.get('OBS_VALUE')

        if date_str and rate_str:
            try:
                # Parse date (should be YYYY-MM-DD format)
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                rate_value = float(rate_str)

                fx_data.append({
                    'date': date_obj,
                    'gbp_per_eur': rate_value
                })
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Skipping invalid observation: {date_str}={rate_str}, error: {e}")

    logger.info(f"Successfully parsed {len(fx_data)} valid exchange rates")

    if not fx_data:
        raise ValueError("No valid exchange rate data found")

    # Convert to DataFrame
    fx_df = pd.DataFrame(fx_data)

    # Sort by date
    fx_df = fx_df.sort_values('date').reset_index(drop=True)

    # Log date range
    min_date = fx_df['date'].min()
    max_date = fx_df['date'].max()
    logger.info(f"FX data date range: {min_date} to {max_date}")

    # Load into DuckDB staging table
    logger.info("Creating raw_fx_rates staging table")
    conn.execute("DROP TABLE IF EXISTS raw_fx_rates")
    conn.execute("""
        CREATE TABLE raw_fx_rates AS 
        SELECT * FROM fx_df
    """)

    # Verify data loaded
    row_count = conn.execute("SELECT COUNT(*) FROM raw_fx_rates").fetchone()[0]
    logger.info(
        f"Successfully loaded {row_count:,} FX rates into raw_fx_rates table")

    # Log some sample data
    sample_data = conn.execute("""
        SELECT date, gbp_per_eur 
        FROM raw_fx_rates 
        ORDER BY date 
        LIMIT 3
    """).fetchall()

    logger.info("Sample FX data:")
    for date, rate in sample_data:
        logger.info(f"  {date}: 1 EUR = {rate} GBP")

    # Check for gaps in data (weekends/holidays will have gaps)
    total_days = (max_date - min_date).days + 1
    logger.info(
        f"Date span: {total_days} calendar days, {row_count} trading days")
    logger.info(f"Missing days (weekends/holidays): {total_days - row_count}")
