"""
Dimensional table creation for retail data warehouse.

Creates dimension tables following star schema design:
- Calendar dimension with business calendar logic
- Product dimension with lifecycle tracking  
- Customer dimension with geographic context
"""

import logging
from datetime import datetime, date, timedelta

import duckdb

logger = logging.getLogger(__name__)


def create_dim_calendar(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create calendar dimension table spanning the retail data period.
    
    Includes date attributes: is_weekend, is_uk_holiday, iso_week, iso_year, month, year.
    Assumes Europe/London timezone for date calculations.
    """
    logger.info("Creating dim_calendar table")

    # Get the actual date range from retail data
    date_range = conn.execute("""
        SELECT 
            MIN(DATE(invoice_ts)) as min_date,
            MAX(DATE(invoice_ts)) as max_date
        FROM raw_retail_data
    """).fetchone()

    min_date, max_date = date_range
    logger.info(f"Retail data date range: {min_date} to {max_date}")

    # Extend range slightly for completeness (e.g., include full months)
    start_date = date(min_date.year, min_date.month, 1)  # Start of month

    # Handle end of month calculation properly (handle December -> January)
    if max_date.month == 12:
        end_date = date(max_date.year + 1, 1, 1) - \
            timedelta(days=1)  # End of December
    else:
        end_date = date(max_date.year, max_date.month + 1, 1) - \
            timedelta(days=1)  # End of month

    logger.info(f"Calendar dimension range: {start_date} to {end_date}")

    # Create calendar table using DuckDB's date generation
    logger.info("Generating calendar dates")
    conn.execute("DROP TABLE IF EXISTS dim_calendar")

    conn.execute(f"""
        CREATE TABLE dim_calendar AS
        SELECT 
            date_val as date,
            EXTRACT(dow FROM date_val) IN (0, 6) as is_weekend,  -- Sunday=0, Saturday=6
            EXTRACT(isoyear FROM date_val) as iso_year,
            EXTRACT(week FROM date_val) as iso_week, 
            EXTRACT(month FROM date_val) as month,
            EXTRACT(year FROM date_val) as year,
            EXTRACT(dow FROM date_val) as day_of_week,
            DAYNAME(date_val) as day_name,
            MONTHNAME(date_val) as month_name
        FROM (
            SELECT unnest(generate_series(
                DATE '{start_date}', 
                DATE '{end_date}', 
                INTERVAL '1 day'
            )) as date_val
        )
        ORDER BY date_val
    """)

    # Add UK holidays flag by joining with raw_uk_holidays
    logger.info("Adding UK holiday flags")
    conn.execute("""
        ALTER TABLE dim_calendar 
        ADD COLUMN is_uk_holiday BOOLEAN DEFAULT FALSE
    """)

    # Update holiday flags
    conn.execute("""
        UPDATE dim_calendar 
        SET is_uk_holiday = TRUE
        WHERE date IN (
            SELECT holiday_date 
            FROM raw_uk_holidays 
            WHERE holiday_date BETWEEN (SELECT MIN(date) FROM dim_calendar) 
                                   AND (SELECT MAX(date) FROM dim_calendar)
        )
    """)

    # Verify the table
    row_count = conn.execute("SELECT COUNT(*) FROM dim_calendar").fetchone()[0]
    logger.info(f"Created dim_calendar with {row_count:,} dates")

    # Log some statistics
    weekend_count = conn.execute("""
        SELECT COUNT(*) FROM dim_calendar WHERE is_weekend = TRUE
    """).fetchone()[0]

    holiday_count = conn.execute("""
        SELECT COUNT(*) FROM dim_calendar WHERE is_uk_holiday = TRUE
    """).fetchone()[0]

    logger.info(f"Calendar statistics:")
    logger.info(f"  - Weekend days: {weekend_count:,}")
    logger.info(f"  - UK holidays: {holiday_count:,}")

    # Sample some dates to verify
    sample_dates = conn.execute("""
        SELECT date, is_weekend, is_uk_holiday, iso_week, iso_year, day_name
        FROM dim_calendar 
        ORDER BY date 
        LIMIT 5
    """).fetchall()

    logger.info("Sample calendar dates:")
    for date_val, is_weekend, is_holiday, iso_week, iso_year, day_name in sample_dates:
        holiday_flag = " (Holiday)" if is_holiday else ""
        weekend_flag = " (Weekend)" if is_weekend else ""
        logger.info(
            f"  {date_val} {day_name} - Week {iso_week}/{iso_year}{weekend_flag}{holiday_flag}")

    # Validate no gaps in date sequence
    gap_check = conn.execute("""
        SELECT COUNT(*) as gap_count
        FROM (
            SELECT date, 
                   LAG(date) OVER (ORDER BY date) as prev_date,
                   date - LAG(date) OVER (ORDER BY date) as date_diff
            FROM dim_calendar
        ) 
        WHERE date_diff > INTERVAL '1 day'
    """).fetchone()[0]

    if gap_check > 0:
        logger.warning(f"Found {gap_check} gaps in calendar sequence!")
    else:
        logger.info("Calendar sequence validated - no gaps found")


def create_dim_product(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create product dimension table from retail data.
    
    Schema: dim_product (stock_code, description, first_seen, last_seen)
    """
    logger.info("Creating dim_product table")

    conn.execute("DROP TABLE IF EXISTS dim_product")

    # Create product dimension with first/last seen dates
    conn.execute("""
        CREATE TABLE dim_product AS
        SELECT 
            stock_code,
            -- Use the most common description (in case of variants)
            MODE(description) as description,
            MIN(DATE(invoice_ts)) as first_seen,
            MAX(DATE(invoice_ts)) as last_seen
        FROM raw_retail_data
        WHERE stock_code IS NOT NULL 
          AND stock_code != ''
          AND stock_code != 'nan'
        GROUP BY stock_code
        ORDER BY stock_code
    """)

    # Verify and log statistics
    row_count = conn.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]
    logger.info(f"Created dim_product with {row_count:,} unique products")

    # Sample products
    sample_products = conn.execute("""
        SELECT stock_code, description, first_seen, last_seen
        FROM dim_product 
        ORDER BY stock_code 
        LIMIT 5
    """).fetchall()

    logger.info("Sample products:")
    for stock_code, desc, first_seen, last_seen in sample_products:
        logger.info(
            f"  {stock_code}: {desc[:50]}... ({first_seen} to {last_seen})")


def create_dim_customer(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create customer dimension table from retail data.
    
    Schema: dim_customer (customer_id, country)
    Includes UNKNOWN_CUSTOMER surrogate for null customer IDs.
    """
    logger.info("Creating dim_customer table")

    conn.execute("DROP TABLE IF EXISTS dim_customer")

    # Create customer dimension with UNKNOWN_CUSTOMER handling
    conn.execute("""
        CREATE TABLE dim_customer AS
        SELECT 
            COALESCE(customer_id, -1) as customer_id,
            CASE 
                WHEN COALESCE(customer_id, -1) = -1 THEN 'UNKNOWN'
                ELSE MODE(country)  -- Most common country for this customer
            END as country
        FROM raw_retail_data
        GROUP BY COALESCE(customer_id, -1)
        ORDER BY customer_id
    """)

    # Verify and log statistics
    row_count = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    logger.info(f"Created dim_customer with {row_count:,} unique customers")

    # Check UNKNOWN_CUSTOMER
    unknown_customer = conn.execute("""
        SELECT customer_id, country 
        FROM dim_customer 
        WHERE customer_id = -1
    """).fetchone()

    if unknown_customer:
        logger.info(
            f"UNKNOWN_CUSTOMER created: ID={unknown_customer[0]}, Country='{unknown_customer[1]}'")

    # Count known vs unknown
    known_customers = conn.execute("""
        SELECT COUNT(*) FROM dim_customer WHERE customer_id != -1
    """).fetchone()[0]

    logger.info(
        f"Customer breakdown: {known_customers:,} known customers + 1 UNKNOWN_CUSTOMER")

    # Sample customers by country
    sample_customers = conn.execute("""
        SELECT country, COUNT(*) as customer_count
        FROM dim_customer 
        GROUP BY country 
        ORDER BY customer_count DESC 
        LIMIT 5
    """).fetchall()

    logger.info("Top countries by customer count:")
    for country, count in sample_customers:
        logger.info(f"  {country}: {count:,} customers")
