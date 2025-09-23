"""Fact table creation for sales transactions and currency conversions."""

import logging

import duckdb

logger = logging.getLogger(__name__)


def create_fct_sales(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create sales fact table with one row per line item.
    
    Schema: fct_sales (
        invoice_no, stock_code, customer_id, date,
        qty, unit_price_gbp, gross_amount_gbp
    )
    
    Business rules:
    - One row per invoice line item (no aggregation)
    - Include cancellations (negative quantities)
    - Join to dimension keys for referential integrity
    - Calculate gross_amount_gbp = qty * unit_price_gbp
    """
    logger.info("Creating fct_sales table")
    
    conn.execute("DROP TABLE IF EXISTS fct_sales")
    
    # Create sales fact table with proper joins to dimensions
    conn.execute("""
        CREATE TABLE fct_sales AS
        SELECT 
            r.invoice_no,
            r.stock_code,
            COALESCE(r.customer_id, -1) as customer_id,  -- Use UNKNOWN_CUSTOMER surrogate
            DATE(r.invoice_ts) as date,
            r.qty,
            r.unit_price_gbp,
            r.qty * r.unit_price_gbp as gross_amount_gbp
        FROM raw_retail_data r
        INNER JOIN dim_calendar c ON DATE(r.invoice_ts) = c.date
        INNER JOIN dim_product p ON r.stock_code = p.stock_code
        INNER JOIN dim_customer cu ON COALESCE(r.customer_id, -1) = cu.customer_id
        WHERE r.stock_code IS NOT NULL 
          AND r.stock_code != ''
          AND r.stock_code != 'nan'
          AND r.unit_price_gbp IS NOT NULL
          AND r.qty IS NOT NULL
        ORDER BY r.invoice_ts, r.invoice_no, r.stock_code
    """)
    
    # Verify and log statistics
    row_count = conn.execute("SELECT COUNT(*) FROM fct_sales").fetchone()[0]
    logger.info(f"Created fct_sales with {row_count:,} line items")
    
    # Check for cancellations (negative quantities)
    cancellation_count = conn.execute("""
        SELECT COUNT(*) FROM fct_sales WHERE qty < 0
    """).fetchone()[0]
    
    # Calculate total sales metrics
    sales_summary = conn.execute("""
        SELECT 
            COUNT(*) as total_line_items,
            COUNT(DISTINCT invoice_no) as unique_invoices,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT stock_code) as unique_products,
            SUM(CASE WHEN qty > 0 THEN qty * unit_price_gbp ELSE 0 END) as total_sales_gbp,
            SUM(CASE WHEN qty < 0 THEN qty * unit_price_gbp ELSE 0 END) as total_returns_gbp,
            SUM(qty * unit_price_gbp) as net_sales_gbp
        FROM fct_sales
    """).fetchone()
    
    total_items, unique_invoices, unique_customers, unique_products, sales_gbp, returns_gbp, net_sales_gbp = sales_summary
    
    logger.info("Sales fact table statistics:")
    logger.info(f"  - Total line items: {total_items:,}")
    logger.info(f"  - Unique invoices: {unique_invoices:,}")
    logger.info(f"  - Unique customers: {unique_customers:,}")
    logger.info(f"  - Unique products: {unique_products:,}")
    logger.info(f"  - Cancellations: {cancellation_count:,} line items")
    logger.info(f"  - Gross sales: £{sales_gbp:,.2f}")
    logger.info(f"  - Returns: £{returns_gbp:,.2f}")
    logger.info(f"  - Net sales: £{net_sales_gbp:,.2f}")
    
    # Check date range
    date_range = conn.execute("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM fct_sales
    """).fetchone()
    logger.info(f"  - Date range: {date_range[0]} to {date_range[1]}")
    
    # Sample transactions
    sample_sales = conn.execute("""
        SELECT invoice_no, stock_code, customer_id, date, qty, unit_price_gbp, gross_amount_gbp
        FROM fct_sales 
        ORDER BY date, invoice_no
        LIMIT 5
    """).fetchall()
    
    logger.info("Sample sales transactions:")
    for invoice_no, stock_code, customer_id, date, qty, unit_price, gross_amount in sample_sales:
        logger.info(f"  {date} | {invoice_no} | {stock_code} | Customer {customer_id} | {qty} × £{unit_price:.2f} = £{gross_amount:.2f}")
    
    # Validate referential integrity
    orphan_check = conn.execute("""
        SELECT 
            'Missing calendar' as issue,
            COUNT(*) as count
        FROM fct_sales f
        LEFT JOIN dim_calendar c ON f.date = c.date
        WHERE c.date IS NULL
        
        UNION ALL
        
        SELECT 
            'Missing product' as issue,
            COUNT(*) as count
        FROM fct_sales f
        LEFT JOIN dim_product p ON f.stock_code = p.stock_code
        WHERE p.stock_code IS NULL
        
        UNION ALL
        
        SELECT 
            'Missing customer' as issue,
            COUNT(*) as count
        FROM fct_sales f
        LEFT JOIN dim_customer cu ON f.customer_id = cu.customer_id
        WHERE cu.customer_id IS NULL
    """).fetchall()
    
    integrity_issues = [(issue, count) for issue, count in orphan_check if count > 0]
    
    if integrity_issues:
        logger.warning("Referential integrity issues found:")
        for issue, count in integrity_issues:
            logger.warning(f"  - {issue}: {count:,} records")
    else:
        logger.info("Referential integrity validated - all foreign keys resolved")


def create_daily_fx_rates(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create daily FX rates table with forward-filled rates for missing dates.
    
    Schema: daily_fx_rates (date, gbp_per_eur)
    
    Business rules:
    - Forward-fill rates for weekends and holidays
    - Ensure complete date coverage for sales date range
    - Handle ECB rate format: GBP per 1 EUR
    """
    logger.info("Creating daily_fx_rates table")
    
    conn.execute("DROP TABLE IF EXISTS daily_fx_rates")
    
    # Get the date range from sales data
    sales_date_range = conn.execute("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM fct_sales
    """).fetchone()
    
    min_sales_date, max_sales_date = sales_date_range
    logger.info(f"Sales date range: {min_sales_date} to {max_sales_date}")
    
    # Create daily FX table with forward-filled rates
    conn.execute(f"""
        CREATE TABLE daily_fx_rates AS
        WITH date_series AS (
            SELECT unnest(generate_series(
                DATE '{min_sales_date}', 
                DATE '{max_sales_date}', 
                INTERVAL '1 day'
            )) as date
        ),
        fx_with_forward_fill AS (
            SELECT 
                ds.date,
                -- Forward fill: use the most recent rate available up to this date
                LAST_VALUE(fx.gbp_per_eur IGNORE NULLS) OVER (
                    ORDER BY ds.date 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as gbp_per_eur
            FROM date_series ds
            LEFT JOIN raw_fx_rates fx ON ds.date = fx.date
        )
        SELECT date, gbp_per_eur
        FROM fx_with_forward_fill
        WHERE gbp_per_eur IS NOT NULL  -- Remove any dates before first FX rate
        ORDER BY date
    """)
    
    # Verify and log statistics
    row_count = conn.execute("SELECT COUNT(*) FROM daily_fx_rates").fetchone()[0]
    logger.info(f"Created daily_fx_rates with {row_count:,} daily rates")
    
    # Check coverage
    missing_dates = conn.execute("""
        SELECT COUNT(*) 
        FROM fct_sales f
        LEFT JOIN daily_fx_rates fx ON f.date = fx.date
        WHERE fx.date IS NULL
    """).fetchone()[0]
    
    if missing_dates > 0:
        logger.warning(f"Missing FX rates for {missing_dates:,} sales transactions")
    else:
        logger.info("Complete FX rate coverage for all sales dates")
    
    # Rate statistics
    rate_stats = conn.execute("""
        SELECT 
            MIN(gbp_per_eur) as min_rate,
            MAX(gbp_per_eur) as max_rate,
            AVG(gbp_per_eur) as avg_rate,
            COUNT(DISTINCT gbp_per_eur) as unique_rates
        FROM daily_fx_rates
    """).fetchone()
    
    min_rate, max_rate, avg_rate, unique_rates = rate_stats
    logger.info(f"FX rate statistics:")
    logger.info(f"  - Min rate: {min_rate:.4f} GBP/EUR")
    logger.info(f"  - Max rate: {max_rate:.4f} GBP/EUR")
    logger.info(f"  - Avg rate: {avg_rate:.4f} GBP/EUR")
    logger.info(f"  - Unique rates: {unique_rates:,}")
    
    # Sample rates
    sample_rates = conn.execute("""
        SELECT date, gbp_per_eur
        FROM daily_fx_rates 
        ORDER BY date 
        LIMIT 5
    """).fetchall()
    
    logger.info("Sample FX rates:")
    for date, rate in sample_rates:
        logger.info(f"  {date}: {rate:.4f} GBP/EUR")


def create_fct_sales_eur(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create EUR-converted sales fact table.
    
    Schema: fct_sales_eur (
        invoice_no, stock_code, customer_id, date,
        qty, unit_price_gbp, unit_price_eur, gross_amount_gbp, gross_amount_eur
    )
    
    Business rules:
    - Convert using: amount_eur = amount_gbp / rate_gbp_per_eur
    - Include both GBP and EUR amounts for comparison
    - Join with daily FX rates table
    """
    logger.info("Creating fct_sales_eur table")
    
    conn.execute("DROP TABLE IF EXISTS fct_sales_eur")
    
    # Create EUR fact table with currency conversion
    conn.execute("""
        CREATE TABLE fct_sales_eur AS
        SELECT 
            f.invoice_no,
            f.stock_code,
            f.customer_id,
            f.date,
            f.qty,
            f.unit_price_gbp,
            f.unit_price_gbp / fx.gbp_per_eur as unit_price_eur,
            f.gross_amount_gbp,
            f.gross_amount_gbp / fx.gbp_per_eur as gross_amount_eur,
            fx.gbp_per_eur as fx_rate_used
        FROM fct_sales f
        INNER JOIN daily_fx_rates fx ON f.date = fx.date
        ORDER BY f.date, f.invoice_no, f.stock_code
    """)
    
    # Verify and log statistics
    row_count = conn.execute("SELECT COUNT(*) FROM fct_sales_eur").fetchone()[0]
    logger.info(f"Created fct_sales_eur with {row_count:,} line items")
    
    # Compare GBP vs EUR totals
    currency_comparison = conn.execute("""
        SELECT 
            SUM(CASE WHEN qty > 0 THEN gross_amount_gbp ELSE 0 END) as sales_gbp,
            SUM(CASE WHEN qty > 0 THEN gross_amount_eur ELSE 0 END) as sales_eur,
            SUM(CASE WHEN qty < 0 THEN gross_amount_gbp ELSE 0 END) as returns_gbp,
            SUM(CASE WHEN qty < 0 THEN gross_amount_eur ELSE 0 END) as returns_eur,
            SUM(gross_amount_gbp) as net_sales_gbp,
            SUM(gross_amount_eur) as net_sales_eur
        FROM fct_sales_eur
    """).fetchone()
    
    sales_gbp, sales_eur, returns_gbp, returns_eur, net_gbp, net_eur = currency_comparison
    
    logger.info("EUR conversion statistics:")
    logger.info(f"  - Gross sales: £{sales_gbp:,.2f} = €{sales_eur:,.2f}")
    logger.info(f"  - Returns: £{returns_gbp:,.2f} = €{returns_eur:,.2f}")
    logger.info(f"  - Net sales: £{net_gbp:,.2f} = €{net_eur:,.2f}")
    
    # Sample conversions
    sample_conversions = conn.execute("""
        SELECT 
            date, invoice_no, stock_code, qty,
            unit_price_gbp, unit_price_eur, 
            gross_amount_gbp, gross_amount_eur,
            fx_rate_used
        FROM fct_sales_eur 
        ORDER BY date, invoice_no
        LIMIT 5
    """).fetchall()
    
    logger.info("Sample EUR conversions:")
    for date, invoice, stock, qty, price_gbp, price_eur, amount_gbp, amount_eur, fx_rate in sample_conversions:
        logger.info(f"  {date} | {invoice} | {stock} | {qty} × £{price_gbp:.2f} (€{price_eur:.2f}) = £{amount_gbp:.2f} (€{amount_eur:.2f}) @ {fx_rate:.4f}")

    # Validation: Check for any conversion errors
    conversion_errors = conn.execute("""
        SELECT COUNT(*) 
        FROM fct_sales_eur 
        WHERE unit_price_eur IS NULL 
           OR gross_amount_eur IS NULL 
           OR fx_rate_used IS NULL
    """).fetchone()[0]
    
    if conversion_errors > 0:
        logger.warning(f"Found {conversion_errors:,} conversion errors (NULL values)")
    else:
        logger.info("All currency conversions completed successfully")