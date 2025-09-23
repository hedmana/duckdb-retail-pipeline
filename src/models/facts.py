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
        logger.info(
            f"  {date} | {invoice_no} | {stock_code} | Customer {customer_id} | {qty} × £{unit_price:.2f} = £{gross_amount:.2f}")

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

    integrity_issues = [(issue, count)
                        for issue, count in orphan_check if count > 0]

    if integrity_issues:
        logger.warning("Referential integrity issues found:")
        for issue, count in integrity_issues:
            logger.warning(f"  - {issue}: {count:,} records")
    else:
        logger.info(
            "Referential integrity validated - all foreign keys resolved")


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
    row_count = conn.execute(
        "SELECT COUNT(*) FROM daily_fx_rates").fetchone()[0]
    logger.info(f"Created daily_fx_rates with {row_count:,} daily rates")

    # Check coverage
    missing_dates = conn.execute("""
        SELECT COUNT(*) 
        FROM fct_sales f
        LEFT JOIN daily_fx_rates fx ON f.date = fx.date
        WHERE fx.date IS NULL
    """).fetchone()[0]

    if missing_dates > 0:
        logger.warning(
            f"Missing FX rates for {missing_dates:,} sales transactions")
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
    row_count = conn.execute(
        "SELECT COUNT(*) FROM fct_sales_eur").fetchone()[0]
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
        logger.info(
            f"  {date} | {invoice} | {stock} | {qty} × £{price_gbp:.2f} (€{price_eur:.2f}) = £{amount_gbp:.2f} (€{amount_eur:.2f}) @ {fx_rate:.4f}")

    # Validation: Check for any conversion errors
    conversion_errors = conn.execute("""
        SELECT COUNT(*) 
        FROM fct_sales_eur 
        WHERE unit_price_eur IS NULL 
           OR gross_amount_eur IS NULL 
           OR fx_rate_used IS NULL
    """).fetchone()[0]

    if conversion_errors > 0:
        logger.warning(
            f"Found {conversion_errors:,} conversion errors (NULL values)")
    else:
        logger.info("All currency conversions completed successfully")


def create_agg_country_day(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create daily country aggregation table for analytics.
    
    Schema: agg_country_day (
        date, country, orders, items, net_qty, 
        net_revenue_gbp, net_revenue_eur,
        is_weekend, is_uk_holiday
    )
    
    Business rules:
    - orders = count(distinct invoice_no) where invoice_no NOT LIKE 'C%'
    - items = count(*) of line items (post-dedupe)
    - net_qty = sum(qty) (returns are negative)
    - net_revenue = sum(qty * unit_price) after returns
    - Include calendar context (weekend/holiday flags)
    """
    logger.info("Creating agg_country_day analytics table")

    conn.execute("DROP TABLE IF EXISTS agg_country_day")

    # Create daily country aggregation with calendar context
    conn.execute("""
        CREATE TABLE agg_country_day AS
        SELECT 
            f.date,
            cu.country,
            -- Orders: distinct invoices, excluding cancellations (C-prefix)
            COUNT(DISTINCT CASE 
                WHEN f.invoice_no NOT LIKE 'C%' THEN f.invoice_no 
                ELSE NULL 
            END) as orders,
            -- Items: total line items count
            COUNT(*) as items,
            -- Net quantities (returns are negative)
            SUM(f.qty) as net_qty,
            -- Net revenue in both currencies
            SUM(f.gross_amount_gbp) as net_revenue_gbp,
            SUM(fe.gross_amount_eur) as net_revenue_eur,
            -- Calendar context
            c.is_weekend,
            c.is_uk_holiday,
            c.iso_week,
            c.iso_year,
            c.month,
            c.year
        FROM fct_sales f
        INNER JOIN fct_sales_eur fe ON (
            f.invoice_no = fe.invoice_no 
            AND f.stock_code = fe.stock_code 
            AND f.date = fe.date
            AND f.customer_id = fe.customer_id
        )
        INNER JOIN dim_customer cu ON f.customer_id = cu.customer_id
        INNER JOIN dim_calendar c ON f.date = c.date
        GROUP BY 
            f.date, 
            cu.country, 
            c.is_weekend, 
            c.is_uk_holiday,
            c.iso_week,
            c.iso_year,
            c.month,
            c.year
        ORDER BY f.date, cu.country
    """)

    # Verify and log statistics
    row_count = conn.execute(
        "SELECT COUNT(*) FROM agg_country_day").fetchone()[0]
    logger.info(
        f"Created agg_country_day with {row_count:,} country-day combinations")

    # Summary statistics
    summary_stats = conn.execute("""
        SELECT 
            COUNT(DISTINCT date) as unique_dates,
            COUNT(DISTINCT country) as unique_countries,
            SUM(orders) as total_orders,
            SUM(items) as total_items,
            SUM(net_qty) as total_net_qty,
            SUM(net_revenue_gbp) as total_net_revenue_gbp,
            SUM(net_revenue_eur) as total_net_revenue_eur
        FROM agg_country_day
    """).fetchone()

    unique_dates, unique_countries, total_orders, total_items, total_net_qty, total_net_gbp, total_net_eur = summary_stats

    logger.info("Analytics table statistics:")
    logger.info(f"  - Date range: {unique_dates:,} unique dates")
    logger.info(f"  - Countries: {unique_countries:,} countries")
    logger.info(f"  - Total orders: {total_orders:,}")
    logger.info(f"  - Total line items: {total_items:,}")
    logger.info(f"  - Net quantity: {total_net_qty:,}")
    logger.info(
        f"  - Net revenue: £{total_net_gbp:,.2f} (€{total_net_eur:,.2f})")

    # Top countries by revenue
    top_countries = conn.execute("""
        SELECT 
            country,
            SUM(orders) as total_orders,
            SUM(net_revenue_gbp) as total_revenue_gbp,
            SUM(net_revenue_eur) as total_revenue_eur
        FROM agg_country_day
        GROUP BY country
        ORDER BY total_revenue_gbp DESC
        LIMIT 5
    """).fetchall()

    logger.info("Top countries by revenue:")
    for country, orders, revenue_gbp, revenue_eur in top_countries:
        logger.info(
            f"  {country}: {orders:,} orders, £{revenue_gbp:,.2f} (€{revenue_eur:,.2f})")

    # Weekend vs weekday performance
    weekend_analysis = conn.execute("""
        SELECT 
            is_weekend,
            COUNT(*) as days,
            SUM(orders) as total_orders,
            SUM(net_revenue_gbp) as total_revenue_gbp,
            AVG(net_revenue_gbp) as avg_daily_revenue_gbp
        FROM agg_country_day
        GROUP BY is_weekend
        ORDER BY is_weekend
    """).fetchall()

    logger.info("Weekend vs weekday analysis:")
    for is_weekend, days, orders, total_revenue, avg_revenue in weekend_analysis:
        day_type = "Weekend" if is_weekend else "Weekday"
        logger.info(
            f"  {day_type}: {days:,} days, {orders:,} orders, £{avg_revenue:,.2f} avg daily revenue")

    # Holiday impact analysis
    holiday_analysis = conn.execute("""
        SELECT 
            is_uk_holiday,
            COUNT(*) as days,
            SUM(orders) as total_orders,
            SUM(net_revenue_gbp) as total_revenue_gbp,
            AVG(net_revenue_gbp) as avg_daily_revenue_gbp
        FROM agg_country_day
        GROUP BY is_uk_holiday
        ORDER BY is_uk_holiday
    """).fetchall()

    logger.info("Holiday vs normal day analysis:")
    for is_holiday, days, orders, total_revenue, avg_revenue in holiday_analysis:
        day_type = "UK Holiday" if is_holiday else "Normal Day"
        logger.info(
            f"  {day_type}: {days:,} days, {orders:,} orders, £{avg_revenue:,.2f} avg daily revenue")

    # Sample daily data
    sample_data = conn.execute("""
        SELECT 
            date, country, orders, items, net_qty, 
            net_revenue_gbp, net_revenue_eur,
            is_weekend, is_uk_holiday
        FROM agg_country_day
        ORDER BY net_revenue_gbp DESC
        LIMIT 5
    """).fetchall()

    logger.info("Sample high-revenue country-days:")
    for date, country, orders, items, qty, rev_gbp, rev_eur, weekend, holiday in sample_data:
        flags = []
        if weekend:
            flags.append("Weekend")
        if holiday:
            flags.append("Holiday")
        flag_str = f" ({', '.join(flags)})" if flags else ""
        logger.info(
            f"  {date} | {country} | {orders} orders, {items} items, {qty} qty | £{rev_gbp:,.2f} (€{rev_eur:,.2f}){flag_str}")

    # Data quality validation
    validation_checks = conn.execute("""
        SELECT 
            'Negative orders' as check_name,
            COUNT(*) as violations
        FROM agg_country_day 
        WHERE orders < 0
        
        UNION ALL
        
        SELECT 
            'Negative items' as check_name,
            COUNT(*) as violations
        FROM agg_country_day 
        WHERE items < 0
        
        UNION ALL
        
        SELECT 
            'Revenue mismatch' as check_name,
            COUNT(*) as violations
        FROM agg_country_day 
        WHERE ABS(net_revenue_eur * 0.8654 - net_revenue_gbp) / NULLIF(net_revenue_gbp, 0) > 0.1
    """).fetchall()

    quality_issues = [(check, violations)
                      for check, violations in validation_checks if violations > 0]

    if quality_issues:
        logger.warning("Data quality issues found:")
        for check, violations in quality_issues:
            logger.warning(f"  - {check}: {violations:,} violations")
    else:
        logger.info("All data quality checks passed")
