-- Monthly Sales Summary View
-- Aggregates daily sales data into monthly totals by country
-- Shows revenue trends, order patterns, and growth metrics

CREATE OR REPLACE VIEW v_monthly_sales_summary AS
SELECT 
    -- Time dimensions
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    DATE_TRUNC('month', date) as month_start_date,
    
    -- Geographic dimension
    country,
    
    -- Core metrics
    COUNT(DISTINCT date) as trading_days,
    SUM(orders) as total_orders,
    SUM(items) as total_items,
    SUM(net_qty) as total_quantity,
    
    -- Revenue metrics (both currencies)
    SUM(net_revenue_gbp) as total_revenue_gbp,
    SUM(net_revenue_eur) as total_revenue_eur,
    
    -- Calculated metrics
    ROUND(SUM(net_revenue_gbp) / NULLIF(COUNT(DISTINCT date), 0), 2) as avg_daily_revenue_gbp,
    ROUND(SUM(orders) / NULLIF(COUNT(DISTINCT date), 0), 2) as avg_daily_orders,
    ROUND(SUM(net_revenue_gbp) / NULLIF(SUM(orders), 0), 2) as avg_order_value_gbp

FROM agg_country_day
WHERE net_revenue_gbp > 0  -- Exclude days with only returns/cancellations
GROUP BY 
    EXTRACT(YEAR FROM date),
    EXTRACT(MONTH FROM date), 
    DATE_TRUNC('month', date),
    country
ORDER BY 
    year, 
    month, 
    total_revenue_gbp DESC;