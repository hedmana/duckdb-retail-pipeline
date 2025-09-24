#!/usr/bin/env python3
"""
Create SQL views and generate visualizations for the retail data warehouse.

This script demonstrates how to:
1. Create business intelligence views in DuckDB
2. Query the views for analysis
3. Create meaningful visualizations

Focus: Monthly Sales Summary View
"""

import logging
from pathlib import Path
import matplotlib.pyplot as plt
import duckdb
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_monthly_sales_view(conn):
    """Create the monthly sales summary view."""
    logger.info("Creating monthly sales summary view...")

    # Read the SQL file
    sql_file = Path(__file__).parent.parent / "sql" / \
        "views" / "monthly_sales_summary.sql"
    with open(sql_file, 'r') as f:
        view_sql = f.read()

    # Execute the view creation
    conn.execute(view_sql)
    logger.info("Monthly sales summary view created successfully")

    # Test the view
    sample_data = conn.execute(
        "SELECT * FROM v_monthly_sales_summary LIMIT 5").fetchdf()
    logger.info(
        f"View contains {len(conn.execute('SELECT * FROM v_monthly_sales_summary').fetchdf())} rows")
    logger.info("Sample data:")
    for _, row in sample_data.iterrows():
        logger.info(
            f"  {row['year']}-{row['month']:02d} | {row['country']} | £{row['total_revenue_gbp']:,.0f} ({row['total_orders']} orders)")


def visualize_monthly_trends(conn):
    """Create visualizations from the monthly sales view."""
    logger.info("Creating monthly sales visualizations...")

    # Query the view
    df = conn.execute("""
        SELECT 
            year,
            month,
            month_start_date,
            country,
            total_revenue_gbp,
            total_revenue_eur,
            total_orders,
            ROUND(total_revenue_eur / NULLIF(total_orders, 0), 2) as avg_order_value_eur
        FROM v_monthly_sales_summary
        ORDER BY year, month, total_revenue_eur DESC
    """).fetchdf()

    # Create a figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Monthly Sales Summary Dashboard (EUR)',
                 fontsize=16, fontweight='bold')

    # 1. Total Revenue Trend (Top 5 Countries) - EUR
    top_countries = df.groupby(
        'country')['total_revenue_eur'].sum().nlargest(5).index
    top_countries_data = df[df['country'].isin(top_countries)]

    for country in top_countries:
        country_data = top_countries_data[top_countries_data['country'] == country]
        country_data = country_data.sort_values(['year', 'month'])
        axes[0, 0].plot(range(len(country_data)), country_data['total_revenue_eur'],
                        marker='o', label=country, linewidth=2)

    axes[0, 0].set_title('Monthly Revenue Trends (Top 5 Countries)')
    axes[0, 0].set_xlabel('Month')
    axes[0, 0].set_ylabel('Revenue (EUR)')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # 2. Country Revenue Distribution
    country_totals = df.groupby('country')['total_revenue_eur'].sum(
    ).sort_values(ascending=False).head(8)
    axes[0, 1].barh(country_totals.index, country_totals.values)
    axes[0, 1].set_title('Total Revenue by Country')
    axes[0, 1].set_xlabel('Revenue (EUR)')

    # Format revenue labels
    for i, v in enumerate(country_totals.values):
        axes[0, 1].text(v + max(country_totals.values) * 0.01, i, f'€{v:,.0f}',
                        va='center', fontsize=9)

    # 3. Monthly Order Volume
    monthly_orders = df.groupby(['year', 'month'])[
        'total_orders'].sum().reset_index()
    monthly_orders['period'] = monthly_orders['year'].astype(
        str) + '-' + monthly_orders['month'].astype(str).str.zfill(2)

    axes[1, 0].bar(range(len(monthly_orders)), monthly_orders['total_orders'])
    axes[1, 0].set_title('Monthly Order Volume')
    axes[1, 0].set_xlabel('Month')
    axes[1, 0].set_ylabel('Total Orders')
    axes[1, 0].set_xticks(range(len(monthly_orders)))
    axes[1, 0].set_xticklabels(monthly_orders['period'], rotation=45)

    # 4. Average Order Value by Country (Top 10) - EUR
    avg_order_value_eur = df.groupby('country').agg({
        'total_revenue_eur': 'sum',
        'total_orders': 'sum'
    }).reset_index()
    avg_order_value_eur['avg_order_value'] = avg_order_value_eur['total_revenue_eur'] / \
        avg_order_value_eur['total_orders']
    avg_order_value_eur = avg_order_value_eur.sort_values(
        'avg_order_value', ascending=False).head(10)

    axes[1, 1].bar(range(len(avg_order_value_eur)),
                   avg_order_value_eur['avg_order_value'])
    axes[1, 1].set_title('Average Order Value by Country')
    axes[1, 1].set_xlabel('Country')
    axes[1, 1].set_ylabel('Avg Order Value (EUR)')
    axes[1, 1].set_xticks(range(len(avg_order_value_eur)))
    axes[1, 1].set_xticklabels(
        avg_order_value_eur['country'], rotation=45, ha='right')

    plt.tight_layout()

    # Save the visualization
    output_path = Path(__file__).parent / "images" / \
        "monthly_sales_dashboard.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    logger.info(f"Dashboard saved to: {output_path}")

    # Show some key insights
    logger.info("\nKEY INSIGHTS FROM MONTHLY SALES:")

    total_revenue_gbp = df['total_revenue_gbp'].sum()
    total_revenue_eur = df['total_revenue_eur'].sum()
    total_orders = df['total_orders'].sum()
    top_country = df.groupby('country')['total_revenue_gbp'].sum().idxmax()
    best_month = df.groupby(['year', 'month'])[
        'total_revenue_gbp'].sum().idxmax()

    logger.info(
        f"Total Revenue: £{total_revenue_gbp:,.0f} (€{total_revenue_eur:,.0f})")
    logger.info(f"Total Orders: {total_orders:,}")
    logger.info(f"Top Country: {top_country}")
    logger.info(f"Best Month: {best_month[0]}-{best_month[1]:02d}")

    # Growth analysis
    monthly_totals = df.groupby(['year', 'month'])[
        'total_revenue_eur'].sum().reset_index()
    if len(monthly_totals) > 1:
        first_month = monthly_totals.iloc[0]['total_revenue_eur']
        last_month = monthly_totals.iloc[-1]['total_revenue_eur']
        growth = ((last_month - first_month) / first_month) * 100
        logger.info(f"Revenue Growth: {growth:+.1f}% from first to last month")

    return df


def main():
    """Main function to create view and generate visualizations."""
    # Connect to database
    db_path = Path(__file__).parent.parent / "build" / "retail.duckdb"
    if not db_path.exists():
        logger.error(
            "Database not found. Run the pipeline first: python src/run.py --rebuild")
        return

    conn = duckdb.connect(str(db_path))
    logger.info(f"Connected to database: {db_path}")

    try:
        # Create the view
        create_monthly_sales_view(conn)

        # Generate visualizations
        df = visualize_monthly_trends(conn)

        logger.info("Monthly sales analysis completed successfully!")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
