# DuckDB Retail Analytics Pipeline

A production-ready ETL pipeline that transforms retail transaction data into a dimensional data warehouse using DuckDB and Python. Built for business intelligence and analytics with FX enrichment, customer segmentation, and automated reporting.

## 🎯 Overview

This pipeline demonstrates modern data engineering practices by processing retail transactions through a complete ETL workflow:
- **Data Ingestion**: Clean ingestion of Excel, XML, and XLS files
- **Dimensional Modeling**: Star schema with fact/dimension tables
- **FX Enrichment**: Currency conversion using ECB exchange rates
- **Business Intelligence**: Pre-built SQL views and EUR-focused analytics
- **Data Quality**: Comprehensive validation and error handling

## 📊 Data Architecture

```
Raw Data → Staging → Dimensions → Facts → Aggregations → Views → Analytics
```

The pipeline processes retail transactions through these layers:
1. **Staging**: Clean and validate raw data files
2. **Dimensions**: Products, customers, countries, calendar with business logic
3. **Facts**: Transaction records with proper referential integrity
4. **Aggregations**: Pre-computed daily metrics by country for performance
5. **Views**: Monthly summaries for analytics
6. **Analytics**: EUR-focused dashboards and business insights

## 🚀 Quick Start

### Prerequisites
- Python 3.13+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager
- Git LFS (for large Excel files). 

### Setup Environment
```bash
# Clone repository
git clone duckdb-retail-pipeline
cd duckdb-retail-pipeline

# Pull large data files
git lfs pull

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\Activate.ps1

# Install dependencies
uv sync
```

### Run the Pipeline
```bash
# Full pipeline rebuild (recommended first run)
python src/run.py --rawdir data/raw --db build/retail.duckdb --rebuild

# Incremental run (preserves existing data)
python src/run.py --rawdir data/raw --db build/retail.duckdb
```

### Generate Analytics
```bash
# Create EUR-focused business intelligence dashboard
python analysis/analyze_monthly_sales.py
```

## 📁 Project Structure

```
duckdb-retail-pipeline/
├── src/                          # Core ETL pipeline
│   ├── run.py                    # Main orchestrator
│   ├── ingestion/                # Data ingestion modules
│   │   ├── retail_data.py        # Excel transaction data
│   │   ├── fx_data.py            # ECB XML exchange rates
│   │   └── holidays_data.py      # UK bank holidays
│   └── models/                   # Data modeling
│       ├── dimensions.py         # Dimension table creation
│       └── facts.py              # Fact tables and aggregations
├── sql/                          # Business intelligence layer
│   └── views/                    # Reusable SQL views
│       └── monthly_sales_summary.sql
├── analysis/                     # Analytics and visualization
│   ├── analyze_monthly_sales.py  # EUR-focused BI dashboard
│   └── images/                   # Generated visualizations
├── data_exploration/             # Ad-hoc data exploration scripts
├── data/raw/                     # Source data files
├── build/                        # Generated database
└── pyproject.toml               # Dependencies and configuration
```

## 📊 Expected Outputs

### Database Tables
After running the pipeline, `build/retail.duckdb` contains:

**Dimensions:**
- `dim_product` (5.3K products) - Product catalog with lifecycle
- `dim_customer` (5.9K customers) - Customer geography  
- `dim_calendar` (761 dates) - Business calendar with UK holidays

**Facts:**
- `fct_sales` (1.07M line items) - Transaction-level data in GBP
- `fct_sales_eur` (1.07M line items) - EUR-converted transactions
- `daily_fx_rates` (739 rates) - Complete FX coverage

**Aggregations:**
- `agg_country_day` (3.7K records) - Daily metrics by country

**Views:**
- `v_monthly_sales_summary` - Monthly revenue trends by country

### Business Metrics
- **Total Revenue**: £19.3M (€22.3M)
- **Transaction Volume**: 1.07M line items across 53K invoices
- **Geographic Coverage**: 42 countries with UK dominance
- **Time Span**: 25 months (Dec 2009 - Dec 2011)
- **Data Quality**: >99.9% accuracy with comprehensive validation

### Analytics Dashboard
EUR-focused visualizations showing:
1. Monthly revenue trends
2. Country revenue distribution  
3. Order volume patterns
4. Average order values by market

## 🔍 Example Analytics Queries

### Monthly Revenue Trends
```sql
SELECT 
    year, month, country,
    total_revenue_eur,
    total_orders,
    avg_order_value_eur
FROM v_monthly_sales_summary
WHERE country = 'United Kingdom'
ORDER BY year, month;
```

### Weekend vs Weekday Performance
```sql
SELECT 
    is_weekend,
    COUNT(*) as days,
    SUM(net_revenue_eur) as total_revenue,
    AVG(net_revenue_eur) as avg_daily_revenue
FROM agg_country_day
GROUP BY is_weekend;
```

### Top Products by Revenue
```sql
SELECT 
    p.description,
    SUM(f.gross_amount_eur) as total_revenue_eur,
    COUNT(*) as line_items
FROM fct_sales_eur f
JOIN dim_product p ON f.stock_code = p.stock_code
WHERE f.qty > 0  -- Exclude returns
GROUP BY p.stock_code, p.description
ORDER BY total_revenue_eur DESC
LIMIT 10;
```

### Customer Geographic Analysis
```sql
SELECT 
    c.country,
    COUNT(DISTINCT f.customer_id) as customers,
    SUM(f.gross_amount_eur) as revenue_eur,
    AVG(f.gross_amount_eur) as avg_transaction_eur
FROM fct_sales_eur f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.qty > 0
GROUP BY c.country
ORDER BY revenue_eur DESC;
```

## 🛠️ Development

### Data Exploration
```bash
# Explore raw data structure
python data_exploration/online_retail_data.py
python data_exploration/gbp_data.py
python data_exploration/ukbankholidays_data.py

# Inspect generated database
python data_exploration/inspect_db.py
```

### Package Management
```bash
# Add new dependencies
uv add package_name

# Update all packages
uv sync

# Check for updates
uv lock --upgrade
```

## 🚨 Troubleshooting

### Common Issues
- **Git LFS**: If you have trouble downloading the data files you can find the Online Retail II dataset [here](https://archive.ics.uci.edu/dataset/502/online+retail+ii), the gbp.xml [here](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/eurofxref-graph-gbp.en.html) and the historical UK bank holidays [here](https://www.dmo.gov.uk/media/bfknrcrn/ukbankholidays-jul19.xls). 
- **Database locked**: Ensure no other processes are using the database
- **Memory errors**: Large datasets may require increased system memory
- **FX conversion errors**: Check ECB XML file format and date ranges
- **Import errors**: Verify virtual environment activation and dependencies

*Built with DuckDB, Python 3.13, and modern data engineering best practices.*