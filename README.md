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
5. **Views**: Business-ready monthly summaries for analytics
6. **Analytics**: EUR-focused dashboards and business insights

## 🚀 Quick Start

### Prerequisites
- Python 3.13
- uv package manager
- Git LFS (for large Excel files)

### Setup Environment
```bash
# Clone repository
git clone <your-repo>
cd duckdb-retail-pipeline

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

## 🔧 Key Features

### Data Processing
- **Robust ETL**: Comprehensive error handling and logging
- **Deduplication**: Business key-based duplicate removal
- **Data Quality**: Validation of PKs, FKs, data types, and business rules
- **Incremental Processing**: Smart rebuilds and efficient updates

### Dimensional Modeling
- **Star Schema**: Proper fact/dimension design with surrogate keys
- **SCD Type 1**: Slowly changing dimensions with lifecycle tracking
- **Calendar Logic**: Weekend, holiday, and ISO week calculations
- **Unknown Members**: Proper handling of NULL customer IDs

### Currency Enrichment
- **ECB Integration**: Real EUR/GBP exchange rates from European Central Bank
- **Forward Filling**: Complete date coverage with missing rate interpolation
- **Dual Currency**: Both GBP and EUR fact tables for international analysis
- **FX Validation**: Sanity checks on currency conversion accuracy

### Business Intelligence
- **SQL Views**: Clean, reusable business logic layer
- **EUR Focus**: International market analysis (perfect for Finnish stakeholders)
- **Performance**: Pre-aggregated tables for fast query response
- **Visualization**: Professional matplotlib dashboards with business insights

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
1. Monthly revenue trends (top 5 countries)
2. Country revenue distribution  
3. Order volume patterns
4. Average order values by market

## 🏗️ Modeling Choices

### Returns and Cancellations
- **Negative Quantities**: Preserved to maintain audit trail
- **Cancellation Invoices**: C-prefixed invoices excluded from order counts
- **Net Calculations**: Revenue and quantities net to zero for complete returns
- **Line Item Granularity**: One fact record per invoice line item

### Foreign Key Strategy
- **Surrogate Keys**: Natural business keys used for joins
- **Unknown Members**: UNKNOWN_CUSTOMER (-1) for NULL customer IDs
- **Referential Integrity**: All fact records resolve to valid dimensions
- **Orphan Prevention**: Validation prevents broken references

### FX Join Policy
- **Daily Rates**: Forward-filled ECB rates for complete coverage
- **Conversion Logic**: `amount_eur = amount_gbp / rate_gbp_per_eur`
- **Missing Dates**: Weekend/holiday rates interpolated from last trading day
- **Validation**: Currency conversion accuracy verified (tolerance: 1e-6)

### Calendar Logic
- **Europe/London Timezone**: UK business context for date calculations
- **ISO Standards**: ISO week and year for international reporting
- **Holiday Integration**: UK bank holiday flags for business analysis
- **Weekend Logic**: Saturday/Sunday identification for performance analysis

## ✅ Data Quality Rules

The pipeline validates:
- **No NULL Primary Keys**: All dimension keys properly populated
- **Referential Integrity**: All foreign keys resolve to valid dimensions
- **Data Types**: Proper numeric, date, and text formatting
- **Business Logic**: No negative unit prices, integer quantities enforced
- **Deduplication**: No duplicate line items (business key uniqueness)
- **FX Accuracy**: Currency conversions within acceptable tolerance
- **Calendar Coverage**: Complete date range with holiday/weekend flags

Failures surface through:
- **Exception Handling**: Clear error messages with context
- **Comprehensive Logging**: Detailed processing status and statistics
- **Validation Checks**: Automated quality gates at each pipeline phase
- **Data Profiling**: Summary statistics and anomaly detection

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

### Adding New Analysis
```bash
# Create new analysis script
cp analysis/analyze_monthly_sales.py analysis/my_analysis.py

# Add SQL views as needed
echo "CREATE VIEW v_my_view AS SELECT ..." > sql/views/my_view.sql

# Run analysis
python analysis/my_analysis.py
```

### Data Exploration
```bash
# Explore raw data structure
python data_exploration/online_retail_data.py
python data_exploration/gbp_data.py

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
- **Database locked**: Ensure no other processes are using the database
- **Memory errors**: Large datasets may require increased system memory
- **FX conversion errors**: Check ECB XML file format and date ranges
- **Import errors**: Verify virtual environment activation and dependencies

### Debug Mode
```bash
# Enable detailed logging
export PYTHONPATH=src
python -m logging DEBUG src/run.py --rebuild
```

## 📈 Performance Characteristics

- **Processing Time**: ~3 minutes for 1M+ transactions
- **Memory Usage**: <2GB RAM for full dataset
- **Storage**: ~100MB database file
- **Scalability**: Designed for datasets up to 10M+ transactions

## 🎯 Business Value

This pipeline demonstrates:
- **Real-world Skills**: Production ETL patterns and data warehousing
- **International Scope**: Multi-currency analysis for global businesses  
- **Performance Engineering**: Optimized aggregations and indexing strategies
- **Data Quality**: Enterprise-grade validation and error handling
- **Business Intelligence**: Actionable insights and professional visualizations

Perfect for showcasing modern data engineering capabilities in interviews and portfolio demonstrations.

---

*Built with DuckDB, Python 3.13, and modern data engineering best practices.*