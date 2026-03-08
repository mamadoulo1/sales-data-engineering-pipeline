# Sales Data Engineering Pipeline

A production-style ETL pipeline built with Python and pandas that processes raw sales data through Extract → Transform → Load stages.

## Project Structure

```
.
├── config/
│   └── config.yaml          # All pipeline configuration
├── data/
│   ├── raw/                 # Source CSV files (input)
│   ├── processed/           # Parquet / CSV output
│   └── output/              # SQLite database & logs
├── src/
│   ├── extract/
│   │   └── extractor.py     # Reads raw CSV files
│   ├── transform/
│   │   └── transformer.py   # Cleans, joins & enriches data
│   ├── load/
│   │   └── loader.py        # Writes Parquet + SQLite
│   └── pipeline.py          # ETL orchestrator
├── tests/
│   ├── test_extractor.py
│   ├── test_transformer.py
│   └── test_loader.py
├── main.py                  # Entry point
└── requirements.txt
```

## Dataset

Three raw CSV source files:

| File | Description |
|---|---|
| `data/raw/orders.csv` | Order transactions (30 rows) |
| `data/raw/customers.csv` | Customer master data (12 rows) |
| `data/raw/products.csv` | Product catalog (12 rows) |

## Pipeline Stages

### Extract
Reads `orders.csv`, `customers.csv`, and `products.csv` into pandas DataFrames.

### Transform
1. **Clean orders** — drop duplicates, fill missing quantity, parse dates, validate status values
2. **Clean customers** — compute `full_name`, fill missing emails
3. **Clean products** — coerce numeric prices, drop invalid rows
4. **Join** — merge orders → customers → products
5. **Compute financials** — `gross_revenue`, `discount_amount`, `net_revenue`, `gross_profit`
6. **Aggregate** — build a `sales_summary` table grouped by month × category

### Load
- Writes each output table as **Parquet** (default) or **CSV** to `data/processed/`
- Persists all tables into a **SQLite** database at `data/output/sales.db`
- Appends structured logs to `data/output/pipeline.log`

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the pipeline
python main.py

# 3. Run tests
pytest tests/ -v
```

## Configuration

Edit `config/config.yaml` to change paths, valid order statuses, output format, or the revenue margin used for profit calculation.

```yaml
load:
  output_format: parquet   # Change to "csv" if parquet is not needed
  parquet_compression: snappy
```

## Output Tables

### `orders_enriched`
One row per order line, fully joined and enriched:

| Column | Description |
|---|---|
| `order_id` | Unique order identifier |
| `full_name` | Customer full name |
| `country` | Customer country |
| `name` | Product name |
| `category` | Product category |
| `gross_revenue` | quantity × unit_price |
| `net_revenue` | gross_revenue after discount |
| `gross_profit` | net_revenue minus estimated cost |
| `order_year_month` | Period label (e.g. `2024-01`) |

### `sales_summary`
Aggregated KPIs per month and category:

| Column | Description |
|---|---|
| `total_orders` | Number of non-cancelled orders |
| `net_revenue` | Total net revenue |
| `gross_profit` | Total gross profit |
| `profit_margin_pct` | Gross profit / net revenue × 100 |
