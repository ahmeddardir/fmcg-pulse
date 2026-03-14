# fmcg-pulse

A simulated FMCG retail analytics pipeline. Synthetic supermarket transaction data flows through a pipeline that cleans, transforms, and aggregates it into brand-level and category-level performance summaries.

---

## Architecture

```text
[Data Generation]
  └── Faker + random -> synthetic transactions CSV + product catalog JSON

[Ingestion Layer]
  └── Generator-based CSV reader (lazy, memory-efficient)
  └── JSON deserializer for product catalog

[Transformation Layer - Polars]
  └── Schema validation with type hints
  └── Joins: transactions <-> product catalog
  └── KPI computation: volume, revenue, market share %

[Orchestration - Prefect]
  └── Flow: ingest -> transform -> validate -> export
  └── Decorated tasks with retry + logging
  └── File handling

[Output]
  └── Parquet
  └── CSV summary reports
  └── JSON run manifest (audit log)
```

---

## Getting started

Set up your environment:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -e ".[dev]"
```

Then run the pipeline:

```bash
python -m fmcg_pulse
```

Outputs land in `data/output/`. Logs land in `logs/`.

---

## Configuration

All pipeline settings live in `config.yaml`.

```yaml
pipeline:
  market: "Germany"             # Market label used in run manifest

paths:
  raw_dir: "data/raw"           # Where generated data is written
  output_dir: "data/output"     # Where reports are written
  logs_dir: "logs"              # Where log files are written

logging:
  log_level: "DEBUG"            # DEBUG | INFO | WARNING | ERROR | CRITICAL
  standard_format: "..."        # Python logging format string (console + file)
  json_format: "..."            # Python logging format string (JSON file)

generation:
  n_transactions: 50000         # Number of synthetic transactions to generate
  n_products: 300               # Number of synthetic products in the catalog
  start_date: "2025-01-01"      # Transaction date range start
  end_date: "2025-12-31"        # Transaction date range end

quality:
  max_null_pct: 0.05            # Maximum acceptable null rate (5%)
  min_transactions: 100         # Minimum transactions required to pass validation
  min_price: 1.0                # Minimum valid unit price
  max_price: 2000.0             # Maximum valid unit price

reporting:
  reports:
    - name: "q1_monthly_category_performance"
      dimensions: ["category", "sub_category"]
      partition_by: "category"                      # Share computed within category
      time_grain: "month"                           # Group by month
      filters:
        date_from: "2025-01-01"                     # Inclusive start date
        date_to: "2025-03-31"                       # Inclusive end date
    - name: "overall_category_performance"
      dimensions: ["category", "sub_category"]
      partition_by: "category"                      # Share computed within category
    - name: "brand_performance"
      dimensions: ["category", "manufacturer", "brand"]
      partition_by: ["category", "manufacturer"]    # Share within manufacturer per category
      time_grain: "month"                           # Group by month
```

Reports are dimension-agnostic. Add a new entry under `reporting.reports` with any combination of product dimensions and an optional `partition_by` to control how market share is partitioned.

---

## Outputs

**`q1_monthly_category_performance.csv`** - revenue, volume, weighted average selling price, and within-category market share grouped by category and sub-category, broken down by month for Q1 2025.

**`overall_category_performance.csv`** - revenue, volume, weighted average selling price, and within-category market share grouped by category and sub-category across the full dataset.

**`brand_performance.csv`** - revenue, volume, weighted average selling price, and within-manufacturer market share grouped by category, manufacturer, and brand, broken down by month.

**`run_manifest.json`** - audit log for each pipeline run: timestamps, transaction counts, rejection rate, and quality check results.

---

## Design notes

**Config validation at startup.** Bad configuration (e.g. `min_price >= max_price`, invalid date range) raises immediately before the pipeline runs.

**Reports are fully defined by config.** Each report defines its own `dimensions`, `partition_by`, `time_grain`, and `filters`. The transformation layer reads these and builds Polars aggregations dynamically. Adding a new report requires only a new entry in `config.yaml`.

**`partition_by` controls market share partitioning.** `market_share_pct` is computed as a window function partitioned by `partition_by`. If omitted, share is computed globally. `time_grain` is automatically prepended to `partition_by` at transform time so share is always computed within the correct period.

**`avg_selling_price` uses weighted mean.** Computed as `sum(unit_price * quantity) / sum(quantity)` within each group, reflecting what consumers actually paid on average across all units.

**Row-level validation in bulk.** Data quality checks run in `validators.py` using Polars across the full dataset, not per-row in `__post_init__`. High-volume data requires bulk processing.

**Three log outputs per run.** Console (human-readable), rotating file (human-readable), rotating file (JSON). Log files are timestamped per run to avoid overwriting.

**Type aliases for string semantics.** `Barcode`, `TrnId`, `StoreId` are all `str` at runtime, but the aliases make field intent unambiguous without adding overhead.

---

## Stack

- Python 3.13
- [Polars](https://pola.rs/) - DataFrame transformations
- [Prefect](https://www.prefect.io/) - pipeline orchestration
- [Faker](https://faker.readthedocs.io/) - synthetic data generation
- [python-json-logger](https://github.com/madzak/python-json-logger) - structured JSON logging
- [Ruff](https://docs.astral.sh/ruff/) - linting and formatting
