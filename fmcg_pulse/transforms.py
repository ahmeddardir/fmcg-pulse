"""Reporting transformations.

Provides utilities for enriching transaction data and building analytical
reports. build_enriched() joins transactions with product attributes, while
build_report() applies filters, derives time-grain periods, aggregates fixed
metrics, and computes market-share percentages across optional partitions.
"""

import logging

import polars as pl

from fmcg_pulse.models.config import Report

logger = logging.getLogger(__name__)


_GRAIN_MAP = {
    "day": lambda c: c.cast(pl.String),
    "week": lambda c: c.dt.strftime("%G-W%V"),
    "month": lambda c: c.dt.strftime("%Y-%m"),
    "quarter": lambda c: pl.concat_str(
        [
            c.dt.year().cast(pl.String),
            pl.lit("-Q"),
            c.dt.month().sub(1).floordiv(3).add(1).cast(pl.String),
        ]
    ),
}


def build_enriched(
    transactions_lf: pl.LazyFrame, products_lf: pl.LazyFrame
) -> pl.DataFrame:
    """Perform a left join between transactions and products and return a DataFrame.

    Enriches transaction records with product attributes by joining on the shared
    'barcode' column. Both inputs are LazyFrames, and the final result is collected
    into a materialized DataFrame.

    Args:
        transactions_lf (pl.LazyFrame): LazyFrame containing transaction records.
        products_lf (pl.LazyFrame): LazyFrame containing product metadata.

    Returns:
        pl.DataFrame: A DataFrame containing transactions enriched with product fields.

    """
    enriched = (
        transactions_lf.join(products_lf, on="barcode", how="left")
        .drop("ref_price")
        .collect()
    )
    unmatched = enriched.filter(pl.col("category").is_null()).height
    logger.debug(
        "enriched dataset: %d rows, %d unmatched barcodes", len(enriched), unmatched
    )
    return enriched


def build_report(enriched_df: pl.DataFrame, report: Report) -> pl.DataFrame:
    """Build an aggregated analytical report from enriched transaction data.

    This function applies optional date filters, derives a time-grain period
    (day/week/month/quarter), groups the dataset by the configured dimensions,
    computes fixed commercial metrics, and calculates market share percentages
    either globally or within user-defined partitions.

    Args:
        enriched_df (pl.DataFrame):
            A DataFrame containing joined transaction and product attributes.
            Must include at least: date, unit_price, quantity, category.
        report (Report):
            A configuration object defining:
                - dimensions: grouping columns
                - partition_by: optional window columns for market share
                - time_grain: period granularity (day/week/month/quarter)
                - filters: optional date_from/date_to constraints

    Returns:
        pl.DataFrame:
            A grouped and aggregated report containing:
                - grouping columns (period + dimensions)
                - computed metrics (revenue, units, transactions, ASP)
                - market_share_pct

    """
    report_df = enriched_df.filter(pl.col("category").is_not_null())
    dropped = len(enriched_df) - len(report_df)
    logger.debug("report %s: dropped %d unmatched rows", report.name, dropped)

    groupby_cols = []
    partition_cols = [] if report.partition_by is not None else None

    if report.filters is not None:
        if report.filters.date_from is not None:
            report_df = report_df.filter(pl.col("date") >= report.filters.date_from)
        if report.filters.date_to is not None:
            report_df = report_df.filter(pl.col("date") <= report.filters.date_to)

    if report.time_grain is not None:
        if report.time_grain not in _GRAIN_MAP:
            raise ValueError(f"Unsupported time_grain: {report.time_grain}")

        pl_expr = _GRAIN_MAP[report.time_grain]
        report_df = report_df.with_columns(pl_expr(pl.col("date")).alias("period"))

        groupby_cols.append("period")
        if partition_cols is not None:
            partition_cols.append("period")

    groupby_cols.extend(report.dimensions)
    if report.partition_by and partition_cols is not None:
        partition_cols.extend(report.partition_by)

    metrics = {
        "total_revenue": pl.col("unit_price").mul(pl.col("quantity")).sum(),
        "total_units": pl.col("quantity").sum(),
        "transactions": pl.len(),
        "avg_selling_price": pl.col("unit_price")
        .mul(pl.col("quantity"))
        .sum()
        .truediv(pl.col("quantity").sum()),
    }
    logger.debug("report %s: %d rows entering aggregation", report.name, len(report_df))
    report_df = report_df.group_by(groupby_cols).agg(metrics)
    logger.debug("report %s: %d groups after aggregation", report.name, len(report_df))

    if partition_cols is not None:
        denom = pl.col("total_revenue").sum().over(partition_cols)
    else:
        denom = pl.col("total_revenue").sum().over()

    return report_df.with_columns(
        pl.col("total_revenue")
        .truediv(denom)
        .mul(100)
        .round(2)
        .alias("market_share_pct")
    ).select(groupby_cols + list(metrics.keys()) + ["market_share_pct"])
