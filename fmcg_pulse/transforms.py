"""Reporting transformations.

Provides utilities for enriching transaction data and building analytical
reports. build_enriched() joins transactions with product attributes and
preserves unmatched rows for downstream validation. build_report() applies
filters, derives time-grain periods, aggregates fixed metrics, and computes
market-share percentages across optional partitions.
"""

# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false

import logging
from collections.abc import Callable

import polars as pl

from fmcg_pulse.models.config import Report

logger = logging.getLogger(__name__)


_GRAIN_MAP: dict[str, Callable[[pl.Expr], pl.Expr]] = {
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
) -> tuple[pl.DataFrame, int]:
    """Perform a left join between transactions and products and return a DataFrame.

    Enriches transaction records with product attributes by joining on the shared
    'barcode' column. Both inputs are LazyFrames, and the final result is collected
    into a materialized DataFrame. Unmatched rows are not dropped.

    Args:
        transactions_lf (pl.LazyFrame): LazyFrame containing transaction records.
        products_lf (pl.LazyFrame): LazyFrame containing product metadata.

    Returns:
        tuple[pl.DataFrame, int]:
            The enriched DataFrame and the count of unmatched barcodes.

    """
    enriched_df = (
        transactions_lf.join(products_lf, on="barcode", how="left")
        .drop("ref_price")
        .collect()
    )
    unmatched_count = enriched_df.select(pl.col("category").is_null().sum()).item()
    logger.debug(
        "enriched dataset: %d rows, %d unmatched barcodes",
        len(enriched_df),
        unmatched_count,
    )
    return enriched_df, unmatched_count


def build_report(enriched_df: pl.DataFrame, report: Report) -> pl.DataFrame:
    """Build an aggregated analytical report from enriched transaction data.

    Applies optional date filters, derives a time-grain period (day/week/month/quarter).
    Groups the dataset by the configured dimensions and computes a fixed set of metrics.
    Computes market share percentages either globally or within the defined partitions.
    Assumes the provided DataFrame is clean, with no unmatched rows (i.e. no null
    product columns from the enrichment join).

    Args:
        enriched_df (pl.DataFrame):
            A clean DataFrame containing joined transaction and product attributes.
            Must include at least: trn_date, unit_price, quantity, category.
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
    groupby_cols = []
    partition_cols = [] if report.partition_by is not None else None

    if report.filters is not None:
        if report.filters.date_from is not None:
            enriched_df = enriched_df.filter(
                pl.col("trn_date") >= report.filters.date_from
            )
        if report.filters.date_to is not None:
            enriched_df = enriched_df.filter(
                pl.col("trn_date") <= report.filters.date_to
            )

    if report.time_grain is not None:
        if report.time_grain not in _GRAIN_MAP:
            raise ValueError(f"Unsupported time_grain: {report.time_grain}")

        pl_expr = _GRAIN_MAP[report.time_grain]
        enriched_df = enriched_df.with_columns(
            pl_expr(pl.col("trn_date")).alias("period")
        )

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
    logger.debug(
        "report %s: %d rows entering aggregation", report.name, len(enriched_df)
    )
    enriched_df = enriched_df.group_by(groupby_cols).agg(metrics)
    logger.debug(
        "report %s: %d groups after aggregation", report.name, len(enriched_df)
    )

    if partition_cols is not None:
        denom = pl.col("total_revenue").sum().over(partition_cols)
    else:
        denom = pl.col("total_revenue").sum().over()

    return enriched_df.with_columns(
        pl.col("total_revenue")
        .truediv(denom)
        .mul(100)
        .round(2)
        .alias("market_share_pct")
    ).select(groupby_cols + list(metrics.keys()) + ["market_share_pct"])
