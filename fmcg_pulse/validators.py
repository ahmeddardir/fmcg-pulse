"""Data validation.

Provides bulk data quality checks to validate enriched transactional DataFrames.
"""

# pyright: reportUnknownMemberType=false

import logging

import polars as pl

from fmcg_pulse.models.config import QualityConfig
from fmcg_pulse.models.data import QualityChecks

logger = logging.getLogger(__name__)


def validate_enriched(
    enriched_df: pl.DataFrame, quality_config: QualityConfig
) -> QualityChecks:
    """Validate an enriched transactional DataFrame against quality thresholds.

    Evaluates three core data-quality dimensions:
        - Null-rate check
        - Minimum transaction count check
        - Price-range check

    Args:
        enriched_df (pl.DataFrame):
            The enriched transactional DataFrame to validate.
        quality_config (QualityConfig):
            Configuration object specifying thresholds for null rate,
            minimum transactions, and allowed price range.

    Returns:
        QualityChecks:
            A structured summary of the validation results, containing:
                - null_rate_passed (bool)
                - min_transactions_passed (bool)
                - price_range_passed (bool)

    """
    null_count = enriched_df.select(pl.col("category").is_null().sum()).item()
    max_null_count = int(quality_config.max_null_pct * len(enriched_df))
    price_outliers_count = enriched_df.select(
        (
            (pl.col("unit_price") < quality_config.min_price)
            | (pl.col("unit_price") > quality_config.max_price)
        ).sum()
    ).item()

    logger.debug("maximum null count allowed: %d", max_null_count)
    logger.debug("actual null count: %d", null_count)
    logger.debug("minimum transactions required: %d", quality_config.min_transactions)
    logger.debug("actual transactions: %d", len(enriched_df))
    logger.debug("prices outside the allowed range: %d", price_outliers_count)

    null_rate_passed = null_count <= max_null_count
    logger.info("null_rate_passed: %s", null_rate_passed)
    min_transactions_passed = len(enriched_df) >= quality_config.min_transactions
    logger.info("min_transactions_passed: %s", min_transactions_passed)
    price_range_passed = price_outliers_count == 0
    logger.info("price_range_passed: %s", price_range_passed)

    return QualityChecks(null_rate_passed, min_transactions_passed, price_range_passed)
