"""Prefect tasks for the fmcg-pulse pipeline.

Each task wraps a single pipeline step: data generation, ingestion and
enrichment, validation, report building, and manifest writing. Tasks are
intentionally thin; all business logic lives in the modules they delegate to.
"""

import json
import logging
from dataclasses import asdict
from pathlib import Path

import polars as pl
from prefect import task
from prefect.cache_policies import NONE

from fmcg_pulse.generators import generate_all
from fmcg_pulse.models.config import AppConfig, QualityConfig, Report
from fmcg_pulse.models.data import QualityChecks, RunManifest
from fmcg_pulse.readers import scan_products, scan_transactions
from fmcg_pulse.transforms import build_enriched, build_report
from fmcg_pulse.validators import validate_enriched

logger = logging.getLogger(__name__)


@task(cache_policy=NONE)
def generate(config: AppConfig) -> None:
    """Run synthetic data generation.

    Delegates to generate_all(), which builds products and transactions
    and writes them to the raw data directory defined in the config.

    Args:
        config (AppConfig):
            Application configuration containing generation settings
            and output paths.

    """
    generate_all(config)


@task(cache_policy=NONE)
def ingest_enrich(raw_dir: Path) -> tuple[pl.DataFrame, int]:
    """Scan raw products and transactions and build the enriched DataFrame.

    Reads products.json and transactions.csv from the raw directory,
    constructs LazyFrames with enforced schemas, and joins product
    attributes onto transaction records.

    Args:
        raw_dir (Path): Directory containing products.json and transactions.csv.

    Returns:
        tuple[pl.DataFrame, int]:
            The enriched DataFrame and the count of unmatched barcodes.

    """
    products_path = raw_dir / "products.json"
    transactions_path = raw_dir / "transactions.csv"

    products_lf = scan_products(products_path)
    transactions_lf = scan_transactions(transactions_path)

    logger.debug("building enriched DataFrame")
    enriched_df, unmatched_count = build_enriched(transactions_lf, products_lf)
    logger.info(
        "enriched DataFrame built: %d rows, %d unmatched barcodes",
        len(enriched_df),
        unmatched_count,
    )

    return enriched_df, unmatched_count


@task(cache_policy=NONE)
def validate(enriched_df: pl.DataFrame, quality_config: QualityConfig) -> QualityChecks:
    """Validate an enriched transactional DataFrame against quality thresholds.

    Checks null rate, minimum transaction count, and price range across
    the full dataset using bulk Polars operations.

    Args:
        enriched_df (pl.DataFrame):
            The enriched transactional DataFrame to validate.
        quality_config (QualityConfig):
            Configuration object specifying validation thresholds.

    Returns:
        QualityChecks:
            Structured results summarizing all three validation outcomes.

    """
    logger.debug("validating enriched DataFrame")
    quality_checks = validate_enriched(enriched_df, quality_config)
    logger.info(
        "validation complete: null_rate=%s, min_transactions=%s, price_range=%s",
        quality_checks.null_rate_passed,
        quality_checks.min_transactions_passed,
        quality_checks.price_range_passed,
    )

    return quality_checks


@task(cache_policy=NONE)
def run_report(
    clean_df: pl.DataFrame, report: Report, output_dir: Path, run_ts: str
) -> None:
    """Build an aggregated report and write it to a timestamped CSV file.

    Applies the report configuration to the clean, enriched DataFrame,
    computing grouped metrics and market share. Writes the result to output_dir.
    Exceptions from either build or write steps are logged and re-raised.

    Args:
        clean_df (pl.DataFrame):
            The clean, enriched transactional DataFrame with no unmatched rows.
        report (Report):
            Report configuration defining dimensions, time grain,
            partition columns, and date filters.
        output_dir (Path):
            Directory where the report CSV will be written.
        run_ts (str):
            Timestamp string used to version the output filename.

    """
    logger.debug("building report: %s", report.name)
    try:
        report_df = build_report(clean_df, report)
    except Exception:
        logger.exception("Building report '%s' failed.", report.name)
        raise

    report_path = output_dir / f"{report.name}_{run_ts}.csv"
    logger.debug("writing report '%s' to '%s'", report.name, report_path)
    try:
        report_df.write_csv(report_path)
    except Exception:
        logger.exception(
            "Writing report '%s' to '%s' failed.", report.name, report_path
        )
        raise
    logger.info("report '%s' written to '%s'", report.name, report_path)


@task(cache_policy=NONE)
def write_manifest(run_manifest: RunManifest, output_dir: Path, run_ts: str) -> None:
    """Serialize a RunManifest to a timestamped JSON file.

    Writes run metadata, statistics, and quality check results to
    output_dir as a JSON audit log for the current pipeline run.

    Args:
        run_manifest (RunManifest): Metadata and results for the current pipeline run.
        output_dir (Path): Directory where the manifest file will be written.
        run_ts (str): Timestamp string used to version the manifest filename.

    """
    manifest_path = output_dir / f"run_manifest_{run_ts}.json"
    logger.debug(
        "writing metadata and results for run '%s' to '%s'",
        run_manifest.run_id,
        manifest_path,
    )
    with manifest_path.open("w") as json_file:
        json_file.write(json.dumps(asdict(run_manifest), default=str))
    logger.info(
        "metadata and results for run '%s' saved to '%s'",
        run_manifest.run_id,
        manifest_path,
    )
