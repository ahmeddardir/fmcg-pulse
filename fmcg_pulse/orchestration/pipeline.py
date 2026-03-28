"""Prefect flow for the fmcg-pulse pipeline.

Orchestrates data generation, ingestion, enrichment, validation,
report building, and manifest writing as a single executable flow.
"""

# pyright: reportUnknownMemberType=false

from datetime import datetime
from pathlib import Path

import polars as pl
import yaml
from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner

from fmcg_pulse.logging_config import setup_logging
from fmcg_pulse.models.config import AppConfig
from fmcg_pulse.models.data import RunManifest, RunStats, Status
from fmcg_pulse.orchestration.tasks import (
    generate,
    ingest_enrich,
    run_report,
    validate,
    write_manifest,
)


@flow(task_runner=ThreadPoolTaskRunner())  # type: ignore
def pipeline() -> None:
    """Run the fmcg-pulse pipeline.

    Orchestrates data generation, ingestion, enrichment, validation,
    report building, and manifest writing as a single Prefect flow.
    """
    started_at = datetime.now()

    # Load config
    with Path("config.yaml").open() as config_file:
        raw_cfg = yaml.safe_load(config_file)
    config = AppConfig(**raw_cfg)

    # Create directories
    config.paths.raw_dir.mkdir(parents=True, exist_ok=True)
    config.paths.output_dir.mkdir(parents=True, exist_ok=True)
    config.paths.logs_dir.mkdir(parents=True, exist_ok=True)

    # Derive run timestamp and run id
    run_ts = started_at.strftime("%Y-%m-%d_%H%M%S")
    run_id = f"fmcg-pulse-{config.pipeline.market.lower().replace(' ', '-')}-{run_ts}"

    # Setup logging
    setup_logging(
        run_ts=run_ts,
        log_dir=config.paths.logs_dir,
        log_level=config.logging.log_level,
        log_format_std=config.logging.standard_format,
        log_format_json=config.logging.json_format,
    )

    # Run tasks
    generate(config)
    enriched_df, unmatched_count = ingest_enrich(config.paths.raw_dir)
    quality_checks = validate(enriched_df, config.quality)

    # Data quality checks
    passed = (
        quality_checks.null_rate_passed
        and quality_checks.min_transactions_passed
        and quality_checks.price_range_passed
    )
    status = Status.FAILURE if not passed else Status.SUCCESS

    # Reporting
    failed_reports = []
    if passed:
        clean_df = enriched_df.filter(pl.col("category").is_not_null())
        future_reports = [
            run_report.submit(clean_df, report, config.paths.output_dir, run_ts)
            for report in config.reporting.reports
        ]
        for future in future_reports:
            future.result(raise_on_failure=False)

        failed_reports = [
            report.name
            for report, future in zip(
                config.reporting.reports, future_reports, strict=True
            )
            if future.state.is_failed()
        ]
        if failed_reports:
            status = Status.PARTIAL

    # Stats
    raw_transactions = len(enriched_df)
    rejected_transactions = enriched_df.select(
        pl.col("category").is_null().sum()
    ).item()
    valid_transactions = raw_transactions - rejected_transactions
    rejection_rate_pct = round(rejected_transactions / raw_transactions * 100, 2)
    unique_products = enriched_df.select(pl.col("barcode").n_unique()).item()

    # Manifest
    completed_at = datetime.now()
    stats = RunStats(
        raw_transactions=raw_transactions,
        valid_transactions=valid_transactions,
        rejected_transactions=rejected_transactions,
        rejection_rate_pct=rejection_rate_pct,
        unique_products=unique_products,
        unmatched_barcodes=unmatched_count,
    )
    manifest = RunManifest(
        run_id=run_id,
        market=config.pipeline.market,
        period=f"{config.generation.start_date} to {config.generation.end_date}",
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        stats=stats,
        quality_checks=quality_checks,
    )
    write_manifest(manifest, config.paths.output_dir, run_ts)

    if failed_reports:
        raise RuntimeError(
            "pipeline completed with errors; "
            f"failed reports: {', '.join(failed_reports)}"
        )
    if status == Status.FAILURE:
        raise RuntimeError(
            "Data quality error; the enriched DataFrame "
            "did not pass one or more quality thresholds."
        )


if __name__ == "__main__":
    pipeline()
