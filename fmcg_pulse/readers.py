"""Ingestion layer file scanners.

Product data is ingested from NDJSON using scan_products(),
and transaction data is ingested from CSV using scan_transactions().
Both functions return Polars LazyFrames.
"""

from pathlib import Path

import polars as pl

from fmcg_pulse.models.data import Product, Transaction


def scan_products(file_path: Path) -> pl.LazyFrame:
    """Return a lazy scanner over an NDJSON products file.

    The file must be in newline-delimited JSON (NDJSON) format, where each
    line contains a single product record.

    Args:
        file_path (Path): Path to the NDJSON products file.

    Returns:
        pl.LazyFrame: Lazily scanned product data with the enforced schema.

    """
    return pl.scan_ndjson(source=file_path, schema=Product.get_schema())


def scan_transactions(file_path: Path) -> pl.LazyFrame:
    """Return a lazy scanner over a CSV transactions file.

    Args:
        file_path (Path): Path to the transactions CSV file.

    Returns:
        pl.LazyFrame: Lazily scanned transaction data with the enforced schema.

    """
    return pl.scan_csv(
        source=file_path, schema=Transaction.get_schema(), try_parse_dates=True
    )
