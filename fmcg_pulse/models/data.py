"""Data model dataclasses."""

from dataclasses import dataclass, fields
from datetime import date, datetime
from enum import StrEnum
from typing import get_type_hints

import polars as pl

type Barcode = str
type TrnId = str
type StoreId = str
type RunId = str
type Period = str

PY_TO_PL: dict[type, type[pl.DataType]] = {
    str: pl.String,
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    date: pl.Date,
}


def _schema_from_dataclass(cls: type) -> dict[str, type[pl.DataType]]:
    """Generate a Polars schema dictionary from a dataclass definition.

    Inspects the dataclass fields and maps their annotated Python types to Polars dtypes
    using the PY_TO_PL mapping. Provides a single source of truth for schema generation.

    Args:
        cls: The dataclass type to derive a schema from.

    Returns:
        dict[str, type[pl.DataType]]: A mapping of field names to Polars dtype classes.

    """
    hints = get_type_hints(cls)
    return {field.name: PY_TO_PL[hints[field.name]] for field in fields(cls)}


@dataclass
class Product:
    """Represent a single product."""

    barcode: Barcode
    name: str
    category: str
    sub_category: str
    manufacturer: str
    brand: str
    is_private_label: bool
    ref_price: float

    @classmethod
    def get_schema(cls) -> dict[str, type[pl.DataType]]:
        """Return the Polars schema for the Product model.

        Returns:
            dict[str, type[pl.DataType]]:
                Polars dtype classes for each Product field.

        """
        return _schema_from_dataclass(cls)


@dataclass
class Transaction:
    """Represent a single transaction."""

    trn_id: TrnId
    trn_date: date
    store_id: StoreId
    barcode: Barcode
    quantity: int
    unit_price: float

    @classmethod
    def get_schema(cls) -> dict[str, type[pl.DataType]]:
        """Return the Polars schema for the Transaction model.

        Returns:
            dict[str, type[pl.DataType]]:
                Polars dtype classes for each Transaction field.

        """
        return _schema_from_dataclass(cls)


class Status(StrEnum):
    """Enumeration of run execution statuses."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    PARTIAL = "PARTIAL"


@dataclass
class RunStats:
    """Store aggregated statistics for a pipeline run."""

    raw_transactions: int
    valid_transactions: int
    rejected_transactions: int
    rejection_rate_pct: float
    unique_products: int
    unmatched_barcodes: int


@dataclass
class QualityChecks:
    """Store results of data quality checks."""

    null_rate_passed: bool
    min_transactions_passed: bool
    price_range_passed: bool


@dataclass
class RunManifest:
    """Represent metadata and results for a pipeline run."""

    run_id: RunId
    market: str
    period: Period
    status: Status
    started_at: datetime
    completed_at: datetime
    stats: RunStats
    quality_checks: QualityChecks
