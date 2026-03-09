"""Data model dataclasses."""

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

type Barcode = str
type TrnId = str
type StoreId = str
type RunId = str
type Period = str


@dataclass
class Product:
    barcode: Barcode
    name: str
    category: str
    sub_category: str
    manufacturer: str
    brand: str
    is_private_label: bool
    ref_price: float


@dataclass
class Transaction:
    trn_id: TrnId
    trn_date: date
    store_id: StoreId
    barcode: Barcode
    quantity: int
    unit_price: float

    def __post_init__(self):
        # Transaction date needs coercion
        if not isinstance(self.trn_date, date):
            self.trn_date = date.fromisoformat(self.trn_date)


class Status(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass
class RunStats:
    raw_transactions: int
    valid_transactions: int
    rejected_transactions: int
    rejection_rate_pct: float
    unique_products: int
    unmatched_barcodes: int


@dataclass
class QualityChecks:
    null_rate_passed: bool
    min_transactions_passed: bool
    price_range_passed: bool


@dataclass
class RunManifest:
    run_id: RunId
    market: str
    period: Period
    status: Status
    started_at: datetime
    completed_at: datetime
    stats: RunStats
    quality_checks: QualityChecks

    def __post_init__(self):
        # status, stats, checks and dates need coercion
        if not isinstance(self.status, Status):
            self.status = Status(self.status)

        if not isinstance(self.stats, RunStats):
            self.stats = RunStats(**self.stats)

        if not isinstance(self.quality_checks, QualityChecks):
            self.quality_checks = QualityChecks(**self.quality_checks)

        if not isinstance(self.started_at, datetime):
            self.started_at = datetime.fromisoformat(self.started_at)

        if not isinstance(self.completed_at, datetime):
            self.completed_at = datetime.fromisoformat(self.completed_at)
