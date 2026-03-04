"""Configuration dataclasses."""

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path


def _coerce_attr_dict(cls, instance):
    for field_name, constructor in cls.__annotations__.items():
        value = getattr(instance, field_name)
        if isinstance(value, dict):
            setattr(instance, field_name, constructor(**value))


@dataclass
class PipelineConfig:
    market: str


@dataclass
class PathsConfig:
    raw_dir: Path
    output_dir: Path
    logs_dir: Path

    def __post_init__(self):
        self.raw_dir = Path(self.raw_dir)
        self.output_dir = Path(self.output_dir)
        self.logs_dir = Path(self.logs_dir)


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LoggingConfig:
    log_level: LogLevel
    standard_format: str
    json_format: str

    def __post_init__(self):
        # log_level needs coercion
        if not isinstance(self.log_level, LogLevel):
            self.log_level = LogLevel(self.log_level)


@dataclass
class GenerationConfig:
    n_transactions: int
    n_products: int
    start_date: date
    end_date: date

    def __post_init__(self):
        if not isinstance(self.start_date, date):
            self.start_date = date.fromisoformat(self.start_date)
        if not isinstance(self.end_date, date):
            self.end_date = date.fromisoformat(self.end_date)

        # Validate date order
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date {self.start_date} must be <= end_date {self.end_date}"
            )


@dataclass
class QualityConfig:
    max_null_pct: float
    min_transactions: int
    min_price: float
    max_price: float

    def __post_init__(self):
        # Basic positivity checks
        if self.min_transactions <= 0:
            raise ValueError("min_transactions must be > 0")

        if self.min_price <= 0:
            raise ValueError("min_price must be > 0")

        if self.max_price <= 0:
            raise ValueError("max_price must be > 0")

        # Semantic checks
        if not (0 < self.max_null_pct <= 1):
            raise ValueError("max_null_pct must be between 0 and 1 (e.g., 0.05 for 5%)")

        if self.min_price >= self.max_price:
            raise ValueError(
                f"min_price ({self.min_price}) must be < max_price ({self.max_price})"
            )


@dataclass
class Report:
    name: str
    dimensions: list[str]

    def __post_init__(self):
        if not self.name:
            raise ValueError("Report name cannot be empty")

        if not self.dimensions:
            raise ValueError(f"Report '{self.name}' must have at least one dimension")

        # Enforce uniqueness on dimensions
        if len(self.dimensions) != len(set(self.dimensions)):
            raise ValueError(f"Report '{self.name}' has duplicate dimensions")


@dataclass
class ReportingConfig:
    reports: list[Report]

    def __post_init__(self):
        self.reports = [
            Report(**report) if isinstance(report, dict) else report
            for report in self.reports
        ]
        # Enforce uniqueness on report names
        names = [report.name for report in self.reports]

        if len(names) != len(set(names)):
            raise ValueError("Duplicate report names found in reporting config")


@dataclass
class AppConfig:
    pipeline: PipelineConfig
    paths: PathsConfig
    logging: LoggingConfig
    generation: GenerationConfig
    quality: QualityConfig
    reporting: ReportingConfig

    def __post_init__(self):
        _coerce_attr_dict(AppConfig, self)
