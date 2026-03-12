"""Configuration dataclasses."""

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path


def _coerce_attr_dict(cls, instance):
    """Coerce dict-valued attributes into their declared dataclass types."""
    for field_name, constructor in cls.__annotations__.items():
        value = getattr(instance, field_name)
        if isinstance(value, dict):
            setattr(instance, field_name, constructor(**value))


@dataclass
class PipelineConfig:
    """Store pipeline-level configuration."""

    market: str


@dataclass
class PathsConfig:
    """Store filesystem paths for raw data, outputs, and logs."""

    raw_dir: Path
    output_dir: Path
    logs_dir: Path

    def __post_init__(self):
        """Normalize all paths to Path objects."""
        self.raw_dir = Path(self.raw_dir)
        self.output_dir = Path(self.output_dir)
        self.logs_dir = Path(self.logs_dir)


class LogLevel(StrEnum):
    """Enumeration of supported logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LoggingConfig:
    """Store logging formats and log level configuration."""

    log_level: LogLevel
    standard_format: str
    json_format: str

    def __post_init__(self):
        """Coerce log_level into a LogLevel enum if provided as a string."""
        if not isinstance(self.log_level, LogLevel):
            self.log_level = LogLevel(self.log_level)


@dataclass
class GenerationConfig:
    """Store parameters controlling synthetic data generation."""

    n_transactions: int
    n_products: int
    start_date: date
    end_date: date

    def __post_init__(self):
        """Coerce dates and validate chronological order."""
        if not isinstance(self.start_date, date):
            self.start_date = date.fromisoformat(self.start_date)
        if not isinstance(self.end_date, date):
            self.end_date = date.fromisoformat(self.end_date)
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date {self.start_date} must be <= end_date {self.end_date}."
            )


@dataclass
class QualityConfig:
    """Store validation thresholds for generated data quality."""

    max_null_pct: float
    min_transactions: int
    min_price: float
    max_price: float

    def __post_init__(self):
        """Validate numeric ranges and semantic constraints."""
        if not (0 < self.max_null_pct <= 1):
            raise ValueError("max_null_pct must be between 0 and 1.")

        if self.min_transactions <= 0:
            raise ValueError("min_transactions must be > 0.")

        if self.min_price <= 0:
            raise ValueError("min_price must be > 0.")
        if self.max_price <= 0:
            raise ValueError("max_price must be > 0.")
        if self.min_price >= self.max_price:
            raise ValueError(
                f"min_price ({self.min_price}) must be < max_price ({self.max_price})."
            )


@dataclass
class Report:
    """Define a report's dimensions and partition columns for window-based metrics."""

    name: str
    dimensions: list[str]
    partition_by: str | list[str] | None = None

    def __post_init__(self):
        """Validate name and dimensions, and normalize and validate partition_by."""
        if not self.name:
            raise ValueError("Report name cannot be empty.")

        if not self.dimensions:
            raise ValueError(f"Report '{self.name}' must have at least one dimension.")
        if len(self.dimensions) != len(set(self.dimensions)):
            raise ValueError(f"Report '{self.name}' has duplicate dimensions.")

        if self.partition_by is not None:
            if not isinstance(self.partition_by, list):
                self.partition_by = [self.partition_by]
            if not self.partition_by:  # empty list after normalization
                self.partition_by = None
            else:
                if len(self.partition_by) != len(set(self.partition_by)):
                    raise ValueError(
                        f"Report '{self.name}' has duplicate partition_by values."
                    )
                invalid = [
                    col for col in self.partition_by if col not in self.dimensions
                ]
                if invalid:
                    raise ValueError(
                        f"Report '{self.name}' has partition_by values "
                        f"not in dimensions: {invalid}."
                    )


@dataclass
class ReportingConfig:
    """Store and validate a collection of report definitions."""

    reports: list[Report]

    def __post_init__(self):
        """Coerce dicts into Report objects and enforce unique names."""
        self.reports = [
            Report(**report) if isinstance(report, dict) else report
            for report in self.reports
        ]

        names = [report.name for report in self.reports]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate report names found in reporting config.")


@dataclass
class AppConfig:
    """Aggregate all application configuration sections."""

    pipeline: PipelineConfig
    paths: PathsConfig
    logging: LoggingConfig
    generation: GenerationConfig
    quality: QualityConfig
    reporting: ReportingConfig

    def __post_init__(self):
        """Coerce nested dicts into their respective dataclass types."""
        _coerce_attr_dict(AppConfig, self)
