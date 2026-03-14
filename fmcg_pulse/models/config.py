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


def _coerce_date_range(
    start_date: str | date | None,
    end_date: str | date | None,
) -> tuple[date | None, date | None]:
    """Coerce inputs to date objects and ensure start <= end when both exist.

    Accepts ISO-formatted strings (YYYY-MM-DD), date objects, or None.
    Converts non-None values to date instances and validates that the start
    date does not occur after the end date when both are provided.

    Args:
        start_date (str | date | None): Start of the date range.
        end_date (str | date | None): End of the date range.

    Raises:
        ValueError: If both dates are provided and start_date > end_date.

    Returns:
        tuple[date | None, date | None]: The coerced (start_date, end_date).

    """
    if start_date is not None and not isinstance(start_date, date):
        start_date = date.fromisoformat(start_date)
    if end_date is not None and not isinstance(end_date, date):
        end_date = date.fromisoformat(end_date)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError(f"start date {start_date} must be <= end date {end_date}.")
    return start_date, end_date


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
        start, end = _coerce_date_range(self.start_date, self.end_date)
        if start is None or end is None:
            raise ValueError("start_date and end_date are required.")
        self.start_date, self.end_date = start, end


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


class TimeGrain(StrEnum):
    """Enumeration of supported time grains."""

    day = "day"
    week = "week"
    month = "month"
    quarter = "quarter"


@dataclass
class ReportFilters:
    """Define an optional date range filter for a report."""

    date_from: date | None = None
    date_to: date | None = None

    def __post_init__(self):
        """Coerce dates and validate chronological order."""
        self.date_from, self.date_to = _coerce_date_range(self.date_from, self.date_to)


@dataclass
class Report:
    """Define a report's dimensions, partition columns, time grain, and date filters."""

    name: str
    dimensions: list[str]
    partition_by: str | list[str] | None = None
    time_grain: TimeGrain | None = None
    filters: ReportFilters | None = None

    def __post_init__(self):
        """Validate and normalize all Report fields.

        Checks name and dimensions, normalizes and validates partition_by,
        and coerces time_grain and filters to their correct types.
        """
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

        if self.time_grain is not None and not isinstance(self.time_grain, TimeGrain):
            self.time_grain = TimeGrain(self.time_grain)

        if self.filters is not None and not isinstance(self.filters, ReportFilters):
            self.filters = ReportFilters(**self.filters)
        if (
            isinstance(self.filters, ReportFilters)
            and self.filters.date_from is None
            and self.filters.date_to is None
        ):  # empty filters after normalization
            self.filters = None


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
