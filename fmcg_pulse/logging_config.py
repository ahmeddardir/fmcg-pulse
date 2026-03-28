"""Logging configuration.

Configures three handlers on the root logger:
  - Console: human-readable output to stdout
  - File (standard): human-readable rotating log file
  - File (JSON): structured rotating log file for machine consumption
"""

import logging
import logging.config
from pathlib import Path

from pythonjsonlogger.json import JsonFormatter

from fmcg_pulse.models.config import LogLevel

LOG_FMT_STD = "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"
LOG_FMT_JSON = "%(asctime)s %(levelname)s %(name)s %(filename)s %(lineno)d %(message)s"


def setup_logging(
    run_ts: str,
    log_dir: Path,
    log_level: LogLevel = LogLevel.DEBUG,
    log_format_std: str = LOG_FMT_STD,
    log_format_json: str = LOG_FMT_JSON,
) -> None:
    """Configure root logging for a pipeline run.

    Args:
        run_ts (str):
            Timestamp string used to version log filenames.
        log_dir (Path):
            Directory where log files are written.
        log_level (LogLevel, optional):
            Minimum log level applied to all handlers. Defaults to LogLevel.DEBUG.
        log_format_std (str, optional):
            Format string for human-readable log output. Defaults to LOG_FMT_STD.
        log_format_json (str, optional):
            Format string for structured JSON log output. Defaults to LOG_FMT_JSON.

    """
    config: dict[str, object] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {"format": log_format_std},
            "json": {"()": JsonFormatter, "fmt": log_format_json},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": log_level,
            },
            "file_standard": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": f"{log_dir}/pipeline_{run_ts}.log",
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "standard",
                "level": log_level,
            },
            "file_json": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": f"{log_dir}/pipeline_{run_ts}.json.log",
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "json",
                "level": log_level,
            },
        },
        "root": {
            "handlers": ["console", "file_standard", "file_json"],
            "level": log_level,
        },
    }

    logging.config.dictConfig(config)
