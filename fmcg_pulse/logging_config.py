"""
Logging configuration.

Configures three handlers on the root logger:
  - Console: human-readable output to stdout
  - File (standard): human-readable rotating log file
  - File (JSON): structured rotating log file for machine consumption
"""

import logging
import logging.config
from datetime import datetime
from pathlib import Path

from pythonjsonlogger.json import JsonFormatter

from fmcg_pulse.models.config import LogLevel

LOG_FMT_STD = "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"
LOG_FMT_JSON = "%(asctime)s %(levelname)s %(name)s %(filename)s %(lineno)d %(message)s"


def setup_logging(
    log_dir: Path,
    started_at: datetime,
    log_level: LogLevel = LogLevel.DEBUG,
    log_format_std: str = LOG_FMT_STD,
    log_format_json: str = LOG_FMT_JSON,
) -> None:
    """
    Configure the root logger.

    Sets up console, rotating text file, and rotating JSON file handlers.
    """
    ts = started_at.strftime("%Y-%m-%d_%H%M%S")

    config = {
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
                "filename": f"{log_dir}/pipeline_{ts}.log",
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "standard",
                "level": log_level,
            },
            "file_json": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": f"{log_dir}/pipeline_{ts}.json.log",
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
