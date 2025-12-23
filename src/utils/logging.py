"""
============================================================================
Logging - Configuration logging structuré
============================================================================
"""
import logging
import sys
from typing import Any

import structlog

from src.config.settings import get_settings


def setup_logging() -> None:
    """Configure le logging structuré avec structlog"""
    settings = get_settings()

    # Configuration structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if sys.stderr.isatty() else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(settings.log_level)),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> Any:
    """Obtenir un logger"""
    return structlog.get_logger(name)