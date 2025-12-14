"""
Logging configuration for the ETL pipeline
"""
import logging
import sys
from typing import Optional
import structlog
from etl.config import config


def setup_logging(log_level: Optional[str] = None) -> None:
    """Configure structured logging for the application"""
    
    level = log_level or config.etl.log_level
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if config.etl.log_level == "json" 
            else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a logger instance"""
    return structlog.get_logger(name)
