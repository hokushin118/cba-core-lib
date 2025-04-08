"""
Log Handlers.

This module contains utility functions to set up logging
consistently for containerized environments.
"""
import logging
import sys

# Constants for log formatting (can be customized)
DEFAULT_LOG_FORMAT = \
    "[%(asctime)s] [%(levelname)s] [%(pathname)s:%(lineno)d] %(message)s"
DEFAULT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S %z"


def init_logging(
        logger: logging.Logger,
        log_level: int = logging.INFO,
        log_format: str = DEFAULT_LOG_FORMAT,
        date_format: str = DEFAULT_LOG_DATE_FORMAT,
        clear_existing: bool = True,
) -> None:
    """Sets up standard logging handlers targeting stdout and stderr.

    This function configures the provided logger instance to send logs
    to stdout for INFO level (and below, depending on `log_level`) and
    to stderr for ERROR and CRITICAL levels. This separation is ideal for
    container orchestrators (like Docker or Kubernetes) to capture and
    route log streams appropriately.

    Args:
        logger (logging.Logger): The standard Python logger instance
                        to configure.(e.g., `logging.getLogger(__name__)`
                        or `app.logger` for Flask).
        log_level (int): The minimum logging level for the logger and stdout
                         handler (default: logging.INFO).
        log_format (str): The format string for log messages.
        date_format (str): The format string for the date/time part of logs.
        clear_existing (bool): If True, remove any existing handlers attached
                               to the logger before adding the new ones
                               (default: True). This prevents potential log
                               duplication.

    Usage:
        Configure Flask's default logger using the shared library function
        Flask's app.logger is a standard logging.Logger instance
        init_logging(app.logger, log_level=log_level)
    """
    # Ensure the logger level is set
    logger.setLevel(log_level)

    # Define a consistent log formatter
    formatter = logging.Formatter(log_format, date_format)

    # Clear existing handlers if requested
    if clear_existing and logger.hasHandlers():
        logger.handlers.clear()
        # Prevent logs from propagating to parent loggers
        # (like the root logger) if we are managing handlers
        # directly here
        logger.propagate = False

    # Configure logging to stdout for log_level and below
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    # Filter out logs >= ERROR level for stdout handler
    stdout_handler.addFilter(lambda record: record.levelno < logging.ERROR)
    stdout_handler.setFormatter(formatter)
    # Ensure unbuffered writes for stdout if possible (best effort)
    # stdout_handler.flush = sys.stdout.flush # Note: StreamHandler
    # typically manages flush

    # Configure logging to stderr for ERROR and CRITICAL messages
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(formatter)
    # Ensure unbuffered writes for stderr if possible (best effort)
    # stderr_handler.flush = sys.stderr.flush # Note: StreamHandler
    # typically manages flush

    # Add the new handlers
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

    logger.info(
        "Container-friendly logging handlers configured for logger '%s'.",
        logger.name
    )
