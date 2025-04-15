"""
Log Handlers Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import logging
import sys

import pytest

from cba_core_lib.logging.log_handlers import (
    init_logging,
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_DATE_FORMAT
)
from tests.conftest import get_handler_by_stream


######################################################################
#  LOG HANDLERS UNIT TEST CASES
######################################################################
def test_adds_two_handlers(test_logger):
    """It should add two StreamHandlers to the logger."""
    init_logging(test_logger)
    assert len(test_logger.handlers) == 2
    assert all(
        isinstance(h, logging.StreamHandler) for h in test_logger.handlers
    )


def test_handlers_target_stdout_stderr(test_logger):
    """It should configure handlers to target sys.stdout and sys.stderr."""
    init_logging(test_logger)
    stdout_handler = get_handler_by_stream(test_logger, sys.stdout)
    stderr_handler = get_handler_by_stream(test_logger, sys.stderr)
    assert stdout_handler is not None
    assert stderr_handler is not None


@pytest.mark.parametrize('level_in, expected_level', [
    (logging.DEBUG, logging.DEBUG),
    (logging.INFO, logging.INFO),
    (logging.WARNING, logging.WARNING),
    (logging.ERROR, logging.ERROR),
    # Even if set to ERROR, stdout handler exists but won't emit below ERROR
])
def test_logger_level_set(test_logger, level_in, expected_level):
    """It should set the logger's main level to the provided log level."""
    init_logging(test_logger, log_level=level_in)
    assert test_logger.level == expected_level


@pytest.mark.parametrize('level_in, expected_stdout_level', [
    (logging.DEBUG, logging.DEBUG),
    (logging.INFO, logging.INFO),
    (logging.WARNING, logging.WARNING),
    (logging.ERROR, logging.ERROR),
    # Stdout handler level matches logger level
])
def test_handler_levels_set(test_logger, level_in, expected_stdout_level):
    """It should set the stdout handler's level to the provided log level and stderr
    to ERROR."""
    init_logging(test_logger, log_level=level_in)
    stdout_handler = get_handler_by_stream(test_logger, sys.stdout)
    stderr_handler = get_handler_by_stream(test_logger, sys.stderr)

    assert stdout_handler.level == expected_stdout_level
    assert stderr_handler.level == logging.ERROR  # stderr handler is always ERROR


def test_stdout_handler_filter(test_logger):
    """It should add a filter to the stdout handler to filter out ERROR and above."""
    init_logging(test_logger, log_level=logging.DEBUG)
    stdout_handler = get_handler_by_stream(test_logger, sys.stdout)
    # We expect exactly one filter, which is the lambda checking levelno
    assert len(stdout_handler.filters) == 1


def test_stderr_handler_no_extra_filters(test_logger):
    """It should not add any extra filters to the stderr handler."""
    init_logging(test_logger)
    stderr_handler = get_handler_by_stream(test_logger, sys.stderr)
    assert len(stderr_handler.filters) == 0  # No filters added by our function


@pytest.mark.parametrize('log_format, date_format', [
    (DEFAULT_LOG_FORMAT, DEFAULT_LOG_DATE_FORMAT),
    ("CUSTOM_FORMAT [%(levelname)s] %(message)s", '%H:%M:%S'),
])
def test_formatter_applied(test_logger, log_format, date_format):
    """It should apply the provided formatter to both handlers."""
    init_logging(
        test_logger,
        log_format=log_format,
        date_format=date_format
    )
    stdout_handler = get_handler_by_stream(test_logger, sys.stdout)
    stderr_handler = get_handler_by_stream(test_logger, sys.stderr)

    assert stdout_handler.formatter is not None
    assert stderr_handler.formatter is not None
    # Check if it's the *same* formatter instance
    assert stdout_handler.formatter is stderr_handler.formatter
    # Check the format strings on the formatter instance
    formatter = stdout_handler.formatter
    assert formatter._fmt == log_format
    assert formatter.datefmt == date_format


def test_clear_existing_handlers_true(test_logger):
    """It should clear existing handlers when clear_existing is True (default)."""
    # Add a dummy handler first
    dummy_handler = logging.NullHandler()
    test_logger.addHandler(dummy_handler)
    test_logger.propagate = True  # Set to True to check if it gets set to False
    assert len(test_logger.handlers) == 1

    init_logging(test_logger)  # clear_existing defaults to True

    assert len(test_logger.handlers) == 2  # Only the two new ones should exist
    assert dummy_handler not in test_logger.handlers
    assert test_logger.propagate is False  # Should be set to False when clearing


def test_clear_existing_handlers_false(test_logger):
    """It should keep existing handlers when clear_existing is False."""
    # Add a dummy handler first
    dummy_handler = logging.NullHandler()
    test_logger.addHandler(dummy_handler)
    test_logger.propagate = True  # Set to True initially
    assert len(test_logger.handlers) == 1

    init_logging(test_logger, clear_existing=False)

    assert len(test_logger.handlers) == 3  # Dummy + stdout + stderr
    assert dummy_handler in test_logger.handlers
    # Verify propagate wasn't changed by our function when not clearing
    assert test_logger.propagate is True


def test_default_settings_smoke_test(test_logger):
    """It should initialize logging with default parameters successfully."""
    init_logging(test_logger)

    assert len(test_logger.handlers) == 2
    assert test_logger.level == logging.INFO  # Default level

    stdout_handler = get_handler_by_stream(test_logger, sys.stdout)
    stderr_handler = get_handler_by_stream(test_logger, sys.stderr)

    assert stdout_handler is not None
    assert stderr_handler is not None
    assert stdout_handler.level == logging.INFO  # Default level
    assert stderr_handler.level == logging.ERROR

    formatter = stdout_handler.formatter
    assert formatter._fmt == DEFAULT_LOG_FORMAT
    assert formatter.datefmt == DEFAULT_LOG_DATE_FORMAT
