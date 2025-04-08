"""
Pytest configuration and fixtures.
"""
import logging

import pytest


######################################################################
#  HELPER FUNCTIONS
######################################################################
def get_handler_by_stream(logger: logging.Logger, stream):
    """Finds a StreamHandler associated with a specific stream."""
    for handler in logger.handlers:
        if isinstance(
                handler,
                logging.StreamHandler
        ) and handler.stream == stream:
            return handler
    return None


######################################################################
#  FIXTURES
######################################################################
@pytest.fixture
def test_logger(request):
    """Provides a clean logger instance for each test."""
    # Use the test name to ensure a unique logger name
    logger_name = f"test_logger_{request.node.name}"
    logger = logging.getLogger(logger_name)
    # Reset logger settings before each test
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)  # Start with a permissive level
    logger.propagate = False  # Avoid interference from root logger handlers
    yield logger
