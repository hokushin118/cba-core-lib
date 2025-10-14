"""
Utility functions.

This module contains common utility functions.
"""
from __future__ import annotations

import logging
import uuid

logger = logging.getLogger(__name__)


######################################################################
#  COMMON UTILS
######################################################################
def generate_correlation_id() -> str:
    """Generates a unique correlation ID.

    It should create and return a new universally unique identifier (UUID)
    as a string, which can be used to correlate logs or trace requests.

    Returns:
        str: A unique correlation ID in string format.
    """
    return str(uuid.uuid4())


######################################################################
# VALIDATION METHODS
######################################################################
def check_not_whitespace_only(
        value: str
) -> str:
    """Ensure the string is not composed solely of whitespace.

    This function checks if the input string, after removing leading and
    trailing whitespace, is empty. If the stripped string is empty, it raises a
    ValueError.

    Args:
        value (str): The string to check.

    Returns:
        str: The original string if it is not composed solely of whitespace.

    Raises:
        ValueError: If the string consists only of whitespace characters.
    """
    if isinstance(value, str):
        if not value or value.isspace():
            raise ValueError(
                'Field cannot consist only of whitespace.'
            )
    return value
