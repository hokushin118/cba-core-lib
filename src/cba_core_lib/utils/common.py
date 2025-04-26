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
