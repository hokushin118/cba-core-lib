"""
Kafka utility functions.

This module contains Kafka utility functions.
"""
from __future__ import annotations

import json
import logging
from json import JSONDecodeError
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _handle_bytes_input_for_json(
        byte_input: bytes
) -> Optional[bytes]:
    """
    Internal helper to handle byte inputs for safe_json_serializer.

    Attempts to decode/parse bytes as UTF-8 JSON and re-encode. Logs
    warnings and errors appropriately.

    Args:
        byte_input: The bytes object suspected to be JSON.

    Returns:
        Re-encoded JSON bytes if successful, None otherwise.
    """
    logger.warning(
        "Audit JSON serializer received unexpected bytes input. "
        "Attempting to decode/re-encode as UTF-8 JSON. Input prefix: %s",
        byte_input[:100]
    )
    try:
        v_decoded = json.loads(byte_input.decode('utf-8'))
        return json.dumps(v_decoded).encode('utf-8')
    except (
            UnicodeDecodeError,
            JSONDecodeError
    ) as err:
        logger.error(
            "Could not decode/parse bytes received by JSON serializer "
            "as valid UTF-8 JSON. Input prefix: %s. Error: %s",
            byte_input[:100],
            err
        )
        return None
    except Exception as err:  # pylint: disable=W0703
        logger.error(
            "Unexpected error processing bytes received by JSON serializer. "
            "Input prefix: %s. Error: %s",
            byte_input[:100],
            err
        )
        return None


def safe_json_serializer(
        input_value: Any
) -> Optional[bytes]:
    """Safely serializes an input value to UTF-8 encoded JSON bytes.

    Handles None directly by returning None.

    If the input is already bytes, it delegates to an internal handler which
    assumes it *might* be UTF-8 encoded JSON. It logs a warning, attempts to
    decode/parse it, and then re-encodes it. If this fails, it logs an error
    and returns None.

    For other types, it attempts to serialize them using json.dumps().
    If serialization fails (e.g., due to non-serializable types), it logs
    a TypeError and returns None. Catches other unexpected serialization
    errors as well.

    Args:
        input_value: The value to serialize. Can be None, bytes (attempted
                     to be treated as JSON), or any JSON-serializable
                     Python type.

    Returns:
        The UTF-8 encoded byte representation of the input serialized as JSON,
        or None if any step fails (input is None, bytes aren't valid JSON,
        type error during serialization, or other unexpected errors).
    """
    if input_value is None:
        return None

    if isinstance(input_value, bytes):
        return _handle_bytes_input_for_json(input_value)

    try:
        return json.dumps(input_value).encode('utf-8')
    except TypeError as err:
        logger.error(
            "Failed to JSON serialize audit value of type %s. "
            "Value snippet: %s. Error: %s",
            type(input_value).__name__,
            repr(input_value)[:100],
            err
        )
        return None
    except Exception as err:  # pylint: disable=W0703
        logger.error(
            "Unexpected error during JSON serialization. "
            "Value type: %s. Error: %s",
            type(input_value).__name__,
            err
        )
        return None


def safe_string_serializer(
        input_str: Any
) -> Optional[bytes]:
    """Safely serializes an input value to UTF-8 encoded bytes as a string.

    Handles None and existing bytes input directly. Other types are converted
    to their string representation using str() before being encoded to UTF-8.

    Note:
        If the input object's __str__ method raises an exception, this function
        will propagate that exception. It only handles None and bytes types
        explicitly for safety before attempting the string conversion.

    Args:
        input_str: The value to serialize. Can be None, bytes, or any object
                   that can be converted to a string via str().

    Returns:
        The UTF-8 encoded byte representation of the input's string form,
        the original bytes if input was already bytes, or None if the input
        was None.
    """
    if input_str is None:
        return None
    if isinstance(input_str, bytes):
        return input_str
    return str(input_str).encode('utf-8')
