"""
Http utility functions.

This module contains utility functions to REST API.
"""
import hashlib
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class UnsupportedMediaTypeError(ValueError):
    """Raised when an unexpected media type is encountered."""

    def __init__(
            self,
            expected_media_type: str,
            actual_media_type: Optional[str]
    ) -> None:
        """Initialize the UnsupportedMediaTypeError.

        Args:
            expected_media_type (str): The expected media type.
            actual_media_type (Optional[str]): The actual media type received,
            or None if not available.
        """
        self.expected = expected_media_type
        self.actual = actual_media_type

        message = f"Unsupported Media Type. Expected '{expected_media_type}'"
        if actual_media_type is not None:
            message += f" but received '{actual_media_type}'."
        else:
            message += '.'

        super().__init__(message)


######################################################################
#  HTTP UTILITY FUNCTIONS
######################################################################
def validate_content_type(
        expected_media_type: str,
        actual_content_type: Optional[str]
) -> None:
    """Validates that the actual content type matches the expected one.

    Args:
        expected_media_type: The expected Content-Type string
        (e.g., "application/json").
        actual_content_type: The actual Content-Type string received,
        or None if missing.

    Raises:
        UnsupportedMediaTypeError: If the actual content type does not match
        the expected type or if it's missing when one is expected.

    Usage:
        # In a FastAPI route handler
        @app.post("/api/data")
        async def create_data(request: Request):
            # Get the content type from the request headers
            content_type = request.headers.get("Content-Type")
            
            # Validate that it's JSON
            validate_content_type("application/json", content_type)
            
        # In a Flask route handler
        @app.route("/api/data", methods=["POST"])
        def create_data():
            # Get the content type from the request headers
            content_type = request.headers.get("Content-Type")
            
            # Validate that it's JSON
            validate_content_type("application/json", content_type)
    """
    # Check if the content type matches (case-insensitive check might be
    # desired depending on strictness)
    if actual_content_type and actual_content_type == expected_media_type:
        logger.debug(
            "Content-Type '%s' matches expected '%s'.",
            actual_content_type,
            expected_media_type
        )
        return

    logger.warning(
        "Invalid Content-Type. Expected: '%s', Received: '%s'",
        expected_media_type,
        actual_content_type
    )
    raise UnsupportedMediaTypeError(expected_media_type, actual_content_type)


def generate_etag_hash(data: dict) -> str:
    """Generates an ETag hash for the given data.

    This function calculates the MD5 hash of the string representation of the
    input dictionary.  The resulting hash can be used as an ETag for caching
    purposes.

    Args:
        data: A dictionary representing the data to be hashed.

    Returns:
        A string representing the hexadecimal MD5 hash of the data.
    """
    data_str = str(data).encode('utf-8')  # Encode to bytes before hashing
    return hashlib.md5(data_str).hexdigest()  # Hash the data
