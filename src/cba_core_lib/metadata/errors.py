"""
Custom errors for File Metadata.

This module defines custom exception classes for file metadata operations,
providing specific error types for better error handling and debugging.
"""
from __future__ import annotations

from typing import Optional, Dict, Any


######################################################################
#  FILE METADATA CUSTOM ERRORS
######################################################################
class MetadataError(Exception):
    """Exception class for file metadata-related errors.

    This exception serves as a general category for errors that occur
    during interactions with the file metadata storage system (e.g.,
    MongoDB, PostgreSQL). It can encompass issues such as connection
    problems, access failures (e.g., to collections or tables), and
    errors during create, read, update, or delete (CRUD) operations
    on metadata.

    Attributes:
        message (Optional[str]): A descriptive error message (optional).
    """

    def __init__(self, message: Optional[str] = None) -> None:
        """Initialize a MetadataError exception.

        Args:
            message: An optional, detailed error message providing
                specific information about the error. If omitted, the
                exception's message will default to an empty string.
        """
        self.message = message
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the exception to a dictionary representation.

        This method provides a structured format for the error information,
        making it easier to serialize for API responses, logging, or
        other structured output.

        Returns:
            Dict[str, Any]: A dictionary containing the error information.
            Currently, it has a single key 'error' with the error
            message as its value. Future versions might include more
            structured error details.
        """
        return {'error': self.message}

    def __str__(self) -> str:
        """Return the error message as a string.

        This method is invoked when the exception instance needs to be
        represented as a string, such as when it's printed or used in
        string formatting.

        Returns:
            str: The error message. If no message was provided during
            initialization, an empty string is returned.
        """
        return self.message or ''
