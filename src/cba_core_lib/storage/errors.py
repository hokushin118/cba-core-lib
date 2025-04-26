"""
Custom errors for File Storage.

This module defines custom exception classes for file storage operations.
"""
from __future__ import annotations

from typing import Optional, Dict, Any


######################################################################
#  FILE STORAGE CUSTOM ERRORS
######################################################################
class FileStorageError(Exception):
    """Exception class for file storage-related errors.

    This exception is raised when an error occurs during interaction with
    the file storage system (e.g., S3, MinIO, Azure blob). It encompasses
    issues such as connection problems, bucket access failures,
    and upload/download errors.

    Attributes:
        message (Optional[str]): A descriptive error message (optional).
    """

    def __init__(self, message: Optional[str] = None) -> None:
        """Initialize a FileStorageError exception.

        Args:
            message: An optional, detailed error message. If omitted,
                the exception will have an empty string as its message.
        """
        self.message = message
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the exception to a dictionary representation.

        This method provides a structured format for the error information,
        suitable for API responses or logging.

        Returns:
            A dictionary with a single key, 'error', whose value is the
            error message string.
        """
        return {'error': self.message}

    def __str__(self) -> str:
        """Return the error message as a string.

        This method is called when the exception instance is converted to
        a string, such as when it is printed.

        Returns:
            The error message, or an empty string if no message was provided
            during initialization.
        """
        return self.message or ''
