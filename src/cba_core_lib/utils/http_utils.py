"""
Utility functions.

This module contains utility functions to REST API.
"""
import hashlib
import json
import logging
from typing import Any, Optional, Mapping

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
#  UTILITY FUNCTIONS
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


def generate_etag_hash(data: Mapping[str, Any]) -> str:
    """Generates a stable ETag hash (MD5) for the given data structure.

    This function calculates the MD5 hash of the JSON string representation
    of the input data structure (sorting keys for stability). The resulting
    hash can be used as an ETag for HTTP caching.

    Args:
        data: A dictionary or other mapping representing the data to be hashed.

    Returns:
        A string representing the hexadecimal MD5 hash of the data's stable
        JSON representation.

    Raises:
        TypeError: If the input is not a dictionary or cannot be serialized to JSON.
        
    Usage:
        # Basic usage with a simple dictionary
        user_data = {"id": 123, "name": "John Doe", "email": "john@example.com"}
        etag = generate_etag_hash(user_data)
        # etag will be something like "a1b2c3d4e5f6g7h8i9j0..."
        
        # Using with FastAPI for HTTP caching
        from fastapi import FastAPI, Response
        from fastapi.responses import JSONResponse
        
        app = FastAPI()
        
        @app.get("/users/{user_id}")
        def get_user(user_id: int, response: Response):
            # Fetch user data from database
            user_data = {"id": user_id, "name": "John Doe", "email": "john@example.com"}
            
            # Generate ETag for the response
            etag = generate_etag_hash(user_data)
            
            # Set the ETag in the response headers
            response.headers["ETag"] = f'"{etag}"'
            
            return JSONResponse(content=user_data)
            
        # Using with Flask for HTTP caching
        from flask import Flask, jsonify, make_response
        
        app = Flask(__name__)
        
        @app.route("/users/<int:user_id>")
        def get_user(user_id):
            # Fetch user data from database
            user_data = {"id": user_id, "name": "John Doe", "email": "john@example.com"}
            
            # Generate ETag for the response
            etag = generate_etag_hash(user_data)
            
            # Create response with ETag
            response = make_response(jsonify(user_data))
            response.headers["ETag"] = f'"{etag}"'
            
            return response
            
        # Using for conditional requests (If-None-Match)
        @app.get("/users/{user_id}")
        def get_user_if_modified(user_id: int, request: Request, response: Response):
            # Fetch user data from database
            user_data = {"id": user_id, "name": "John Doe", "email": "john@example.com"}
            
            # Generate ETag for the response
            etag = generate_etag_hash(user_data)
            
            # Check if client has a cached version
            if_none_match = request.headers.get("If-None-Match")
            if if_none_match and if_none_match.strip('"') == etag:
                # Client's cached version is still valid
                return Response(status_code=304)  # Not Modified
                
            # Client needs the updated data
            response.headers["ETag"] = f'"{etag}"'
            return JSONResponse(content=user_data)
    """
    # Validate that the input is a dictionary
    if not isinstance(data, dict):
        logger.error(
            "Failed to generate ETag hash: input must be a dictionary, got %s",
            type(data).__name__
        )
        raise TypeError(
            f"Input must be a dictionary, got {type(data).__name__}")

    try:
        # Use JSON dumps with sorted keys for a stable representation
        # Ensure non-ASCII characters are handled correctly
        # (ensure_ascii=False) and separators are compact.
        data_str = json.dumps(
            data,
            sort_keys=True,
            ensure_ascii=False,
            separators=(',', ':')
        )
        encoded_str = data_str.encode('utf-8')
        return hashlib.md5(encoded_str).hexdigest()
    except TypeError as err:
        logger.error(
            "Failed to generate ETag hash due to JSON serialization error: %s",
            err
        )
        raise  # Re-raise the TypeError
