"""
Package: utils.

Package for common utility functions.
"""
from .constants import (
    AUTHORIZATION_HEADER,
    BEARER_HEADER,
)
from .enums import UserRole
from .env_utils import (
    get_enum_from_env,
    get_bool_from_env,
    get_int_from_env
)
from .http_utils import (
    validate_content_type,
    generate_etag_hash,
    UnsupportedMediaTypeError
)
from . import status

__all__ = [
    # Constants
    'AUTHORIZATION_HEADER',
    'BEARER_HEADER',
    # Enums
    'UserRole',
    # Environment utilities
    'get_enum_from_env',
    'get_bool_from_env',
    'get_int_from_env',
    # HTTP utilities
    'validate_content_type',
    'generate_etag_hash',
    'UnsupportedMediaTypeError',
    # Status module
    'status',
]
