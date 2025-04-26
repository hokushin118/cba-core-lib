"""
Package: utils.

Package for common utility functions.
"""
from __future__ import annotations

from cba_core_lib.utils import status
from cba_core_lib.utils.common import generate_correlation_id
from cba_core_lib.utils.constants import (
    AUTHORIZATION_HEADER,
    BEARER_HEADER,
)
from cba_core_lib.utils.enums import UserRole, HTTPSchema
from cba_core_lib.utils.env_utils import (
    get_enum_from_env,
    get_bool_from_env,
    get_int_from_env
)
from cba_core_lib.utils.http_utils import (
    validate_content_type,
    generate_etag_hash,
    UnsupportedMediaTypeError
)

__all__ = [
    # Constants
    'AUTHORIZATION_HEADER',
    'BEARER_HEADER',
    # Enums
    'UserRole',
    'HTTPSchema',
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
    # Common utilities
    'generate_correlation_id',
]
