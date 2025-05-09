"""
Package: cba_core_lib.

Reusable utilities for Python microservices.
"""
from __future__ import annotations

from cba_core_lib.audit.adapters import FlaskAuditAdapter
from cba_core_lib.audit.configs import AuditConfig
from cba_core_lib.audit.core import AuditLogger
from cba_core_lib.kafka.configs import (
    SecurityProtocol,
    AutoOffsetReset,
    KafkaConsumerConfig,
    KafkaProducerConfig,
)
from cba_core_lib.kafka.consumer import KafkaConsumerManager
from cba_core_lib.kafka.producer import KafkaProducerManager
from cba_core_lib.kafka.utils import (
    safe_string_serializer,
    safe_json_serializer,
)
from cba_core_lib.logging.log_handlers import init_logging
from cba_core_lib.storage.configs import MinioConfig
from cba_core_lib.storage.errors import FileStorageError
from cba_core_lib.storage.schemas import FileUploadData, SimpleFileData
from cba_core_lib.storage.services import FileStorageService, MinioService
from cba_core_lib.utils.common import generate_correlation_id
from cba_core_lib.utils.constants import (
    AUTHORIZATION_HEADER,
    BEARER_HEADER,
)
from cba_core_lib.utils.enums import UserRole, HTTPSchema
from cba_core_lib.utils.status import (
    # Informational - 1xx
    HTTP_100_CONTINUE,
    HTTP_101_SWITCHING_PROTOCOLS,
    # Successful - 2xx
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_202_ACCEPTED,
    HTTP_203_NON_AUTHORITATIVE_INFORMATION,
    HTTP_204_NO_CONTENT,
    HTTP_205_RESET_CONTENT,
    HTTP_206_PARTIAL_CONTENT,
    # Redirection - 3xx
    HTTP_300_MULTIPLE_CHOICES,
    HTTP_301_MOVED_PERMANENTLY,
    HTTP_302_FOUND,
    HTTP_303_SEE_OTHER,
    HTTP_304_NOT_MODIFIED,
    HTTP_305_USE_PROXY,
    HTTP_306_RESERVED,
    HTTP_307_TEMPORARY_REDIRECT,
    # Client Error - 4xx
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_402_PAYMENT_REQUIRED,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_405_METHOD_NOT_ALLOWED,
    HTTP_406_NOT_ACCEPTABLE,
    HTTP_407_PROXY_AUTHENTICATION_REQUIRED,
    HTTP_408_REQUEST_TIMEOUT,
    HTTP_409_CONFLICT,
    HTTP_410_GONE,
    HTTP_411_LENGTH_REQUIRED,
    HTTP_412_PRECONDITION_FAILED,
    HTTP_413_REQUEST_ENTITY_TOO_LARGE,
    HTTP_414_REQUEST_URI_TOO_LONG,
    HTTP_415_UNSUPPORTED_MEDIA_TYPE,
    HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
    HTTP_417_EXPECTATION_FAILED,
    HTTP_428_PRECONDITION_REQUIRED,
    HTTP_429_TOO_MANY_REQUESTS,
    HTTP_431_REQUEST_HEADER_FIELDS_TOO_LARGE,
    # Server Error - 5xx
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_501_NOT_IMPLEMENTED,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
    HTTP_505_HTTP_VERSION_NOT_SUPPORTED,
    HTTP_511_NETWORK_AUTHENTICATION_REQUIRED,
)

__version__ = '1.0.21'

__all__ = [
    # HTTP Status Codes
    'HTTP_100_CONTINUE',
    'HTTP_101_SWITCHING_PROTOCOLS',
    'HTTP_200_OK',
    'HTTP_201_CREATED',
    'HTTP_202_ACCEPTED',
    'HTTP_203_NON_AUTHORITATIVE_INFORMATION',
    'HTTP_204_NO_CONTENT',
    'HTTP_205_RESET_CONTENT',
    'HTTP_206_PARTIAL_CONTENT',
    'HTTP_300_MULTIPLE_CHOICES',
    'HTTP_301_MOVED_PERMANENTLY',
    'HTTP_302_FOUND',
    'HTTP_303_SEE_OTHER',
    'HTTP_304_NOT_MODIFIED',
    'HTTP_305_USE_PROXY',
    'HTTP_306_RESERVED',
    'HTTP_307_TEMPORARY_REDIRECT',
    'HTTP_400_BAD_REQUEST',
    'HTTP_401_UNAUTHORIZED',
    'HTTP_402_PAYMENT_REQUIRED',
    'HTTP_403_FORBIDDEN',
    'HTTP_404_NOT_FOUND',
    'HTTP_405_METHOD_NOT_ALLOWED',
    'HTTP_406_NOT_ACCEPTABLE',
    'HTTP_407_PROXY_AUTHENTICATION_REQUIRED',
    'HTTP_408_REQUEST_TIMEOUT',
    'HTTP_409_CONFLICT',
    'HTTP_410_GONE',
    'HTTP_411_LENGTH_REQUIRED',
    'HTTP_412_PRECONDITION_FAILED',
    'HTTP_413_REQUEST_ENTITY_TOO_LARGE',
    'HTTP_414_REQUEST_URI_TOO_LONG',
    'HTTP_415_UNSUPPORTED_MEDIA_TYPE',
    'HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE',
    'HTTP_417_EXPECTATION_FAILED',
    'HTTP_428_PRECONDITION_REQUIRED',
    'HTTP_429_TOO_MANY_REQUESTS',
    'HTTP_431_REQUEST_HEADER_FIELDS_TOO_LARGE',
    'HTTP_500_INTERNAL_SERVER_ERROR',
    'HTTP_501_NOT_IMPLEMENTED',
    'HTTP_502_BAD_GATEWAY',
    'HTTP_503_SERVICE_UNAVAILABLE',
    'HTTP_504_GATEWAY_TIMEOUT',
    'HTTP_505_HTTP_VERSION_NOT_SUPPORTED',
    'HTTP_511_NETWORK_AUTHENTICATION_REQUIRED',
    # Authorization Constants
    'AUTHORIZATION_HEADER',
    'BEARER_HEADER',
    # Enums
    'UserRole',
    'HTTPSchema',
    # Audit Components
    'AuditConfig',
    'AuditLogger',
    'FlaskAuditAdapter',
    # Kafka Components
    'SecurityProtocol',
    'AutoOffsetReset',
    'KafkaConsumerConfig',
    'KafkaProducerConfig',
    'KafkaConsumerManager',
    'KafkaProducerManager',
    'safe_string_serializer',
    'safe_json_serializer',
    # Common utilities
    'generate_correlation_id',
    # Logging
    'init_logging',
    # File Storage
    'MinioConfig',
    'FileStorageError',
    'FileUploadData',
    'SimpleFileData',
    'FileStorageService',
    'MinioService',
]
