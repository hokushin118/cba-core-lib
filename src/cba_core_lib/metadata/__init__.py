"""
Package: metadata.

CBA Core Library - File Metadata Utilities.
"""
from __future__ import annotations

from cba_core_lib.metadata.configs import MongoConfig
from cba_core_lib.metadata.errors import MetadataError
from cba_core_lib.metadata.schemas import UploadMetadataDTO
from cba_core_lib.metadata.services import MetadataStorageService

__all__ = [
    'MongoConfig',
    'MetadataError',
    'UploadMetadataDTO',
    'MetadataStorageService',
]
