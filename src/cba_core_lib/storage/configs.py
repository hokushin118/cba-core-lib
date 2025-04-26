"""
File Storage configs.

This module defines configuration classes for File Storage. It provides
a structured way to manage File Storage settings, ensuring consistency and
readability across the application.
"""
from __future__ import annotations

import logging

from pydantic import AnyHttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


######################################################################
#  MINIO CONFIGURATION
######################################################################
class MinioConfig(BaseSettings):
    """Encapsulates MinIO file storage configuration settings (Pydantic V2).

    Retrieves settings from environment variables with sensible defaults.
    Secrets are handled using Pydantic's SecretStr type.

    This class is immutable.
    """

    endpoint: AnyHttpUrl
    """The MinIO server endpoint URL."""

    access_key: SecretStr
    """The MinIO access key."""

    secret_key: SecretStr
    """The MinIO secret key."""

    use_ssl: bool = False
    """Whether to use SSL for the MinIO connection."""

    model_config = SettingsConfigDict(
        env_prefix='minio_',
        case_sensitive=False,
        extra='ignore',
        frozen=True,
    )
