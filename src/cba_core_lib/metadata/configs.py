"""
File Metadata configs.

This module defines configuration classes for File Metadata. It provides
a structured way to manage File Metadata settings, ensuring consistency and
readability across the application.
"""
from __future__ import annotations

import logging

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


######################################################################
# MONGODB CONFIGURATION
######################################################################
class MongoConfig(BaseSettings):
    """Encapsulates MongoDB configuration settings (Pydantic V2).

    Retrieves settings from environment variables with sensible defaults.
    Secrets are handled using Pydantic's SecretStr type.

    This class is immutable.
    """

    uri: SecretStr
    """The MongoDB connection URI."""

    db_name: str
    """The name of the MongoDB database."""

    model_config = SettingsConfigDict(
        env_prefix='mongo_',
        case_sensitive=False,
        extra='ignore',
        frozen=True,
    )
