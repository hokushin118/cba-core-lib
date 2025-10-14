"""
Enums.

This module contains common enumerations used throughout the microservices.
These enums provide centralized definitions for consistent and type-safe
representation of predefined values.
"""
from __future__ import annotations

from enum import Enum


class UserRole(Enum):
    """Enumeration representing the different roles that users can have
    within the microservice.

    This enum provides a centralized and type-safe way to manage user
    permissions and access levels across various components of the system,
    ensuring consistency in role assignment and authorization checks.
    """
    USER = 'ROLE_USER'
    """Represents a standard, authenticated user with basic access
    privileges."""

    MODERATOR = 'ROLE_MODERATOR'
    """Represents a user with elevated privileges for content management
    or moderation tasks."""

    ADMIN = 'ROLE_ADMIN'
    """Represents a user with the highest level of privileges, typically
    for system administration."""

    def __str__(
            self
    ) -> str:
        """Returns the string value associated with the enum member."""
        return self.value


class Status(Enum):
    """Enumeration representing the different states or statuses of operations
    within the microservice.

    This enum provides a centralized and type-safe way to manage the
    various lifecycle stages or outcomes of processes within the system,
    ensuring consistency in status tracking and reporting.
    It includes values for successful completion, failure, pending initiation,
    active execution, and final completion (which could be success or failure).
    """
    SUCCESS = 'SUCCESS'
    """Indicates that an operation completed successfully."""

    FAILURE = 'FAILURE'
    """Indicates that an operation encountered an error and did not
    complete successfully."""

    PENDING = 'PENDING'
    """Indicates that an operation has been initiated but has not yet
    started execution."""

    RUNNING = 'RUNNING'
    """Indicates that an operation is currently in progress."""

    COMPLETED = 'COMPLETED'
    """Indicates that an operation has finished its execution, regardless of
    success or failure."""

    def __str__(
            self
    ) -> str:
        """Returns the string value associated with the enum member."""
        return self.value


class HTTPSchema(Enum):
    """Enumeration representing common HTTP schemes.

    This enum provides a type-safe way to define and refer to the
    standard HTTP and HTTPS protocols used for web communication.
    """
    HTTP = 'http://'
    """Represents the Hypertext Transfer Protocol (HTTP) scheme."""

    HTTPS = 'https://'
    """Represents the Hypertext Transfer Protocol Secure (HTTPS) scheme."""

    def __str__(
            self
    ) -> str:
        """Returns the string value associated with the enum member."""
        return self.value


class StorageProviderType(Enum):
    """Enumeration representing different storage providers supported
    by the microservice.

    This enum provides a centralized and type-safe way to refer to the
    various backend systems used for storing files. It ensures
    consistency throughout the application when specifying or handling
    different file storage implementations.
    """
    MINIO = 'minio'
    """Represents the MinIO object storage provider."""

    AWS = 'aws'
    """Represents the Amazon Web Services (AWS) Simple Storage Service (S3)
    provider."""

    AZURE = 'azure'
    """Represents the Microsoft Azure Blob Storage provider."""

    def __str__(
            self
    ) -> str:
        """Returns the string value associated with the enum member."""
        return self.value


class MetadataProviderType(Enum):
    """Enumeration representing different metadata storage providers supported
    by the microservice.

    This enum provides a centralized and type-safe way to refer to the
    various backend systems used for storing metadata. It ensures
    consistency throughout the application when specifying or handling
    different metadata storage implementations.
    """
    MONGO = 'mongo'
    """Represents the MongoDB metadata storage provider."""

    POSTGRES = 'postgres'
    """Represents the PostgreSQL metadata storage provider."""

    def __str__(
            self
    ) -> str:
        """Returns the string value associated with the enum member."""
        return self.value
