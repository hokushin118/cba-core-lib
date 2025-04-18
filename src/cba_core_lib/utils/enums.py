"""
Enums.

This module contains common enumerations used throughout the microservices.
These enums provide centralized definitions for consistent and type-safe
representation of predefined values.
"""
from enum import Enum


class UserRole(Enum):
    """Enumeration representing user roles within the microservice.

    This enum provides a centralized definition of user roles, ensuring
    consistency across different parts of the system.
    """
    USER = 'ROLE_USER'
    MODERATOR = 'ROLE_MODERATOR'
    ADMIN = 'ROLE_ADMIN'

    @property
    def role_value(self) -> str:
        """Returns the string value associated with the enum member.

        This property allows easy access to the string representation
        of the role, which is often required when interacting with external
        systems or databases.

        Returns:
            str: The string value of the role.
        """
        return self.value


class Status(Enum):
    """Enumeration representing statuses within the microservice.

    This enum provides a centralized definition of statuses, ensuring
    consistency across different parts of the system.  It includes
    values for success, failure, pending, running, and completed states.
    """
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'

    @property
    def status_value(self) -> str:
        """Returns the string value associated with the enum member.

        This property allows easy access to the string representation
        of the status.

        Returns:
            str: The string value of the status.
        """
        return self.value
