"""
Audit configs.

This module defines configuration classes for Audit. It provides
a structured way to manage Audit settings, ensuring consistency and
readability across the application.
"""
import logging
from dataclasses import dataclass
from typing import Optional, Callable

logger = logging.getLogger(__name__)


######################################################################
# AUDIT CONFIGURATION
######################################################################
@dataclass(frozen=True)
class AuditConfig:
    """Audit configuration settings.

    This class provides a read-only container for Audit settings,
    ensuring that the configuration remains consistent throughout the
    application.

    Attributes:
        audit_topic (str):
            The name of the Kafka topic to which audit logs will be published.
            This is a mandatory parameter.
        event_source (Optional[str]):
            An optional identifier for the source of the audit event
            (e.g., "account-service", "auth-service").  If provided,
            this will be included in each audit log message.

    Raises:
        ValueError:
            If `audit_topic` is not provided.

    This class is immutable.
    """

    audit_topic: str
    """The name of the Kafka topic to which audit logs will be published.
       This is a mandatory parameter.
    """

    event_source: Optional[str] = None
    """An optional identifier for the source of the audit event
       (e.g., "account-service", "auth-service").  If provided, this will be
       included in each audit log message.
    """

    user_identifier_func: Optional[Callable[[], Optional[str]]] = None
    """Optional function to retrieve the current user's ID.
       This is intended to be used by framework-specific adapters
       (e.g., for Flask, it would retrieve the user ID from the
       request context). The function should take no arguments and
       return an optional string representing the user ID.
    """

    def __post_init__(self):
        """Validates the configuration after initialization.

        Raises:
            ValueError: If `audit_topic` is not provided.
        """
        if not self.audit_topic:
            raise ValueError(
                'audit_topic must be provided in AuditConfig'
            )
