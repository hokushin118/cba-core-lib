"""
Schemas for Audit logging.

All schemas are stored in this module.
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, HttpUrl

logger = logging.getLogger(__name__)


######################################################################
# SCHEMAS
######################################################################
class AuditRequestDetails(BaseModel):
    """Model for request details.

    Attributes:
        method (str): The HTTP method of the request (e.g., 'GET', 'POST').
        url (HttpUrl): The URL of the request.
        headers (Optional[Dict[str, str]]):  Optional dictionary of request
        headers.
        body (Optional[Any]):  Optional body of the request.  The type can vary
            depending on the content type (e.g., dict, str).
        client_ip (Optional[str]): Optional IP address of the client making
        the request.
    """
    method: str
    url: HttpUrl
    headers: Optional[Dict[str, str]] = None
    body: Optional[Any] = None
    client_ip: Optional[str] = None

    class ConfigDict:
        """Config class."""
        # pylint: disable=too-few-public-methods
        from_attributes = True
        # Allows the model to be populated by aliases
        populate_by_name = True


class AuditResponseDetails(BaseModel):
    """Model for response details.

    Attributes:
        status_code (int): The HTTP status code of the response
        (e.g., 200, 404, 500).
        headers (Optional[Dict[str, str]]): Optional dictionary of response
        headers.
        body (Optional[Any]):  Optional body of the response. The type can vary
            depending on the content type.
    """
    status_code: int
    headers: Optional[Dict[str, str]] = None
    body: Optional[Any] = None

    class ConfigDict:
        """Config class."""
        # pylint: disable=too-few-public-methods
        from_attributes = True
        # Allows the model to be populated by aliases
        populate_by_name = True


class AuditErrorDetails(BaseModel):
    """Model for error details.

    Attributes:
        type (str): The type of error (e.g., 'ValueError',
        'TypeError').
        message (str): A human-readable error message.
        traceback (Optional[str]):  Optional traceback string providing context
            for the error.
    """
    type: str
    message: str
    traceback: Optional[str] = None

    class ConfigDict:
        """Config class."""
        # pylint: disable=too-few-public-methods
        from_attributes = True
        # Allows the model to be populated by aliases
        populate_by_name = True


class AuditEvent(BaseModel):
    """Model for audit events.

    Attributes:
        timestamp (datetime): The timestamp of when the event occurred.
        correlation_id (str):  A unique identifier for the event, used for
            tracking across services.
        event_type (str):  A string describing the type of event
            (e.g., 'request.received', 'user.login', 'database.query').
        status (str):  The status of the event (e.g., 'success', 'failure',
        'pending').
        duration_ms (float):  The duration of the event in milliseconds.
        user_id (Optional[str]):  Optional identifier for the user associated
        with the event.
        event_source (Optional[str]):  Optional identifier for the service or
            component that generated the event.
        request (Optional[AuditRequestDetails]):  Optional details about the
            incoming request, if applicable.
        response (Optional[AuditResponseDetails]): Optional details about the
            outgoing response, if applicable.
        error (Optional[AuditErrorDetails]):  Optional details about any error
            that occurred during the event.
        custom_data (Optional[Dict[str, Any]]):  Optional dictionary for
        including any additional, application-specific data.
    """
    timestamp: datetime
    correlation_id: str
    event_type: str
    status: str
    duration_ms: float
    user_id: Optional[str] = None
    event_source: Optional[str] = None
    request: Optional[AuditRequestDetails] = None
    response: Optional[AuditResponseDetails] = None
    error: Optional[AuditErrorDetails] = None
    custom_data: Optional[Dict[str, Any]] = None

    class ConfigDict:
        """Config class."""
        # pylint: disable=too-few-public-methods
        from_attributes = True
        # Allows the model to be populated by aliases
        populate_by_name = True
