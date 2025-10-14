"""
Schemas for Audit logging.

All schemas are stored in this module.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, HttpUrl, ConfigDict, Field

logger = logging.getLogger(__name__)


######################################################################
#  AUDIT SCHEMAS
######################################################################
class AuditRequestDetails(BaseModel):
    """Model for request details.

    Attributes:
        method (str): The HTTP method of the request (e.g., 'GET', 'POST').
        url (HttpUrl): The URL of the request.
        headers (Optional[Dict[str, str]]):  Optional dictionary of request
        headers.
        body (Optional[Any]):  Optional body of the request. The type can vary
            depending on the content type (e.g., dict, str).
        client_ip (Optional[str]): Optional IP address of the client making
        the request.
    """
    method: str = Field(
        ...,
        description='The HTTP method of the request.',
        examples=['GET', 'POST', 'PUT', 'DELETE']
    )
    url: HttpUrl = Field(
        ...,
        description='The URL of the request.',
        examples=['http://localhost/api/examples']
    )
    headers: Optional[Dict[str, str]] = Field(
        None,
        description='Dictionary of request headers.',
        examples=[{
            'Content-Type': 'application/json',
            'Authorization': 'Bearer YOUR_TOKEN'
        }]
    )
    body: Optional[Any] = Field(
        None,
        description='Body of the request. The type can vary \
            depending on the content type (e.g., dict, str).',
        examples=['Some body']
    )
    client_ip: Optional[str] = Field(
        None,
        description='IP address of the client making the request.',
        examples=['172.10.10.1']
    )

    model_config = ConfigDict(
        # Extra fields in the input data will be silently ignored
        extra='ignore',
        # Allow model creation from attributes
        from_attributes=True,
        # Allows the model to be populated by aliases
        populate_by_name=True,
        # True. Instances of this class are immutable
        frozen=True,
    )


class AuditResponseDetails(BaseModel):
    """Model for response details.

    Attributes:
        status_code (int): The HTTP status code of the response
        (e.g., 200, 404, 500).
        headers (Optional[Dict[str, str]]): Optional dictionary of response
        headers.
        body (Optional[Any]):  Optional body of the response. The type can vary
            depending on the content type (e.g., dict, str).
    """
    status_code: int = Field(
        ...,
        description='The HTTP status code of the response.',
        examples=[200, 400, 404]
    )
    headers: Optional[Dict[str, str]] = Field(
        None,
        description='Dictionary of response headers.',
        examples=[{
            'Content-Type': 'application/json',
            'Authorization': 'Bearer YOUR_TOKEN'
        }]
    )
    body: Optional[Any] = Field(
        None,
        description='Body of the response. The type can vary \
            depending on the content type (e.g., dict, str).',
        examples=['Some body']
    )

    model_config = ConfigDict(
        # Extra fields in the input data will be silently ignored
        extra='ignore',
        # Allow model creation from attributes
        from_attributes=True,
        # Allows the model to be populated by aliases
        populate_by_name=True,
        # True. Instances of this class are immutable
        frozen=True,
    )


class AuditErrorDetails(BaseModel):
    """Model for error details.

    Attributes:
        type (str): The type of error (e.g., 'ValueError',
        'TypeError').
        message (str): A human-readable error message.
        traceback (Optional[str]):  Optional traceback string providing context
            for the error.
    """
    type: str = Field(
        ...,
        description="The type of error (e.g., 'ValueError', 'TypeError')",
        examples=['ValueError', 'TypeError']
    )
    message: str = Field(
        ...,
        description='A human-readable error message.',
        examples=['An error occurred while writing...']
    )
    traceback: Optional[str] = Field(
        None,
        description='Traceback string providing context for the error.',
        examples=['Traceback of an error...']
    )

    model_config = ConfigDict(
        # Extra fields in the input data will be silently ignored
        extra='ignore',
        # Allow model creation from attributes
        from_attributes=True,
        # Allows the model to be populated by aliases
        populate_by_name=True,
        # True. Instances of this class are immutable
        frozen=True,
    )


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
    timestamp: datetime = Field(
        ...,
        description='The timestamp of when the event occurred.',
        examples=['2024-08-28T10:00:00Z']
    )
    correlation_id: str = Field(
        ...,
        description='A unique identifier for the event, used for '
                    'tracking across services.',
        examples=['4ae289ca-39a6-4bd5-8030-c3d82c7928b1']
    )
    event_type: str = Field(
        ...,
        description="A string describing the type of event (e.g., "
                    "'http_request', 'user.login', 'database.query').",
        examples=['http_request']
    )
    status: str = Field(
        ...,
        description="The status of the event.",
        examples=['SUCCESS', 'FAILURE', 'PENDING']
    )
    duration_ms: float = Field(
        ...,
        description="The duration of the event in milliseconds.",
        examples=[70.12]
    )
    user_id: Optional[str] = Field(
        None,
        description='Identifier of the user associated with the event.',
        examples=['13b914bb-443f-4319-97a9-7a7bb5fdb9f3']
    )
    event_source: Optional[str] = Field(
        None,
        description='Identifier for the service or component that '
                    'generated the event.',
        examples=['Account REST API Service']
    )
    request: Optional[AuditRequestDetails] = Field(
        None,
        description='Details about the incoming request.',
    )
    response: Optional[AuditResponseDetails] = Field(
        None,
        description='Details about the outgoing response.',
    )
    error: Optional[AuditErrorDetails] = Field(
        None,
        description='Details about any error that occurred during the event.',
    )
    custom_data: Optional[Dict[str, Any]] = Field(
        None,
        description='Dictionary for including any additional, '
                    'application-specific data.',
    )

    model_config = ConfigDict(
        # Extra fields in the input data will be silently ignored
        extra='ignore',
        # Allow model creation from attributes
        from_attributes=True,
        # Allows the model to be populated by aliases
        populate_by_name=True,
        # True. Instances of this class are immutable
        frozen=True,
    )
