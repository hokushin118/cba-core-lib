"""
This module provides a Flask-specific adapter for integrating the cba-core-lib's
AuditLogger into Flask applications. It simplifies the process of auditing
HTTP requests by offering utilities to extract request and response data
from Flask's objects and a decorator factory to automate the auditing of
Flask route execution.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from functools import wraps
from json import JSONDecodeError
from typing import Any, Optional, Callable, Union, Dict, TYPE_CHECKING

from cba_core_lib.utils.common import generate_correlation_id
from cba_core_lib.utils.constants import ANONYMOUS_USER_NAME
from cba_core_lib.utils.enums import Status

# Core library imports (adjust path as needed)
try:
    from cba_core_lib.audit.core import AuditLogger
    from cba_core_lib.audit.schemas import (
        AuditEvent,
        AuditRequestDetails,
        AuditResponseDetails,
        AuditErrorDetails
    )

    _core_lib_audit_available = True
except ImportError as core_import_err:
    logging.getLogger(__name__).error(
        "Failed to import core audit library components: %s. "
        "FlaskAuditAdapter cannot function.",
        core_import_err,
        exc_info=True
    )
    _core_lib_audit_available = False
    AuditLogger = Any
    AuditEvent = Any
    AuditRequestDetails = Any
    AuditResponseDetails = Any
    AuditErrorDetails = Any

# Default event type for HTTP requests logged by this adapter
DEFAULT_EVENT_TYPE = 'http_request'

# Default headers to exclude from logging
DEFAULT_EXCLUDE_HEADERS = {
    'authorization',
    'cookie',
    'set-cookie',
    'proxy-authorization'
}

# This block is ignored at runtime but used by type checkers
if TYPE_CHECKING:
    from flask import Response as FlaskResponse

logger = logging.getLogger(__name__)


######################################################################
# FAST API AUDIT ADAPTER
######################################################################

######################################################################
# FLASK AUDIT ADAPTER
######################################################################
class FlaskAuditAdapter:
    """Provides Flask-specific integration for the core AuditLogger.

    This adapter includes helper methods to extract data from Flask's request
    and response objects and provides a decorator factory
    (`create_audit_decorator`) to automatically audit Flask route execution.
    It relies on an instance of the core, framework-independent
    `AuditLogger` for configuration and sending events to Kafka.

    Flask dependencies are loaded lazily only when this adapter is instantiated
    or its methods requiring Flask context are called.
    """
    _flask_response_cls: Optional[type] = None

    def __init__(
            self,
            audit_logger: AuditLogger
    ) -> None:
        """Initializes the Flask Audit Adapter.

        Performs necessary checks and attempts to import Flask components.

        Args:
            audit_logger: An initialized instance of the core AuditLogger.
                         This logger holds the audit configuration and the
                         KafkaProducerManager instance.

        Raises:
            RuntimeError: If the core audit library components are missing,
                          or if the Flask framework is not installed when
                          this adapter is instantiated.
            TypeError: If audit_logger is not an instance of AuditLogger.
        """
        if not _core_lib_audit_available:
            raise RuntimeError(
                'Core audit library components (AuditLogger, models) '
                'could not be imported. FlaskAuditAdapter cannot initialize.'
            )

        if not isinstance(
                audit_logger,
                AuditLogger
        ):
            raise TypeError(
                'The audit_logger must be an instance of the AuditLogger.'
            )

        try:
            # pylint: disable=C0415
            from flask import Response as FlaskResponseCls
            # Store the imported Response class for later isinstance checks
            self._flask_response_cls = FlaskResponseCls
            logger.debug(
                'Flask successfully imported for FlaskAuditAdapter.'
            )
        except ImportError as err:
            logger.error(
                "Flask framework import failed during F"
                "laskAuditAdapter init: %s",
                err
            )
            raise RuntimeError(
                'Flask framework is required to use FlaskAuditAdapter, but it '
                'could not be imported.'
            ) from err

        self.audit_logger = audit_logger
        self.config = audit_logger.config

        logger.info(
            'FlaskAuditAdapter initialized, using AuditLogger.'
        )

    def _get_flask_component(
            self,
            component_name: str
    ) -> Any:
        """Safely imports a component from Flask, raising RuntimeError if
        unavailable.

        This method attempts to import a specified component from the
        Flask library. It handles the import process and raises a
        `RuntimeError` if the import fails. This is designed to provide
        more informative error handling when dealing with optional Flask
        dependencies.

        Args:
            component_name: The name of the Flask component to import (e.g.,
                'request', 'current_app', 'Response').

        Returns:
            The imported Flask component.

        Raises:
            RuntimeError: If the specified Flask component cannot be imported.
                The original `ImportError` is chained to this exception for
                debugging purposes.
        """
        try:
            if component_name == 'request':
                from flask import request  # pylint: disable=C0415
                return request

            if component_name == 'current_app':
                from flask import current_app  # pylint: disable=C0415
                return current_app

            if component_name == 'Response':
                if self._flask_response_cls:
                    return self._flask_response_cls

                # pylint: disable=C0415
                from flask import Response as FlaskResponseCls
                self._flask_response_cls = FlaskResponseCls
                return self._flask_response_cls

            raise ImportError(
                f"Unknown Flask component requested: {component_name}"
            )

        except ImportError as err:
            logger.critical(
                "Failed to lazy-import Flask component '%s' unexpectedly.",
                component_name
            )
            raise RuntimeError(
                f"Required Flask component {component_name} could not be imported."
            ) from err

    ######################################################################
    # FRAMEWORK-SPECIFIC DATA EXTRACTION HELPERS
    ######################################################################
    def _get_request_body(
            self
    ) -> Union[dict, str, None, Any]:
        """Safely extracts and potentially parses the Flask request body.

        Attempts to decode the body as text and parse it as JSON if the
        Content-Type header indicates JSON. Returns raw text, parsed JSON,
        a bytes representation placeholder, or an error message string.

        Returns:
            The processed request body (dict for JSON, str for text/failed
            JSON parse), None if no body, a placeholder string for decoding
            errors, or an error message string for other exceptions.
        """
        # Lazy import request
        request = self._get_flask_component('request')
        if not request:
            logger.warning(
                'Flask request context not available in _get_request_body.'
            )
            return None

        try:
            raw_body = request.get_data()
            if not raw_body:
                return None

            try:
                body_text = raw_body.decode(
                    request.charset or 'utf-8',
                    errors='replace'
                )
                processed_body = body_text

                content_type = request.content_type or ''
                if 'application/json' in content_type.lower():
                    try:
                        processed_body = json.loads(body_text)
                    except (
                            JSONDecodeError,
                            TypeError
                    ) as err:
                        logger.warning(
                            "Request Content-Type is JSON but failed to "
                            "parse body: %s",
                            err,
                            exc_info=False
                        )

            except Exception as err:  # pylint: disable=W0703
                logger.error(
                    "Error decoding Flask request body: %s",
                    err,
                    exc_info=True
                )
                return f"<bytes: {len(raw_body)}>"

        except Exception as err:  # pylint: disable=W0703
            logger.error(
                "Error reading Flask request data (e.g., get_data failed): %s",
                err,
                exc_info=True
            )
            return '<error reading request body>'

        return processed_body

    def _filter_headers(
            self,
            headers: Dict[str, str]
    ) -> Dict[str, str]:
        """Filters sensitive headers from a dictionary.

        Uses the `exclude_request_headers` list configured in the core
        `AuditConfig` (case-insensitive).

        Args:
            headers: The original dictionary of headers.

        Returns:
            A new dictionary with excluded headers removed.
        """
        # Get excluded headers from the config attached to the core logger
        excluded_lower = {
            h.lower() for h in getattr(
                self.config,
                'exclude_request_headers',
                DEFAULT_EXCLUDE_HEADERS
            )
        }

        return {
            k: v for k, v in headers.items()
            if isinstance(k, str) and k.lower() not in excluded_lower
        }

    def _extract_request_details(
            self
    ) -> AuditRequestDetails:
        """Extracts relevant details from the current Flask `request` object.

        Returns:
            An `AuditRequestDetails` instance populated with data from the
            current Flask request context. Returns a minimally populated
            instance if the request context is unavailable.
        """
        # Lazy import request
        request = self._get_flask_component('request')
        if not request:
            logger.warning(
                'Flask request context not available for extracting details.'
            )
            return AuditRequestDetails()

        method = getattr(request, 'method', '<unknown method>')
        url = getattr(request, 'url', '<unknown url>')
        client_ip = getattr(request, 'remote_addr', '<unknown client_ip>')

        try:
            headers = dict(request.headers) if hasattr(
                request,
                'headers'
            ) else {}

        except Exception as err:  # pylint: disable=W0703
            logger.warning(
                "Could not extract Flask request headers: %s",
                err
            )
            headers = {'error': 'could not extract headers'}

        return AuditRequestDetails(
            method=method,
            url=url,
            headers=self._filter_headers(headers),
            body=self._get_request_body(),
            client_ip=client_ip,
        )

    def _get_response_body(
            self,
            response: Optional['FlaskResponse']
    ) -> Union[dict, str, None, Any]:
        """Safely extracts and potentially parses the Flask response body.

        Attempts to decode the body as text and parse it as JSON if the
        mimetype indicates JSON. Returns raw text, parsed JSON,
        a bytes representation placeholder, or an error message string.

        Args:
            response: The Flask Response object, or None.

        Returns:
            The processed response body (dict for JSON, str for
            text/failed JSON parse), None if no response or no body,
            a placeholder string for decoding errors, or an error message
            string for other exceptions.
        """
        if not self._flask_response_cls:
            logger.error(
                'Flask Response class not available for _get_response_body check.'
            )
            return '<error: Flask Response class unavailable>'

        if not response or not isinstance(response, self._flask_response_cls):
            return None

        try:
            raw_body = response.get_data()
            if not raw_body:
                return None

            try:
                body_text = raw_body.decode(
                    response.charset or 'utf-8',
                    errors='replace'
                )
                processed_body = body_text

                mimetype = response.mimetype or ''
                if 'application/json' in mimetype.lower():
                    try:
                        processed_body = json.loads(body_text)
                    except (
                            JSONDecodeError,
                            TypeError
                    ) as err:
                        logger.warning(
                            "Response Content-Type is JSON but failed "
                            "to parse body: %s",
                            err,
                            exc_info=False
                        )

            except Exception as err:  # pylint: disable=W0703
                logger.error(
                    "Error decoding Flask response body: %s",
                    err,
                    exc_info=True
                )
                return f"<bytes: {len(raw_body)}>"

        except Exception as err:  # pylint: disable=W0703
            logger.error(
                "Error reading Flask response data "
                "(e.g., get_data failed): %s",
                err,
                exc_info=True
            )
            return '<error reading response body>'

        return processed_body

    def _extract_response_details(
            self,
            response: Optional['FlaskResponse']
    ) -> Optional[AuditResponseDetails]:
        """Extracts relevant details from the Flask Response object and
        populates an AuditResponseDetails model.

        Args:
            response: The Flask Response object, or None if the request failed
                      before generating a response.

        Returns:
            An AuditResponseDetails instance, or None if no response was
            provided.
        """
        if not response:
            return None

        status_code = getattr(
            response,
            'status_code',
            None
        )

        try:
            headers = dict(response.headers) if hasattr(
                response,
                'headers'
            ) else {}

        except Exception as err:  # pylint: disable=W0703
            logger.warning(
                "Could not extract Flask response headers: %s",
                err
            )
            headers = {'error': 'could not extract headers'}

        excluded_lower = {
            h.lower() for h in getattr(
                self.config,
                'exclude_response_headers',
                DEFAULT_EXCLUDE_HEADERS
            )
        }

        filtered_headers = {
            k: v for k, v in headers.items()
            if isinstance(k, str) and k.lower() not in excluded_lower
        }

        return AuditResponseDetails(
            status_code=status_code,
            headers=filtered_headers,
            body=self._get_response_body(response),
        )

    def _get_user_id_from_context(
            self
    ) -> Optional[str]:
        """Gets the user identifier by calling the function configured in
        `AuditConfiguration.user_identifier_func`, ensuring Flask app
        context is available.

        Returns:
            The user ID string, or None if not found or an error occurred.
            The core logger will use the configured anonymous ID if this
            returns None.
        """
        # Lazy import current_app
        current_app = self._get_flask_component('current_app')

        user_id_func = getattr(
            self.config,
            'user_identifier_func',
            None
        )

        if callable(user_id_func):
            try:
                # Ensure Flask app context is pushed if function needs it
                if current_app:
                    with current_app.app_context():
                        user_id = user_id_func()
                        # Ensure result is string or None
                        return str(user_id) if user_id is not None else None
                else:
                    logger.warning(
                        'Cannot get user ID: No Flask application context.'
                    )
                    return None
            except Exception as err:  # pylint: disable=W0703
                logger.warning(
                    "Configured user identifier function failed: %s.",
                    err,
                    exc_info=False
                )
                return None
        else:
            logger.debug(
                'No user_identifier_func configured in AuditConfiguration.'
            )
            return None

    ######################################################################
    # DECORATOR FACTORY
    ######################################################################
    def create_audit_decorator(
            self,
            event_type: str = DEFAULT_EVENT_TYPE
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Creates a Flask decorator for automatically auditing route
        execution.

        This decorator wraps a Flask route function. It records timing,
        extracts request/response details, handles errors, constructs an
        `AuditEvent` object, and uses the core `AuditLogger` to send it
        asynchronously to Kafka.

        Args:
            event_type: A string identifying the type of event logged by this
                        decorator (default: 'http_request').

        Returns:
            A decorator function that can be applied to Flask route functions.
        """
        # Check core lib availability early
        if not _core_lib_audit_available:
            logger.error(
                'Cannot create audit decorator: Core audit library '
                'components missing.'
            )

            def dummy_decorator(f):
                return f

            return dummy_decorator

        # Check Response class availability early if needed for the decorator logic itself
        if not self._flask_response_cls:
            logger.error(
                'Flask Response class unavailable, cannot create functional audit decorator.'
            )

            def dummy_decorator(f):
                return f

            return dummy_decorator

        def decorator(
                function: Callable[..., Any]
        ) -> Callable[..., Any]:
            """The actual decorator that wraps the Flask route function."""

            @wraps(function)
            # pylint: disable=R0914
            def wrapper(
                    *args: Any,
                    **kwargs: Any
            ) -> Any:
                """The wrapper function executed for each request to the
                decorated route."""
                # Lazy import current_app and Response class
                # reference inside the wrapper
                current_app = self._get_flask_component('current_app')
                response_cls = self._flask_response_cls

                if not current_app:
                    logger.error(
                        'Audit skipped: Flask application context not available.'
                    )
                    return function(*args, **kwargs)

                response: Optional[Any] = None
                start_time = time.monotonic()
                correlation_id = generate_correlation_id()
                status = Status.SUCCESS.value
                error_obj: Optional[AuditErrorDetails] = None
                req_details: Optional[AuditRequestDetails] = None
                resp_details: Optional[AuditResponseDetails] = None
                user_id: Optional[str] = None

                try:
                    # 1. Extract Request Details & User ID
                    req_details = self._extract_request_details()
                    user_id = self._get_user_id_from_context()

                    # 2. Execute Decorated Function
                    response = function(*args, **kwargs)

                    # 3. Check if response is a Flask Response using
                    # stored class
                    if not isinstance(response, response_cls):
                        logger.debug(
                            'Decorated function did not return a Flask '
                            'Response object.'
                        )

                    return response

                except Exception as err:  # pylint: disable=W0703
                    # 4. Handle Exception from Decorated Function
                    status = Status.FAILURE.value
                    error_obj = AuditErrorDetails(
                        type=type(err).__name__,
                        message=str(err)
                    )
                    logger.debug(
                        'Caught exception in decorated function for audit '
                        'correlation_id: %s: %s',
                        correlation_id,
                        type(err).__name__
                    )
                    raise err
                finally:
                    # 5. Build and Log Audit Event
                    end_time = time.monotonic()
                    try:
                        # Ensure response details are extracted only for
                        # Flask Response objects
                        if status == Status.SUCCESS.value \
                                and response is not None \
                                and isinstance(response, response_cls):
                            resp_details = self._extract_response_details(
                                response
                            )

                        # Get user ID again if it was None (might be set
                        # later in request) Ensure context is checked within
                        # this method if needed
                        if user_id is None:
                            user_id = self._get_user_id_from_context() \
                                      or ANONYMOUS_USER_NAME

                        # Use the user id as the key (encoded as UTF-8) to help
                        # with ordered partitioning
                        kafka_key_method = getattr(
                            req_details,
                            'method',
                            'UNKNOWN'
                        )
                        kafka_key = f"{user_id}-{kafka_key_method}".encode(
                            'utf-8'
                        )

                        logger.debug("Kafka key: %s", kafka_key)

                        # Create the AuditEvent instance
                        audit_event = AuditEvent(
                            timestamp=datetime.utcnow().isoformat() + 'Z',
                            correlation_id=correlation_id,
                            event_source=self.config.event_source,
                            event_type=event_type,
                            user_id=user_id,
                            status=status,
                            duration_ms=round(
                                (end_time - start_time) * 1000,
                                2
                            ),
                            request=req_details,
                            response=resp_details,
                            error=error_obj
                        )

                        logger.debug("Audit event: %s", audit_event)

                        # Log the complete audit event using the audit logger
                        self.audit_logger.log_event(
                            audit_event,
                            kafka_key
                        )

                    except Exception as err:  # pylint: disable=W0703
                        logger.error(
                            "Critical error during final audit logging, "
                            "attempt correlation_id: %s: %s",
                            correlation_id,
                            err
                        )

            return wrapper

        return decorator
