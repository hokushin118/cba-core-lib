import json
import logging
import time
from datetime import datetime
from functools import wraps
from json import JSONDecodeError
from typing import Any, Optional, Callable, Union

from ..utils.common import generate_correlation_id
from ..utils.constants import COOKIES_NAME, AUTHORIZATION_HEADER, \
    ANONYMOUS_USER_NAME
from ..utils.enums import Status

logger = logging.getLogger(__name__)

# Framework specific imports - handled with try-except for robustness
try:
    from flask import request, Response, current_app

    _flask_installed = True
except ImportError as flask_import_err:
    _flask_installed = False
    Response = Any
    request = None
    current_app = None
    logger.error(
        "Failed to import Flask framework library components: %s. "
        "FlaskAuditAdapter functionality will be limited.",
        flask_import_err,
        exc_info=True
    )

# Core library imports (adjust path as needed)
try:
    from .core import AuditLogger
    from .schemas import (
        AuditEvent,
        AuditRequestDetails,
        AuditResponseDetails,
        AuditErrorDetails
    )

    _core_lib_audit_available = True
except ImportError as core_import_err:
    logger.error(
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

DEFAULT_EVENT_TYPE = 'http_request'


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
    """

    def __init__(
            self,
            audit_logger: AuditLogger
    ) -> None:
        """Initializes the Flask Audit Adapter.

        Args:
            audit_logger: An initialized instance of the core AuditLogger.
                         This logger holds the audit configuration and the
                         KafkaProducerManager instance.

        Raises:
            RuntimeError: If Flask is not installed.
            TypeError: If audit_logger is not an instance of AuditLogger.
        """
        if not _flask_installed:
            raise RuntimeError(
                'Flask framework is required to use FlaskAuditAdapter.'
            )
        if not _core_lib_audit_available:
            raise RuntimeError(
                'Core audit library components (AuditLogger, models) '
                'could not be imported.'
            )
        if not isinstance(
                audit_logger,
                AuditLogger
        ):
            raise TypeError(
                'The audit_logger must be an instance of the AuditLogger.'
            )
        self.audit_logger = audit_logger
        self.config = audit_logger.config

        logger.info(
            'FlaskAuditAdapter initialized, using AuditLogger.'
        )

    ######################################################################
    # FRAMEWORK-SPECIFIC DATA EXTRACTION HELPERS
    ######################################################################
    def _get_request_body(
            self
    ) -> Union[dict, str, None, Any]:  # More specific return type hint
        """Safely extracts and potentially parses the Flask request body.

        Attempts to decode the body as text and parse it as JSON if the
        Content-Type header indicates JSON. Returns raw text, parsed JSON,
        a bytes representation placeholder, or an error message string.

        Returns:
            The processed request body (dict for JSON, str for text/failed
            JSON parse), None if no body, a placeholder string for decoding
            errors, or an error message string for other exceptions.
        """
        if not _flask_installed or not request:
            logger.warning(
                "Flask request context not available in _get_request_body."
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

    def _extract_request_details(
            self
    ) -> AuditRequestDetails:
        """Extracts relevant details from the current Flask request object
        and populates an AuditRequestDetails model.

        Returns:
            An AuditRequestDetails instance containing extracted information.
            Returns an empty instance if no request context is found.
        """
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
            headers=headers,
            body=self._get_request_body(),
            client_ip=client_ip,
        )

    def _get_response_body(
            self,
            response: Optional[Response]
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
        if not _flask_installed:
            logger.warning(
                'Flask not installed, cannot process response body.'
            )
            return None

        if not response or not isinstance(response, Response):
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
            response: Optional[Response]
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

        return AuditResponseDetails(
            status_code=status_code,
            headers=headers,
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
    ) -> Callable[[Callable[..., Response]], Callable[..., Response]]:
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
        if not _core_lib_audit_available:
            logger.error(
                'Cannot create audit decorator: Core audit library '
                'components missing.'
            )

            # Return a dummy decorator that does nothing
            def dummy_decorator(f):
                return f

            return dummy_decorator

        def decorator(
                function: Callable[..., Response]
        ) -> Callable[..., Response]:
            """The actual decorator that wraps the Flask route function."""

            @wraps(function)
            def wrapper(
                    *args: Any,
                    **kwargs: Any
            ) -> Response:
                """The wrapper function executed for each request to the
                decorated route."""
                # Ensure we are in a Flask context before proceeding
                if not _flask_installed or not current_app:
                    logger.error(
                        'Audit skipped: Flask application context '
                        'not available.'
                    )
                    return function(*args, **kwargs)

                response: Optional[Response] = None
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
                    # Ensure the returned value is a Flask Response object
                    if not isinstance(response, Response):
                        logger.debug(
                            'Decorated function did not return a Flask '
                            'Response object.'
                        )

                    return response

                except Exception as err:  # pylint: disable=W0703
                    # 3. Handle Exception from Decorated Function
                    status = Status.FAILURE.value
                    response = self._generate_error_response(err)
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
                    # 4. Build and Log Audit Event
                    end_time = time.monotonic()
                    try:
                        if response is not None:
                            resp_details = self._extract_response_details(
                                response
                            )

                            # Prepare headers without sensitive authorization
                            # information
                            req_details.headers.pop(
                                AUTHORIZATION_HEADER,
                                None
                            )

                            req_details.headers.pop(
                                COOKIES_NAME,
                                None
                            )

                        if user_id is None:
                            user_id = self._get_user_id_from_context()

                        if user_id is None:
                            user_id = ANONYMOUS_USER_NAME

                        # Use the user id as the key (encoded as UTF-8) to help
                        # with ordered partitioning
                        kafka_key = f"{user_id}-{req_details.method}".encode(
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
