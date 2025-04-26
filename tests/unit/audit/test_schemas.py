"""
Audit Schemas Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
from datetime import datetime

import pytest
from pydantic import ValidationError
from pytest import fixture

from cba_core_lib.audit.schemas import (
    AuditEvent,
    AuditRequestDetails,
    AuditResponseDetails,
    AuditErrorDetails
)
from tests.conftest import TEST_USER_ID, TEST_SERVICE_NAME


######################################################################
#  AUDIT SCHEMAS UNIT TEST CASES
######################################################################
class TestAuditRequestDetails:
    """Tests for the AuditRequestDetails model."""

    @fixture
    def request_data(self):
        """Provides a sample dictionary representing audit request
        details data for testing."""
        return {
            'method': 'GET',
            'url': 'http://example.com/',
            'headers': {'Content-Type': 'application/json'},
            'body': {'key': 'value'},
            'client_ip': '192.0.2.1',
        }

    def test_valid_request_details(self):
        """It should create a valid AuditRequestDetails instance."""
        request_data = {
            'method': 'GET',
            'url': 'http://example.com/',
        }
        request_details = AuditRequestDetails(**request_data)
        assert request_details.method == 'GET'
        assert str(request_details.url) == 'http://example.com/'

    def test_valid_request_details_with_optional_fields(
            self,
            request_data
    ):
        """It should create a valid AuditRequestDetails instance
        with all fields."""
        request_details = AuditRequestDetails(**request_data)
        assert request_details.method == 'GET'
        assert str(request_details.url) == 'http://example.com/'
        assert request_details.headers == {'Content-Type': 'application/json'}
        assert request_details.body == {'key': 'value'}
        assert request_details.client_ip == '192.0.2.1'

    def test_missing_required_field_request(self):
        """It should raise ValidationError if a required field is missing."""
        with pytest.raises(ValidationError):
            AuditRequestDetails(
                url='http://example.com'
            )  # Missing method

    def test_invalid_url_request(self):
        """It should raise ValidationError if the URL is invalid."""
        with pytest.raises(ValidationError):
            AuditRequestDetails(
                method='GET',
                url='invalid_url'
            )

    def test_invalid_headers_type_request(self):
        """It should raise ValidationError if headers is not a dict."""
        with pytest.raises(ValidationError):
            AuditRequestDetails(
                method='GET',
                url='http://example.com',
                headers='not_a_dict'
            )

    def test_invalid_body_type_request(self):
        """It should not raise error if body type is not a dict."""
        try:
            AuditRequestDetails(
                method='GET',
                url='http://example.com',
                body=123
            )
        except Exception as err:  # pylint: disable=W0703
            assert False, f"Raised an exception {err}"

    def test_invalid_client_ip_type_request(self):
        """It should not raise error if client_ip type is not a string."""
        try:
            AuditRequestDetails(
                method='GET',
                url='http://example.com',
                client_ip=None
            )
        except Exception as err:  # pylint: disable=W0703
            assert False, f"Raised an exception {err}"

    def test_audit_request_details_immutability(
            self,
            request_data
    ):
        """It should ensure AuditRequestDetails instance is immutable."""
        request_details = AuditRequestDetails(**request_data)
        with pytest.raises(ValidationError):
            request_details.method = 'new-method'
        with pytest.raises(ValidationError):
            request_details.url = 'new-url'
        with pytest.raises(ValidationError):
            request_details.headers = {'new-key': 'new-value'}
        with pytest.raises(ValidationError):
            request_details.body = {'new-key', 'new-value'}
        with pytest.raises(ValidationError):
            request_details.client_ip = '0.0.0.0'


class TestAuditResponseDetails:
    """The AuditResponseDetails Class Tests."""

    @fixture
    def response_data(self):
        """Provides a sample dictionary representing audit response
        details data for testing."""
        return {
            'status_code': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': {'result': 'success'},
        }

    def test_valid_response_details(self):
        """It should create a valid AuditResponseDetails instance."""
        response_data = {'status_code': 200}
        response_details = AuditResponseDetails(**response_data)
        assert response_details.status_code == 200

    def test_valid_response_details_with_optional_fields(
            self,
            response_data
    ):
        """It should create a valid AuditResponseDetails
        instance with all fields."""
        response_details = AuditResponseDetails(**response_data)
        assert response_details.status_code == 200
        assert response_details.headers == {"Content-Type": "application/json"}
        assert response_details.body == {"result": "success"}

    def test_missing_status_code_response(self):
        """It should raise ValidationError if status_code is missing."""
        with pytest.raises(ValidationError):
            AuditResponseDetails()

    def test_invalid_status_code_type_response(self):
        """It should raise ValidationError if status_code is not an integer."""
        with pytest.raises(ValidationError):
            AuditResponseDetails(
                status_code='invalid'
            )

    def test_invalid_headers_type_response(self):
        """It should raise ValidationError if headers is not a dict."""
        with pytest.raises(ValidationError):
            AuditResponseDetails(
                status_code=200,
                headers='not_a_dict'
            )

    def test_invalid_body_type_response(self):
        """It should not raise error if body type is not a dict."""
        try:
            AuditResponseDetails(
                status_code=200,
                body=123
            )
        except Exception as err:  # pylint: disable=W0703
            assert False, f"Raised an exception {err}"

    def test_audit_response_details_immutability(
            self,
            response_data
    ):
        """It should ensure AuditResponseDetails instance is immutable."""
        response_details = AuditResponseDetails(**response_data)
        with pytest.raises(ValidationError):
            response_details.status_code = 'new-status-code'
        with pytest.raises(ValidationError):
            response_details.headers = {'new-key': 'new-value'}
        with pytest.raises(ValidationError):
            response_details.body = {'new-key', 'new-value'}


class TestAuditErrorDetails:
    """The AuditErrorDetails Class Tests."""

    @fixture
    def error_data(self):
        """Provides a sample dictionary representing audit error
        details data for testing."""
        return {
            'type': 'ValueError',
            'message': 'Invalid value provided.',
            'traceback': 'Traceback (most recent call last):\n  File ...',
        }

    def test_valid_error_details(self):
        """It should create a valid AuditErrorDetails instance."""
        error_data = {
            'type': 'ValueError',
            'message': 'Invalid value provided.',
        }
        error_details = AuditErrorDetails(**error_data)
        assert error_details.type == 'ValueError'
        assert error_details.message == 'Invalid value provided.'

    def test_valid_error_details_with_traceback(
            self,
            error_data
    ):
        """It should create a valid AuditErrorDetails instance
        with traceback."""
        error_details = AuditErrorDetails(**error_data)
        assert error_details.type == 'ValueError'
        assert error_details.message == 'Invalid value provided.'
        assert error_details.traceback == \
               'Traceback (most recent call last):\n  File ...'

    def test_missing_type_error(self):
        """It should raise ValidationError if type is missing."""
        with pytest.raises(ValidationError):
            AuditErrorDetails(message='Missing type')

    def test_missing_message_error(self):
        """It should raise ValidationError if message is missing."""
        with pytest.raises(ValidationError):
            AuditErrorDetails(type='TypeError')

    def test_invalid_type_type_error(self):
        """It should raise ValidationError if type is not a string."""
        with pytest.raises(ValidationError):
            AuditErrorDetails(
                type=123,
                message='Invalid type'
            )

    def test_invalid_message_type_error(self):
        """It should raise ValidationError if message is not a string."""
        with pytest.raises(ValidationError):
            AuditErrorDetails(
                type='TypeError',
                message=123
            )

    def test_invalid_traceback_type_error(self):
        """It should raise ValidationError if traceback is not a string."""
        with pytest.raises(ValidationError):
            AuditErrorDetails(
                type='TypeError',
                message='Test message',
                traceback=123
            )

    def test_audit_error_details_immutability(
            self,
            error_data
    ):
        """It should ensure AuditErrorDetails instance is immutable."""
        error_details = AuditErrorDetails(**error_data)
        with pytest.raises(ValidationError):
            error_details.type = 'new-type'
        with pytest.raises(ValidationError):
            error_details.message = 'new-message'
        with pytest.raises(ValidationError):
            error_details.traceback = 'new-trace-back'


class TestAuditEvent:
    """The AuditEvent Class Tests."""

    now = datetime.now()

    @fixture
    def event_data(self):
        """Provides a sample dictionary representing audit error
        details data for testing."""
        return {
            'timestamp': self.now,
            'correlation_id': 'test_correlation_id',
            'event_type': 'test_event_type',
            'status': 'success',
            'duration_ms': 100.0,
        }

    def test_valid_audit_event(
            self,
            event_data
    ):
        """It should create a valid AuditEvent instance with all
        required fields."""
        audit_event = AuditEvent(**event_data)
        assert audit_event.timestamp == self.now
        assert audit_event.correlation_id == 'test_correlation_id'
        assert audit_event.event_type == 'test_event_type'
        assert audit_event.status == 'success'
        assert audit_event.duration_ms == 100.0

    def test_valid_audit_event_with_optional_fields(self):
        """It should create a valid AuditEvent instance with all fields,
        including optional ones."""
        now = datetime.now()
        request_data = {
            'method': 'GET',
            'url': 'http://example.com',
        }
        response_data = {'status_code': 200}
        error_data = {'type': 'TestError', 'message': 'A test error occurred.'}
        event_data = {
            'timestamp': now,
            'correlation_id': 'test_correlation_id',
            'event_type': 'test_event_type',
            'status': 'success',
            'duration_ms': 100.0,
            'user_id': TEST_USER_ID,
            'event_source': TEST_SERVICE_NAME,
            'request': AuditRequestDetails(**request_data),
            'response': AuditResponseDetails(**response_data),
            'error': AuditErrorDetails(**error_data),
            'custom_data': {'key1': 'value1'},
        }
        audit_event = AuditEvent(**event_data)
        assert audit_event.timestamp == now
        assert audit_event.correlation_id == 'test_correlation_id'
        assert audit_event.event_type == 'test_event_type'
        assert audit_event.status == 'success'
        assert audit_event.duration_ms == 100.0
        assert audit_event.user_id == TEST_USER_ID
        assert audit_event.event_source == TEST_SERVICE_NAME
        assert audit_event.request.method == 'GET'
        assert audit_event.response.status_code == 200
        assert audit_event.error.type == 'TestError'
        assert audit_event.custom_data == {'key1': 'value1'}

    def test_missing_required_field(self):
        """It should raise ValidationError if a required field is missing."""
        with pytest.raises(ValidationError):
            AuditEvent(
                correlation_id='test_correlation_id',
                event_type='test_event_type',
                status='success',
                duration_ms=100.0,
                timestamp=None
            )  # Missing timestamp

    def test_invalid_field_type(self):
        """It should raise ValidationError if a field has an invalid type."""
        with pytest.raises(ValidationError):
            AuditEvent(
                timestamp=datetime.now(),
                correlation_id='test_correlation_id',
                event_type='test_event_type',
                status='success',
                duration_ms='invalid',  # Should be a float, not a string
            )

    def test_request_response_error_validation(self):
        """It should raise ValidationError if request, response,
        or error details are invalid."""
        now = datetime.now()
        with pytest.raises(ValidationError):
            AuditEvent(
                timestamp=now,
                correlation_id='test_correlation_id',
                event_type='test_event_type',
                status='success',
                duration_ms=100.0,
                request={'invalid': 'data'},  # Invalid request data
            )

        with pytest.raises(ValidationError):
            AuditEvent(
                timestamp=now,
                correlation_id='test_correlation_id',
                event_type='test_event_type',
                status='success',
                duration_ms=100.0,
                response={'invalid': 'data'},  # Invalid response data
            )

        with pytest.raises(ValidationError):
            AuditEvent(
                timestamp=now,
                correlation_id='test_correlation_id',
                event_type='test_event_type',
                status='success',
                duration_ms=100.0,
                error={'invalid': 'data'},  # Invalid error data
            )

    def test_audit_event_immutability(
            self,
            event_data
    ):
        """It should ensure AuditEvent instance is immutable."""
        audit_event = AuditEvent(**event_data)
        with pytest.raises(ValidationError):
            audit_event.timestamp = datetime.now()
        with pytest.raises(ValidationError):
            audit_event.correlation_id = 'new-correlation_id'
        with pytest.raises(ValidationError):
            audit_event.event_type = 'new-event_type'
        with pytest.raises(ValidationError):
            audit_event.status = 'new-status'
        with pytest.raises(ValidationError):
            audit_event.duration_ms = 200.0
