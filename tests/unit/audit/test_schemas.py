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

    def test_valid_schema(self):
        """It should create a valid schema instance."""
        request_data = {
            'method': 'GET',
            'url': 'http://example.com/',
        }
        request_details = AuditRequestDetails(**request_data)
        assert request_details.method == 'GET'
        assert str(request_details.url) == 'http://example.com/'

    def test_valid_schema_with_optional_fields(
            self,
            request_data
    ):
        """It should create a valid schema instance
        with all fields."""
        request_details = AuditRequestDetails(**request_data)
        assert request_details.method == 'GET'
        assert str(request_details.url) == 'http://example.com/'
        assert request_details.headers == {'Content-Type': 'application/json'}
        assert request_details.body == {'key': 'value'}
        assert request_details.client_ip == '192.0.2.1'

    @pytest.mark.parametrize(
        'missing_field',
        [
            'method',
            'url',
        ],
    )
    def test_schema_missing_required_field(
            self,
            missing_field,
            request_data
    ):
        """It should raise ValidationError if a required field is missing."""
        del request_data[missing_field]  # Remove the required field

        with pytest.raises(ValidationError) as excinfo:
            AuditRequestDetails(**request_data)

        assert f"{missing_field}" in str(
            excinfo.value
        ), f"Expected error for missing field '{missing_field}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, invalid_value',
        [
            ('method', 1),
            ('url', 'Invalid type'),
            ('headers', 'Not a dict'),
            ('client_ip', 1),
        ],
    )
    def test_schema_validation_error(
            self,
            field_name,
            invalid_value,
            request_data
    ):
        """It should raise ValidationError when specific fields
        are provided with invalid data types."""
        request_data[field_name] = invalid_value

        with pytest.raises(ValidationError) as excinfo:
            AuditRequestDetails(**request_data)

        assert f"{field_name}" in str(
            excinfo.value
        ), f"Expected error for field '{field_name}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('method', 'new-method'),
            ('url', 'new-url'),
            ('headers', {'new-key': 'new-value'}),
            ('body', {'new-key', 'new-value'}),
            ('client_ip', '0.0.0.0'),
        ],
    )
    def test_schema_immutability(
            self,
            request_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        schema = AuditRequestDetails(**request_data)
        with pytest.raises(ValidationError):
            setattr(schema, field_name, new_value)


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

    def test_valid_schema(self):
        """It should create a valid schema instance."""
        response_data = {'status_code': 200}
        response_details = AuditResponseDetails(**response_data)
        assert response_details.status_code == 200

    def test_valid_schema_with_optional_fields(
            self,
            response_data
    ):
        """It should create a valid schema
        instance with all fields."""
        response_details = AuditResponseDetails(**response_data)
        assert response_details.status_code == 200
        assert response_details.headers == {"Content-Type": "application/json"}
        assert response_details.body == {"result": "success"}

    @pytest.mark.parametrize(
        'missing_field',
        [
            'status_code',
        ],
    )
    def test_schema_missing_required_field(
            self,
            missing_field,
            response_data
    ):
        """It should raise ValidationError if a required field is missing."""
        del response_data[missing_field]  # Remove the required field

        with pytest.raises(ValidationError) as excinfo:
            AuditResponseDetails(**response_data)

        assert f"{missing_field}" in str(
            excinfo.value
        ), f"Expected error for missing field '{missing_field}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, invalid_value',
        [
            ('status_code', 'Invalid type'),
            ('headers', 'Not a dict'),
        ],
    )
    def test_schema_validation_error(
            self,
            field_name,
            invalid_value,
            response_data
    ):
        """It should raise ValidationError when specific fields
        are provided with invalid data types."""
        response_data[field_name] = invalid_value

        with pytest.raises(ValidationError) as excinfo:
            AuditResponseDetails(**response_data)

        assert f"{field_name}" in str(
            excinfo.value
        ), f"Expected error for field '{field_name}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('status_code', 'new-status-code'),
            ('headers', {'new-key': 'new-value'}),
            ('body', {'new-key', 'new-value'}),
        ],
    )
    def test_schema_immutability(
            self,
            response_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        schema = AuditResponseDetails(**response_data)
        with pytest.raises(ValidationError):
            setattr(schema, field_name, new_value)


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

    def test_valid_schema(self):
        """It should create a valid schema instance."""
        error_data = {
            'type': 'ValueError',
            'message': 'Invalid value provided.',
        }
        error_details = AuditErrorDetails(**error_data)
        assert error_details.type == 'ValueError'
        assert error_details.message == 'Invalid value provided.'

    def test_valid_schema_with_optional_fields(
            self,
            error_data
    ):
        """It should create a valid schema instance
        with optional fields."""
        error_details = AuditErrorDetails(**error_data)
        assert error_details.type == 'ValueError'
        assert error_details.message == 'Invalid value provided.'
        assert error_details.traceback == \
               'Traceback (most recent call last):\n  File ...'

    @pytest.mark.parametrize(
        'missing_field',
        [
            'type',
            'message',
        ],
    )
    def test_schema_missing_required_field(
            self,
            missing_field,
            error_data
    ):
        """It should raise ValidationError if a required field is missing."""
        del error_data[missing_field]  # Remove the required field

        with pytest.raises(ValidationError) as excinfo:
            AuditErrorDetails(**error_data)

        assert f"{missing_field}" in str(
            excinfo.value
        ), f"Expected error for missing field '{missing_field}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, invalid_value',
        [
            ('type', 1),
            ('message', 1),
            ('traceback', 1),
        ],
    )
    def test_schema_validation_error(
            self,
            field_name,
            invalid_value,
            error_data
    ):
        """It should raise ValidationError when specific fields
        are provided with invalid data types."""
        error_data[field_name] = invalid_value

        with pytest.raises(ValidationError) as excinfo:
            AuditErrorDetails(**error_data)

        assert f"{field_name}" in str(
            excinfo.value
        ), f"Expected error for field '{field_name}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('type', 'new-type'),
            ('message', 'new-message'),
            ('traceback', 'new-traceback'),
        ],
    )
    def test_schema_immutability(
            self,
            error_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        schema = AuditErrorDetails(**error_data)
        with pytest.raises(ValidationError):
            setattr(schema, field_name, new_value)


class TestAuditEvent:
    """The AuditEvent Class Tests."""

    now = datetime.now()

    @fixture
    def event_data(self):
        """Provides a sample dictionary representing audit error
        details data for testing."""
        request_data = {
            'method': 'GET',
            'url': 'http://example.com',
        }
        response_data = {'status_code': 200}
        error_data = {'type': 'TestError', 'message': 'A test error occurred.'}
        return {
            'timestamp': self.now,
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

    def test_valid_schema(self):
        """It should create a valid schema instance with all
        required fields."""
        event_data = {
            'timestamp': self.now,
            'correlation_id': 'test_correlation_id',
            'event_type': 'test_event_type',
            'status': 'success',
            'duration_ms': 100.0,
        }
        schema = AuditEvent(**event_data)
        assert schema.timestamp == self.now
        assert schema.correlation_id == 'test_correlation_id'
        assert schema.event_type == 'test_event_type'
        assert schema.status == 'success'
        assert schema.duration_ms == 100.0

    def test_valid_schema_with_optional_fields(
            self,
            event_data
    ):
        """It should create a valid schema instance with all fields,
        including optional ones."""
        schema = AuditEvent(**event_data)
        assert schema.timestamp == self.now
        assert schema.correlation_id == 'test_correlation_id'
        assert schema.event_type == 'test_event_type'
        assert schema.status == 'success'
        assert schema.duration_ms == 100.0
        assert schema.user_id == TEST_USER_ID
        assert schema.event_source == TEST_SERVICE_NAME
        assert schema.request.method == 'GET'
        assert schema.response.status_code == 200
        assert schema.error.type == 'TestError'
        assert schema.custom_data == {'key1': 'value1'}

    @pytest.mark.parametrize(
        'missing_field',
        [
            'timestamp',
            'correlation_id',
            'event_type',
            'status',
            'duration_ms',
        ],
    )
    def test_schema_missing_required_field(
            self,
            missing_field,
            event_data
    ):
        """It should raise ValidationError if a required field is missing."""
        del event_data[missing_field]  # Remove the required field

        with pytest.raises(ValidationError) as excinfo:
            AuditEvent(**event_data)

        assert f"{missing_field}" in str(
            excinfo.value
        ), f"Expected error for missing field '{missing_field}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, invalid_value',
        [
            ('timestamp', 'Invalid type'),
            ('correlation_id', 123),
            ('event_type', 1),
            ('status', 1),
            ('duration_ms', 'Invalid type'),
            ('user_id', 123),
            ('event_source', 123),
            ('request', {'invalid': 'data'}),
            ('response', {'invalid': 'data'}),
            ('error', {'invalid': 'data'}),
        ],
    )
    def test_schema_validation_error(
            self,
            field_name,
            invalid_value,
            event_data
    ):
        """It should raise ValidationError when specific fields
        are provided with invalid data types."""
        event_data[field_name] = invalid_value

        with pytest.raises(ValidationError) as excinfo:
            AuditEvent(**event_data)

        assert f"{field_name}" in str(
            excinfo.value
        ), f"Expected error for field '{field_name}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('timestamp', datetime.now()),
            ('correlation_id', 'new-correlation_id'),
            ('event_type', 'new-event_type'),
            ('status', 'new-status'),
            ('duration_ms', 200.0),
        ],
    )
    def test_schema_immutability(
            self,
            event_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        schema = AuditEvent(**event_data)
        with pytest.raises(ValidationError):
            setattr(schema, field_name, new_value)
