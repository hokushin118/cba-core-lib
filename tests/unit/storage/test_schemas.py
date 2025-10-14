"""
File Storage Schemas Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import pytest
from pydantic import ValidationError
from pytest import fixture

from cba_core_lib.storage.schemas import SimpleFileData, FileUploadData
from tests.conftest import (
    TEST_FILE_NAME,
    TEST_FILE_CONTENT,
    TEST_FILE_SIZE,
    TEST_CONTENT_TYPE
)


######################################################################
#  FILE STORAGE SCHEMAS UNIT TEST CASES
######################################################################
class TestSimpleFileData:
    """The SimpleFileData Class Tests."""

    @fixture
    def file_data(self):
        """Provides a sample dictionary representing file upload
        data for testing."""
        return {
            'content_bytes': TEST_FILE_CONTENT,
            'size': TEST_FILE_SIZE,
            'filename': TEST_FILE_NAME,
            'content_type': TEST_CONTENT_TYPE,
        }

    def test_valid_schema(self):
        """It should create a valid schema instance."""
        file_data = {
            'content_bytes': TEST_FILE_CONTENT,
            'size': TEST_FILE_SIZE,
        }
        schema = SimpleFileData(**file_data)
        assert schema.content_bytes == TEST_FILE_CONTENT
        assert schema.size == TEST_FILE_SIZE

    def test_valid_schema_with_optional_fields(
            self,
            file_data
    ):
        """It should create a valid schema instance
        with all fields."""
        schema = SimpleFileData(**file_data)
        assert schema.content_bytes == TEST_FILE_CONTENT
        assert schema.size == TEST_FILE_SIZE
        assert schema.filename == TEST_FILE_NAME
        assert schema.content_type == TEST_CONTENT_TYPE

    @pytest.mark.parametrize(
        'missing_field',
        [
            'size',
            'content_bytes',
        ],
    )
    def test_schema_missing_required_field(
            self,
            missing_field,
            file_data
    ):
        """It should raise ValidationError if a required field is missing."""
        del file_data[missing_field]  # Remove the required field

        with pytest.raises(ValidationError) as excinfo:
            SimpleFileData(**file_data)

        assert f"{missing_field}" in str(
            excinfo.value
        ), f"Expected error for missing field '{missing_field}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, invalid_value',
        [
            ('content_bytes', 1),
            ('size', 'Invalid type'),
        ],
    )
    def test_schema_validation_error(
            self,
            field_name,
            invalid_value,
            file_data
    ):
        """It should raise ValidationError when specific fields
        are provided with invalid data types."""
        file_data[field_name] = invalid_value

        with pytest.raises(ValidationError) as excinfo:
            SimpleFileData(**file_data)

        assert f"{field_name}" in str(
            excinfo.value
        ), f"Expected error for field '{field_name}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('content_bytes', b'new-content'),
            ('size', 1),
            ('filename', 'new-filename'),
            ('content_type', 'new-content-type'),
        ],
    )
    def test_schema_immutability(
            self,
            file_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        schema = SimpleFileData(**file_data)
        with pytest.raises(ValidationError):
            setattr(schema, field_name, new_value)

    def test_schema_protocol(self):
        """It should check that schema adheres to the
        FileUploadData protocol."""
        schema = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=TEST_FILE_SIZE,
            filename=TEST_FILE_NAME,
            content_type=TEST_CONTENT_TYPE,
        )
        assert isinstance(schema, FileUploadData)
