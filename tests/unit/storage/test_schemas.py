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
    """Tests for the SimpleFileData model."""

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

    def test_valid_simple_file_data(self):
        """It should create a valid SimpleFileData instance."""
        file_data = {
            'content_bytes': TEST_FILE_CONTENT,
            'size': TEST_FILE_SIZE,
        }
        simple_file_data = SimpleFileData(**file_data)
        assert simple_file_data.content_bytes == TEST_FILE_CONTENT
        assert simple_file_data.size == TEST_FILE_SIZE

    def test_valid_simple_file_data_with_optional_fields(
            self,
            file_data
    ):
        """It should create a valid SimpleFileData instance
        with all fields."""
        simple_file_data = SimpleFileData(**file_data)
        assert simple_file_data.content_bytes == TEST_FILE_CONTENT
        assert simple_file_data.size == TEST_FILE_SIZE
        assert simple_file_data.filename == TEST_FILE_NAME
        assert simple_file_data.content_type == TEST_CONTENT_TYPE

    def test_missing_required_field_size(self):
        """It should raise ValidationError if a size field is missing."""
        with pytest.raises(ValidationError):
            SimpleFileData(
                content_bytes=TEST_FILE_CONTENT,
            )  # Missing size

    def test_missing_required_field_content_bytes(self):
        """It should raise ValidationError if a content_bytes
        field is missing."""
        with pytest.raises(ValidationError):
            SimpleFileData(
                size=TEST_FILE_SIZE,
            )  # Missing content_bytes

    def test_invalid_content_bytes_type_request(self):
        """It should raise ValidationError if content_bytes is not bytes."""
        with pytest.raises(ValidationError):
            SimpleFileData(
                content_bytes=1,
                size=TEST_FILE_SIZE,
            )  # Invalid content_bytes

    def test_invalid_size_type_request(self):
        """It should raise ValidationError if size is not an int."""
        with pytest.raises(ValidationError):
            SimpleFileData(
                content_bytes=TEST_FILE_CONTENT,
                size='invald type',
            )  # Invalid size

    def test_simple_file_data_immutability(
            self,
            file_data
    ):
        """It should ensure SimpleFileData instance is immutable."""
        simple_file_data = SimpleFileData(**file_data)
        with pytest.raises(ValidationError):
            simple_file_data.content_bytes = b'new-content'
        with pytest.raises(ValidationError):
            simple_file_data.size = 1
        with pytest.raises(ValidationError):
            simple_file_data.filename = 'new-filename'
        with pytest.raises(ValidationError):
            simple_file_data.content_type = 'new-content-type'

    def test_simple_file_data_model_validation_error(self):
        """It should raise a ValueError when the SimpleFileData Pydantic
        model is initialized with invalid size."""
        invalid_data = {
            'content_bytes': TEST_FILE_CONTENT,
            'size': 10,  # Incorrect size
            'filename': TEST_FILE_NAME,
            'content_type': TEST_CONTENT_TYPE,
        }
        with pytest.raises(ValueError) as excinfo:
            SimpleFileData(**invalid_data)
        assert 'Inconsistent file data' in str(excinfo.value)

    def test_file_upload_data_protocol(self):
        """It should check that SimpleFileData adheres to the
        FileUploadData protocol."""
        simple_file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=TEST_FILE_SIZE,
            filename=TEST_FILE_NAME,
            content_type=TEST_CONTENT_TYPE,
        )
        assert isinstance(simple_file_data, FileUploadData)
