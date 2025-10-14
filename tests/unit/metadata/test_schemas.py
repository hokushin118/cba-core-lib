"""
File Metadata Schemas Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import uuid
from datetime import timezone, datetime

import pytest
from pydantic import ValidationError
from pytest import fixture

from cba_core_lib.metadata.schemas import (
    BaseMetadataDTO,
    UploadMetadataDTO,
    UpdateMetadataDTO
)
from tests.conftest import (
    TEST_UUID,
    TEST_CREATED_AT,
    TEST_UPDATED_AT,
    TEST_FILE_SIZE,
    TEST_FILE_NAME,
    TEST_CONTENT_TYPE,
    TEST_BUCKET_NAME,
    TEST_ETAG,
    TEST_USER_ID,
    TEST_OBJECT_NAME
)


######################################################################
#  FILE METADATA SCHEMAS UNIT TEST CASES
######################################################################
class TestBaseMetadataDTO:
    """The BaseMetadataDTO Class Tests."""

    @fixture
    def meta_data(self):
        """Provides a sample dictionary representing file metadata
         for testing."""
        return {
            'id': TEST_UUID,
            'created_at': TEST_CREATED_AT,
            'updated_at': TEST_UPDATED_AT,
        }

    def test_valid_schema_with_optional_fields(
            self,
            meta_data
    ):
        """It should create a valid schema instance
        with all fields."""
        schema = BaseMetadataDTO(**meta_data)
        assert schema.id == TEST_UUID
        assert schema.created_at == TEST_CREATED_AT
        assert schema.updated_at == TEST_UPDATED_AT

    @pytest.mark.parametrize(
        'invalid_data, expected_error_fields',
        [
            ({'id': 123}, ['id']),  # Invalid id type
            ({'created_at': 'invalid-datetime'}, ['created_at']),
            ({'updated_at': 'invalid-datetime'}, ['updated_at']),
        ],
    )
    def test_schema_validation_error(
            self,
            invalid_data,
            expected_error_fields
    ):
        """It should raise a ValidationError when the BaseMetadataDTO Pydantic
        model is initialized with invalid data types."""
        with pytest.raises(ValidationError) as excinfo:
            BaseMetadataDTO(**invalid_data)

        error_msg = str(excinfo.value)
        for field in expected_error_fields:
            assert field in error_msg.lower(), f"Expected error for field: {field}"
        assert 'validation' in error_msg.lower()

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('id', uuid.uuid4()),
            ('created_at', datetime.now(timezone.utc)),
            ('updated_at', datetime.now(timezone.utc)),
        ],
    )
    def test_schema_immutability(
            self,
            meta_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        base_metadata_dto = BaseMetadataDTO(**meta_data)
        with pytest.raises(ValidationError):
            setattr(base_metadata_dto, field_name, new_value)


class TestUploadMetadataDTO:
    """The UploadMetadataDTO Class Tests."""

    @fixture
    def meta_data(self):
        """Provides a sample dictionary representing file upload
        data for testing."""
        return {
            'original_filename': TEST_FILE_NAME,
            'object_name': TEST_OBJECT_NAME,
            'bucket_name': TEST_BUCKET_NAME,
            'content_type': TEST_CONTENT_TYPE,
            'size': TEST_FILE_SIZE,
            'etag': TEST_ETAG,
            'uploaded_by': TEST_USER_ID,
        }

    def test_valid_schema(self):
        """It should create a valid schema instance."""
        meta_data = {
            'original_filename': TEST_FILE_NAME,
            'object_name': TEST_OBJECT_NAME,
            'bucket_name': TEST_BUCKET_NAME,
            'size': TEST_FILE_SIZE,
        }
        schema = UploadMetadataDTO(**meta_data)
        assert schema.original_filename == TEST_FILE_NAME
        assert schema.object_name == TEST_OBJECT_NAME
        assert schema.bucket_name == TEST_BUCKET_NAME
        assert schema.size == TEST_FILE_SIZE

    def test_valid_schema_with_optional_fields(
            self,
            meta_data
    ):
        """It should create a valid schema instance
        with all fields."""
        schema = UploadMetadataDTO(**meta_data)
        assert schema.original_filename == TEST_FILE_NAME
        assert schema.object_name == TEST_OBJECT_NAME
        assert schema.bucket_name == TEST_BUCKET_NAME
        assert schema.content_type == TEST_CONTENT_TYPE
        assert schema.size == TEST_FILE_SIZE
        assert schema.etag == TEST_ETAG
        assert schema.uploaded_by == TEST_USER_ID

    @pytest.mark.parametrize(
        'missing_field',
        [
            'original_filename',
            'object_name',
            'bucket_name',
            'size',
        ],
    )
    def test_schema_missing_required_field(
            self,
            missing_field,
            meta_data
    ):
        """It should raise ValidationError if a required field is missing."""
        del meta_data[missing_field]  # Remove the required field

        with pytest.raises(ValidationError) as excinfo:
            UploadMetadataDTO(**meta_data)

        assert f"{missing_field}" in str(
            excinfo.value
        ), f"Expected error for missing field '{missing_field}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, invalid_value',
        [
            ('original_filename', 1),
            ('object_name', 1),
            ('bucket_name', 1),
            ('size', 'Invalid type'),
        ],
    )
    def test_schema_validation_error(
            self,
            field_name,
            invalid_value,
            meta_data
    ):
        """It should raise ValidationError when specific fields
        are provided with invalid data types."""
        meta_data[field_name] = invalid_value

        with pytest.raises(ValidationError) as excinfo:
            UploadMetadataDTO(**meta_data)

        assert f"{field_name}" in str(
            excinfo.value
        ), f"Expected error for field '{field_name}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('original_filename', 'new-file-name'),
            ('object_name', 'new-object-name'),
            ('bucket_name', 'new-bucket-name'),
            ('content_type', 'new-content-type'),
            ('size', 1),
            ('etag', 'new-etag'),
            ('uploaded_by', 'new-uploaded-by'),
        ],
    )
    def test_schema_immutability(
            self,
            meta_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        schema = UploadMetadataDTO(**meta_data)
        with pytest.raises(ValidationError):
            setattr(schema, field_name, new_value)


class TestUpdateMetadataDTO:
    """The UpdateMetadataDTO Class Tests."""

    @fixture
    def meta_data(self):
        """Provides a sample dictionary representing file upload
        data for testing."""
        return {
            'original_filename': TEST_FILE_NAME,
            'object_name': TEST_OBJECT_NAME,
            'content_type': TEST_CONTENT_TYPE,
            'etag': TEST_ETAG,
        }

    def test_valid_schema_with_optional_fields(
            self,
            meta_data
    ):
        """It should create a valid schema instance
        with all fields."""
        schema = UpdateMetadataDTO(**meta_data)
        assert schema.original_filename == TEST_FILE_NAME
        assert schema.object_name == TEST_OBJECT_NAME
        assert schema.content_type == TEST_CONTENT_TYPE
        assert schema.etag == TEST_ETAG

    @pytest.mark.parametrize(
        'field_name, invalid_value',
        [
            ('original_filename', 1),
            ('object_name', 1),
            ('content_type', 1),
            ('etag', 1),
        ],
    )
    def test_schema_validation_error(
            self,
            field_name,
            invalid_value,
            meta_data
    ):
        """It should raise ValidationError when specific fields
        are provided with invalid data types."""
        meta_data[field_name] = invalid_value

        with pytest.raises(ValidationError) as excinfo:
            UpdateMetadataDTO(**meta_data)

        assert f"{field_name}" in str(
            excinfo.value
        ), f"Expected error for field '{field_name}', but got '{excinfo.value}'"

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('original_filename', 'new-file-name'),
            ('object_name', 'new-object-name'),
            ('content_type', 'new-content-type'),
            ('etag', 'new-etag'),
        ],
    )
    def test_schema_immutability(
            self,
            meta_data,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        schema = UpdateMetadataDTO(**meta_data)
        with pytest.raises(ValidationError):
            setattr(schema, field_name, new_value)
