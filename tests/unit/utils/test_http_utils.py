"""
HTTP Utils Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""

import hashlib
from typing import Dict, Any

import pytest

from cba_core_lib.utils.http_utils import (
    validate_content_type,
    generate_etag_hash,
    UnsupportedMediaTypeError,
)


######################################################################
#  HTTP UTILS UNIT TEST CASES
######################################################################
class TestValidateContentType:
    """validate_content_type Function Tests."""

    def test_valid_content_type(self):
        """It should not raise an exception when content types match."""
        validate_content_type(
            'application/json',
            'application/json'
        )

    def test_missing_content_type(self):
        """It should raise UnsupportedMediaTypeError when content type i
        s missing."""
        with pytest.raises(UnsupportedMediaTypeError) as excinfo:
            validate_content_type('application/json', None)

        assert excinfo.value.expected == 'application/json'
        assert excinfo.value.actual is None
        assert "Unsupported Media Type. Expected 'application/json'" in str(
            excinfo.value
        )

    def test_mismatched_content_type(self):
        """It should raise UnsupportedMediaTypeError when content
        types don't match."""
        with pytest.raises(UnsupportedMediaTypeError) as excinfo:
            validate_content_type(
                'application/json',
                'text/plain'
            )

        assert excinfo.value.expected == 'application/json'
        assert excinfo.value.actual == 'text/plain'
        assert (
                   "Unsupported Media Type. Expected 'application/json' "
                   "but received 'text/plain'"
               ) in str(
            excinfo.value
        )


class TestGenerateEtagHash:
    """generate_etag_hash Function Tests."""

    def test_generate_etag_hash_empty_dict(self):
        """It should generate the correct hash for an empty dictionary."""
        data: Dict[str, Any] = {}
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash

    def test_generate_etag_hash_simple_dict(self):
        """It should generate the correct hash for a simple dictionary."""
        data = {
            'key1': 'value1',
            'key2': 123
        }
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash

    def test_generate_etag_hash_different_order(self):
        """It should generate different hashes for dictionaries with
        different key order."""
        data1 = {
            'key1': 'value1',
            'key2': 123
        }
        data2 = {
            'key2': 123,
            'key1': 'value1'
        }
        hash1 = generate_etag_hash(data1)
        hash2 = generate_etag_hash(data2)
        assert hash1 != hash2, 'ETags should be different for different order'

    def test_generate_etag_hash_nested_dict(self):
        """It should generate the correct hash for a nested dictionary."""
        data = {
            'key1': {
                'nested_key1': 'nested_value1'
            },
            'key2': 123
        }
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash

    def test_generate_etag_hash_with_unicode(self):
        """It should generate the correct hash for a dictionary containing
        Unicode characters."""
        data = {
            'name': "Jürgen Müller",
            'city': "München"
        }
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash

    def test_generate_etag_hash_with_none(self):
        """It should generate the correct hash for a dictionary
        containing None value."""
        data = {
            'key1': None,
            'key2': 123
        }
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash

    def test_generate_etag_hash_with_boolean(self):
        """It should generate the correct hash for a dictionary
        containing boolean value."""
        data = {
            'key1': True,
            'key2': False
        }
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash

    def test_generate_etag_hash_with_integer_float(self):
        """It should generate the correct hash for a dictionary containing
        integer and float."""
        data = {
            'int_key': 10,
            'float_key': 3.14
        }
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash

    def test_generate_etag_hash_mixed_types(self):
        """It should generate the correct hash for a dictionary
        containing mixed data types."""
        data = {
            'str_key': 'value',
            'int_key': 123,
            'bool_key': True,
            'none_key': None,
            'list_key': [1, 2, 3]
        }
        expected_hash = hashlib.md5(str(data).encode('utf-8')).hexdigest()
        assert generate_etag_hash(data) == expected_hash


class TestUnsupportedMediaTypeError:
    """UnsupportedMediaTypeError Exception Tests."""

    def test_unsupported_media_type_error_message_with_actual(self):
        """It should construct the correct error message when
        actual type is provided."""
        error = UnsupportedMediaTypeError(
            expected_media_type='application/json',
            actual_media_type='text/plain'
        )
        assert str(error) == (
            "Unsupported Media Type. Expected "
            "'application/json' but received 'text/plain'."
        )
        assert error.expected == 'application/json'
        assert error.actual == 'text/plain'

    def test_unsupported_media_type_error_message_without_actual(self):
        """It should construct the correct error message when actual
        type is None."""
        error = UnsupportedMediaTypeError(
            expected_media_type="application/json",
            actual_media_type=None
        )
        assert str(
            error
        ) == "Unsupported Media Type. Expected 'application/json'."
        assert error.expected == 'application/json'
        assert error.actual is None
