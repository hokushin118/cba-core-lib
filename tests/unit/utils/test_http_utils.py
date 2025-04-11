"""
HTTP Utils Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""

import hashlib
import json

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

    @staticmethod
    def calculate_expected_hash(data):
        """Helper method to calculate the expected hash for comparison."""
        data_str = json.dumps(
            data,
            sort_keys=True,
            ensure_ascii=False,
            separators=(',', ':')
        )
        encoded_str = data_str.encode('utf-8')
        return hashlib.md5(encoded_str).hexdigest()

    def test_empty_dict(self):
        """It should generate a hash for an empty dictionary."""
        data = {}
        expected_hash = self.calculate_expected_hash(data)
        hash_value = generate_etag_hash(data)
        assert hash_value == expected_hash

    def test_simple_dict(self):
        """It should generate a hash for a simple dictionary."""
        data = {'name': 'test', 'value': 123}
        expected_hash = self.calculate_expected_hash(data)
        hash_value = generate_etag_hash(data)
        assert hash_value == expected_hash

    def test_nested_dict(self):
        """It should generate a hash for a nested dictionary."""
        data = {
            'user': {'name': 'test', 'roles': ['admin', 'user']}
        }
        expected_hash = self.calculate_expected_hash(data)
        hash_value = generate_etag_hash(data)
        assert hash_value == expected_hash

    def test_dict_with_special_chars(self):
        """It should generate a hash for a dictionary with special
        characters."""
        data = {
            'name': 'Test User',
            'email': 'test@example.com',
            'bio': 'Hello, world!'
        }
        expected_hash = self.calculate_expected_hash(data)
        hash_value = generate_etag_hash(data)
        assert hash_value == expected_hash

    def test_different_order_same_hash(self):
        """It should generate the same hash for dictionaries with the same content in
        different order."""
        data1 = {'a': 1, 'b': 2, 'c': 3}
        data2 = {'c': 3, 'a': 1, 'b': 2}
        hash1 = generate_etag_hash(data1)
        hash2 = generate_etag_hash(data2)
        assert hash1 == hash2
        assert hash1 == self.calculate_expected_hash(data1)

    def test_different_content_different_hash(self):
        """It should generate different hashes for different content."""
        data1 = {'a': 1, 'b': 2}
        data2 = {'a': 1, 'b': 3}
        hash1 = generate_etag_hash(data1)
        hash2 = generate_etag_hash(data2)
        assert hash1 != hash2
        assert hash1 == self.calculate_expected_hash(data1)
        assert hash2 == self.calculate_expected_hash(data2)

    def test_non_dict_input(self):
        """It should raise TypeError when input is not a dictionary."""
        with pytest.raises(TypeError):
            generate_etag_hash(
                'not a dict'
            )

    def test_dict_with_various_types(self):
        """It should generate a hash for a dictionary with various
        JSON-compatible types."""
        data = {
            'string': 'text',
            'number': 42,
            'float': 3.14,
            'boolean': True,
            'null': None,
            'array': [1, 2, 3],
            'object': {'key': 'value'}
        }
        expected_hash = self.calculate_expected_hash(data)
        hash_value = generate_etag_hash(data)
        assert hash_value == expected_hash

    def test_dict_with_unicode(self):
        """It should generate a hash for a dictionary with Unicode
        characters."""
        data = {'text': 'é'}
        expected_hash = self.calculate_expected_hash(data)
        hash_value = generate_etag_hash(data)
        assert hash_value == expected_hash, \
            f"Expected {expected_hash}, got {hash_value}"

        data = {'name': 'John', 'city': 'Amsterdam', 'currency': '€'}
        expected_hash = self.calculate_expected_hash(data)
        hash_value = generate_etag_hash(data)
        assert hash_value == expected_hash, \
            f"Expected {expected_hash}, got {hash_value}"


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
