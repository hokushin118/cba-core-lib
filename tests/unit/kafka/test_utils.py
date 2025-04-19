"""
Kafka Utils Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import pytest

from cba_core_lib.kafka.utils import (
    safe_string_serializer,
    safe_json_serializer
)


######################################################################
#  KAFKA UTILS UNIT TEST CASES
######################################################################
class TestSafeJsonSerializer:
    """The safe_json_serializer Function Tests."""

    def test_handle_none_input(self):
        """It should return None when input is None."""
        assert safe_json_serializer(None) is None

    def test_serialize_dict_to_utf8_json(self):
        """It should serialize a dictionary to UTF-8 encoded JSON."""
        dict_data = {"key": "value"}
        expected_output = b'{"key": "value"}'
        assert safe_json_serializer(dict_data) == expected_output

    def test_serialize_list_to_utf8_json(self):
        """It should serialize a list to UTF-8 encoded JSON."""
        list_data = [1, 2, 3]
        expected_output = b"[1, 2, 3]"
        assert safe_json_serializer(list_data) == expected_output

    def test_serialize_string_to_utf8_json(self):
        """It should serialize a string to UTF-8 encoded JSON."""
        string_data = "test"
        expected_output = b'"test"'
        assert safe_json_serializer(string_data) == expected_output

    def test_serialize_integer_to_utf8_json(self):
        """It should serialize an integer to UTF-8 encoded JSON."""
        int_data = 123
        expected_output = b"123"
        assert safe_json_serializer(int_data) == expected_output

    def test_serialize_float_to_utf8_json(self):
        """It should serialize a float to UTF-8 encoded JSON."""
        float_data = 3.14
        expected_output = b"3.14"
        assert safe_json_serializer(float_data) == expected_output

    def test_serialize_boolean_to_utf8_json(self):
        """It should serialize a boolean to UTF-8 encoded JSON."""
        bool_data = True
        expected_output = b"true"
        assert safe_json_serializer(bool_data) == expected_output

    def test_handle_nested_structures(self):
        """It should correctly serialize nested JSON structures."""
        nested_data = {"a": 1, "b": {"c": 2, "d": [3, 4]}}
        expected_output = b'{"a": 1, "b": {"c": 2, "d": [3, 4]}}'
        assert safe_json_serializer(nested_data) == expected_output

    def test_return_none_on_type_error(self):
        """It should return None when serialization fails due to TypeError."""
        set_data = {1, 2, 3}
        assert safe_json_serializer(set_data) is None

    def test_handle_bytes_input_valid_json(self):
        """It should decode, re-encode valid JSON bytes."""
        valid_json_bytes = b'{"key": "value"}'
        expected_output = b'{"key": "value"}'
        assert safe_json_serializer(valid_json_bytes) == expected_output


class TestSafeStringSerializer:
    """The safe_string_serializer Function Tests."""

    def test_handle_none_input(self):
        """It should return None when input is None."""
        assert safe_string_serializer(None) is None

    def test_pass_through_bytes_input(self):
        """It should return the original bytes when input is bytes."""
        bytes_data = b"test bytes"
        assert safe_string_serializer(bytes_data) == bytes_data

    def test_encode_string_input_to_utf8(self):
        """It should encode string input to UTF-8 bytes."""
        string_data = 'test string'
        expected_output = b"test string"
        assert safe_string_serializer(string_data) == expected_output

    def test_encode_integer_input_to_utf8(self):
        """It should encode integer input to UTF-8 bytes."""
        int_data = 123
        expected_output = b"123"
        assert safe_string_serializer(int_data) == expected_output

    def test_encode_float_input_to_utf8(self):
        """It should encode float input to UTF-8 bytes."""
        float_data = 3.14
        expected_output = b"3.14"
        assert safe_string_serializer(float_data) == expected_output

    def test_encode_boolean_input_to_utf8(self):
        """It should encode boolean input to UTF-8 bytes."""
        bool_data = True
        expected_output = b"True"
        assert safe_string_serializer(bool_data) == expected_output

    def test_encode_complex_object_input_to_utf8(self):
        """It should encode a complex object's string
        representation to UTF-8."""

        class MyObject:
            def __str__(self):
                return "MyObjectString"

        obj_data = MyObject()
        expected_output = b"MyObjectString"
        assert safe_string_serializer(obj_data) == expected_output

    def test_propagate_exception_from_str(self):
        """It should propagate exceptions raised by
        the input object's __str__ method."""

        class MyFailingObject:
            def __str__(self):
                raise ValueError(
                    'Failed to convert to string'
                )

        failing_object = MyFailingObject()
        with pytest.raises(
                ValueError,
                match='Failed to convert to string'
        ):
            safe_string_serializer(failing_object)
