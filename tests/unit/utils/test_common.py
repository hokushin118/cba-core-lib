"""
Common Utils Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import uuid

import pytest

from cba_core_lib.utils.common import (
    generate_correlation_id,
    check_not_whitespace_only
)


######################################################################
#  COMMON UTILITY FUNCTIONS UNIT TEST CASES
######################################################################
class TestGenerateCorrelationId:
    """The generate_correlation_id Function Tests."""

    def test_generate_correlation_id_is_uuid(self):
        """It should generate a valid UUID."""
        correlation_id = generate_correlation_id()
        try:
            uuid.UUID(correlation_id)
        except ValueError:
            pytest.fail(
                f"Generated correlation ID '{correlation_id}'"
                f" is not a valid UUID"
            )

    def test_generate_correlation_id_uniqueness(self):
        """It should generate unique IDs."""
        id1 = generate_correlation_id()
        id2 = generate_correlation_id()
        assert id1 != id2, 'Generated correlation IDs should be unique'

    def test_generate_correlation_id_returns_string(self):
        """It should return a string."""
        correlation_id = generate_correlation_id()
        assert isinstance(
            correlation_id,
            str
        ), 'Generated correlation ID should be a string'


class TestCheckNotWhitespaceOnly:
    """The check_not_whitespace_only Function Tests."""

    def test_check_not_whitespace_only_valid_string(self):
        """It should return the original string when given a valid string
        that contains non-whitespace characters."""
        test_string = 'Hello, World!'
        result = check_not_whitespace_only(test_string)
        assert result == test_string, 'Should return the original string'

    def test_check_not_whitespace_only_string_with_leading_and_trailing_spaces(
            self
    ):
        """It should return the original string when given a string that has
        leading and trailing spaces."""
        test_string = '   Trimmed String   '
        result = check_not_whitespace_only(test_string)
        assert result == test_string, \
            'Should return the original string with spaces'

    def test_check_not_whitespace_only_string_with_internal_spaces(self):
        """It should return the original string when given a string that has
        spaces within the string."""
        test_string = 'Internal Spaces String'
        result = check_not_whitespace_only(test_string)
        assert result == test_string, \
            'Should return the original string with spaces'

    def test_check_not_whitespace_only_string_with_only_spaces(self):
        """It should raise a ValueError when the string contains
        only spaces."""
        test_string = "   "
        with pytest.raises(ValueError) as excinfo:
            check_not_whitespace_only(test_string)
        assert 'Field cannot consist only of whitespace.' in str(
            excinfo.value
        ), 'Should raise ValueError with correct message'

    def test_check_not_whitespace_only_string_with_tabs(self):
        """It should raise a ValueError when the string contains only tabs."""
        test_string = "\t\t\t"
        with pytest.raises(ValueError) as excinfo:
            check_not_whitespace_only(test_string)
        assert 'Field cannot consist only of whitespace.' in str(
            excinfo.value
        ), 'Should raise ValueError with correct message'

    def test_check_not_whitespace_only_string_with_newlines(self):
        """It should raise a ValueError when the string contains only
        newlines."""
        test_string = '\n\n\n'
        with pytest.raises(ValueError) as excinfo:
            check_not_whitespace_only(test_string)
        assert 'Field cannot consist only of whitespace.' in str(
            excinfo.value
        ), 'Should raise ValueError with correct message'

    def test_check_not_whitespace_only_string_with_mixed_whitespace(self):
        """It should raise a ValueError when the string contains mixed
        whitespace characters."""
        test_string = '  \t\n  '
        with pytest.raises(ValueError) as excinfo:
            check_not_whitespace_only(test_string)
        assert 'Field cannot consist only of whitespace.' in str(
            excinfo.value
        ), 'Should raise ValueError with correct message'
