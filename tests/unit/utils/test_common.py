"""
Common Utils Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import uuid

import pytest

from cba_core_lib.utils.common import generate_correlation_id


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
