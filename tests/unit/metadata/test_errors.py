"""
Metadata Custom Errors Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=service --cov-report=term-missing --cov-branch
"""
from cba_core_lib.metadata.errors import MetadataError


######################################################################
#  METADATA CUSTOM ERRORS UNIT TEST CASES
######################################################################
class TestMetadataError:
    """The MetadataError Class Tests."""

    def test_initialization_with_message(self):
        """It should initialize MetadataError with a custom
        error message."""

        message = 'Failed to save file metadata.'
        error = MetadataError(message)
        assert error.message == message
        assert str(error) == message

    def test_initialization_without_message(self):
        """It should initialize MetadataError without an
        error message."""

        error = MetadataError()
        assert error.message is None
        assert str(error) == ''

    def test_to_dict_with_message(self):
        """It should return a dictionary representation with a
        custom error message."""

        message = 'File metadata not found.'
        error = MetadataError(message)
        expected_dict = {'error': message}
        assert error.to_dict() == expected_dict

    def test_to_dict_without_message(self):
        """It should return a dictionary representation without
        an error message."""

        error = MetadataError()
        expected_dict = {'error': None}
        assert error.to_dict() == expected_dict

    def test_inheritance(self):
        """It should inherit from the base Exception class."""

        error = MetadataError('Test inheritance')
        assert isinstance(error, Exception)
