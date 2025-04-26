"""
File Storage Custom Errors Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=service --cov-report=term-missing --cov-branch
"""
from cba_core_lib.storage.errors import FileStorageError


######################################################################
#  FILE STORAGE CUSTOM ERRORS UNIT TEST CASES
######################################################################
class TestFileStorageError:
    """The FileStorageError Class Tests."""

    def test_initialization_with_message(self):
        """It should initialize FileStorageError with a custom
        error message."""

        message = 'Failed to upload file.'
        error = FileStorageError(message)
        assert error.message == message
        assert str(error) == message

    def test_initialization_without_message(self):
        """It should initialize FileStorageError without an
        error message."""

        error = FileStorageError()
        assert error.message is None
        assert str(error) == ''

    def test_to_dict_with_message(self):
        """It should return a dictionary representation with a
        custom error message."""

        message = 'File not found.'
        error = FileStorageError(message)
        expected_dict = {'error': message}
        assert error.to_dict() == expected_dict

    def test_to_dict_without_message(self):
        """It should return a dictionary representation without
        an error message."""

        error = FileStorageError()
        expected_dict = {'error': None}
        assert error.to_dict() == expected_dict

    def test_inheritance(self):
        """It should inherit from the base Exception class."""

        error = FileStorageError('Test inheritance')
        assert isinstance(error, Exception)
