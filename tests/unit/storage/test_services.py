"""
MinioService Class Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=service --cov-report=term-missing --cov-branch
"""
from typing import List, Dict, Any
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import pytest
from minio.error import S3Error
from pytest import fixture

from cba_core_lib.storage.configs import MinioConfig
from cba_core_lib.storage.errors import FileStorageError
from cba_core_lib.storage.schemas import SimpleFileData, FileUploadData
from cba_core_lib.storage.services import (
    FileStorageService,
    MinioService,
)
from tests.conftest import (
    TEST_USE_SSL,
    TEST_ENDPOINT,
    TEST_ACCESS_KEY,
    TEST_SECRET_KEY,
    TEST_ETAG,
    TEST_FILE_CONTENT,
    TEST_CONTENT_TYPE,
    TEST_BUCKET_NAME,
    TEST_FILE_SIZE,
    TEST_FILE_NAME,
)


######################################################################
#  FIXTURES
######################################################################
@fixture
def mock_minio_client():
    """Fixture that provides a mock Minio client."""
    mock = MagicMock()
    mock.bucket_exists.return_value = True
    mock.list_buckets.return_value = []
    mock.put_object.return_value.etag = TEST_ETAG
    return mock


@fixture
def minio_config():
    """Fixture that provides a MinioConfig instance for testing."""
    return MinioConfig(
        endpoint=TEST_ENDPOINT,
        access_key=TEST_ACCESS_KEY,
        secret_key=TEST_SECRET_KEY,
        use_ssl=TEST_USE_SSL,
    )


@fixture
def mock_upload_file():
    """Fixture that provides a properly configured mock SimpleFileData object.

    It simulates a file upload with a filename, headers, content,
    and a close method."""
    return SimpleFileData(
        content_bytes=TEST_FILE_CONTENT,
        size=len(TEST_FILE_CONTENT),
        filename=TEST_FILE_NAME,
        content_type=TEST_CONTENT_TYPE
    )


######################################################################
#  FILE STORAGE SERVICE UNIT TEST CASES
######################################################################
class TestFileStorageServiceABC:
    """The FileStorageService Abstract Class Tests."""

    @pytest.mark.asyncio
    async def test_file_storage_service_abstract_methods(
            self,
            mock_upload_file
    ):
        """It should verify that FileStorageService cannot be instantiated
        directly due to being an abstract base class, and that its
        abstract methods raise NotImplementedError (implicitly through
        not being defined in the ABC). This test also checks the
        behavior of a concrete implementation."""
        with pytest.raises(TypeError):
            # pylint: disable=E0110
            FileStorageService()  # Cannot instantiate ABC

        class ConcreteFileStorageService(FileStorageService):
            """A concrete implementation of the FileStorageService for testing
            the abstract base class. It provides basic non-erroring
            implementations of the abstract methods.
            """

            def check_connection(self) -> bool:
                """Simulates checking the connection to the storage service.

                Returns:
                    bool: Always returns True in this test implementation.
                """
                return True

            def ensure_bucket_exists(
                    self,
                    bucket_name: str
            ) -> None:
                """Simulates ensuring that a bucket exists.

                Args:
                    bucket_name (str): The name of the bucket.
                """

            async def upload_file(
                    self,
                    file_data: FileUploadData,
                    bucket_name: str,
                    object_name_prefix: str = '',
            ) -> tuple[str, str, int]:
                """Simulates uploading a file to a storage service.

                This method mimics the process of uploading a file, taking
                in the file data, bucket name, and an optional object name
                prefix. It returns a fixed tuple representing a successful
                upload.

                Args:
                    file_data:  An object conforming to the FileUploadData
                    protocol, containing the file's content and metadata.
                    bucket_name: The name of the bucket to which the file would
                    be uploaded.
                    object_name_prefix: An optional prefix to be added to
                    the object name. Defaults to an empty string.

                Returns:
                    tuple:  A tuple containing:
                        - A fixed test filename ('test_file.txt').
                        - A fixed test ETag ('test_etag').
                        - A fixed test file size (1024).
                """
                return TEST_FILE_NAME, TEST_ETAG, TEST_FILE_SIZE

            def get_file_url(
                    self,
                    bucket_name: str,
                    object_name: str
            ) -> str:
                """Simulates generating a file URL.

                Args:
                    bucket_name (str): The name of the bucket.
                    object_name (str): The name of the object.

                Returns:
                    str: A simulated file URL.
                """
                return f"http://example.com/{bucket_name}/{object_name}"

            def is_file_exists(
                    self,
                    bucket_name: str,
                    object_name: str
            ) -> bool:
                """Simulates checking if a file exists.

                Args:
                    bucket_name (str): The name of the bucket.
                    object_name (str): The name of the object.

                Returns:
                    bool: Always returns True in this test implementation.
                """
                return True

            def list_buckets(
                    self
            ) -> List[str]:
                """Simulates listing all buckets.

                Returns:
                    List[str]: A list containing the name of a test bucket.
                """
                return [TEST_BUCKET_NAME]

            def list_files(
                    self,
                    bucket_name: str,
                    prefix: str = ''
            ) -> List[Dict[str, Any]]:
                """Simulates listing files in a bucket.

                Args:
                    bucket_name (str): The name of the bucket.
                    prefix (str): An optional prefix to filter files.

                Returns:
                    List[Dict[str, Any]]: A list containing a dictionary
                    with file metadata.
                """
                return [{
                    'name': TEST_FILE_NAME,
                    'size': TEST_FILE_SIZE,
                    'last_modified': '2024-01-01',
                    'etag': TEST_ETAG,
                    'content_type': TEST_CONTENT_TYPE
                }]

            async def delete_file(
                    self,
                    bucket_name: str,
                    object_name: str
            ) -> None:
                """Simulates deleting a file.

                Args:
                    bucket_name (str): The name of the bucket.
                    object_name (str): The name of the object to delete.
                """

        concrete_service = ConcreteFileStorageService()
        assert concrete_service.check_connection() is True
        concrete_service.ensure_bucket_exists(TEST_BUCKET_NAME)
        result = await concrete_service.upload_file(
            mock_upload_file, TEST_BUCKET_NAME
        )
        assert result == (TEST_FILE_NAME, TEST_ETAG, TEST_FILE_SIZE)
        assert concrete_service.get_file_url(
            TEST_BUCKET_NAME, TEST_FILE_NAME
        ) == f"http://example.com/{TEST_BUCKET_NAME}/{TEST_FILE_NAME}"
        assert concrete_service.is_file_exists(
            TEST_BUCKET_NAME,
            TEST_FILE_NAME
        ) is True
        assert concrete_service.list_buckets() == [TEST_BUCKET_NAME]
        files = concrete_service.list_files(TEST_BUCKET_NAME)
        assert len(files) == 1
        assert files[0]['name'] == TEST_FILE_NAME
        await concrete_service.delete_file(TEST_BUCKET_NAME, TEST_FILE_NAME)


######################################################################
#  MINIO SERVICE UNIT TEST CASES
######################################################################
# pylint: disable=R0904
class TestMinioService:
    """The MinioService Class Tests."""

    @patch('minio.Minio')
    def test_minio_service_initialization_success(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should successfully initialize the MinioService with a
        MinioConfig and a Minio client. It also verifies that list_buckets
        is called during initialization."""
        mock_minio_class.return_value = mock_minio_client
        service = MinioService(minio_config)
        assert service.client is mock_minio_client
        mock_minio_client.list_buckets.assert_called_once()

    @patch('minio.Minio')
    def test_minio_service_initialization_failure(
            self,
            mock_minio_class,
            minio_config
    ):
        """It should raise a FileStorageError if the Minio client
        fails to initialize (e.g., due to connection issues)."""
        mock_error = S3Error(
            message='Initialization Error',
            resource='test',
            request_id='test',
            host_id='test',
            response='test',
            code='TestError'
        )
        mock_minio_class.side_effect = mock_error
        with pytest.raises(FileStorageError) as excinfo:
            MinioService(minio_config)
        assert 'Failed to initialize MinIO client' in str(excinfo.value)

    @patch('minio.Minio')
    def test_minio_service_ensure_connection_success(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should return True if the connection to the MinIO server
        can be established successfully."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.list_buckets.return_value = []  # Return empty list of buckets
        service = MinioService(minio_config)
        # Reset the mock to clear the call from __init__
        mock_minio_client.list_buckets.reset_mock()
        assert service.check_connection() is True
        mock_minio_client.list_buckets.assert_called_once()

    @patch('minio.Minio')
    def test_minio_service_ensure_connection_s3error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should return False if an S3Error occurs while trying to
        list buckets (indicating a connection issue)."""
        mock_minio_class.return_value = mock_minio_client
        mock_error = S3Error(
            message='Connection Error',
            resource='test',
            request_id='test',
            host_id='test',
            response='test',
            code='TestError'
        )
        mock_minio_client.list_buckets.side_effect = mock_error
        service = MinioService(minio_config)
        # Reset the mock to clear the call from __init__
        mock_minio_client.list_buckets.reset_mock()
        assert service.check_connection() is False
        mock_minio_client.list_buckets.assert_called_once()

    @patch('minio.Minio')
    def test_minio_service_ensure_connection_general_error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should return False if a general exception occurs while
        trying to list buckets."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.list_buckets.side_effect = Exception(
            'General Error'
        )
        service = MinioService(minio_config)
        # Reset the mock to clear the call from __init__
        mock_minio_client.list_buckets.reset_mock()
        assert service.check_connection() is False
        mock_minio_client.list_buckets.assert_called_once()

    @patch('minio.Minio')
    def test_minio_service_ensure_bucket_exists_already_exists(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should not attempt to create the bucket if it already exists."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True
        service = MinioService(minio_config)
        service.ensure_bucket_exists(TEST_BUCKET_NAME)
        mock_minio_client.bucket_exists.assert_called_once_with(
            TEST_BUCKET_NAME
        )
        mock_minio_client.make_bucket.assert_not_called()

    @patch('minio.Minio')
    def test_minio_service_ensure_bucket_exists_not_exists(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should create the bucket if it does not exist."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = False
        service = MinioService(minio_config)
        service.ensure_bucket_exists(TEST_BUCKET_NAME)
        mock_minio_client.bucket_exists.assert_called_once_with(
            TEST_BUCKET_NAME
        )
        mock_minio_client.make_bucket.assert_called_once_with(
            TEST_BUCKET_NAME
        )

    @patch('minio.Minio')
    def test_minio_service_ensure_bucket_exists_s3error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should raise a FileStorageError if an S3Error occurs
        while checking or creating the bucket."""
        mock_minio_class.return_value = mock_minio_client
        mock_error = S3Error(
            message='Bucket Error',
            resource='test',
            request_id='test',
            host_id='test',
            response='test',
            code='TestError'
        )
        mock_minio_client.bucket_exists.side_effect = mock_error
        service = MinioService(minio_config)
        with pytest.raises(FileStorageError) as excinfo:
            service.ensure_bucket_exists(TEST_BUCKET_NAME)
        assert f"Could not ensure MinIO bucket '{TEST_BUCKET_NAME}' exists" in str(
            excinfo.value
        )

    @patch('minio.Minio')
    def test_minio_service_ensure_bucket_exists_general_error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should raise a FileStorageError for unexpected errors
        during bucket existence check."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.side_effect = Exception(
            'General Bucket Error'
        )
        service = MinioService(minio_config)
        with pytest.raises(FileStorageError) as excinfo:
            service.ensure_bucket_exists(TEST_BUCKET_NAME)
        assert f"Unexpected error interacting with bucket '{TEST_BUCKET_NAME}'" in str(
            excinfo.value
        )

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_upload_file_success(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client,
            mock_upload_file
    ):
        """It should successfully upload a file to MinIO and return
        the object name, ETag, and file size."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True
        service = MinioService(minio_config)
        object_name, etag, file_size = await service.upload_file(
            file_data=mock_upload_file,
            bucket_name=TEST_BUCKET_NAME,
        )
        assert object_name.startswith('')  # No prefix by default
        assert etag == TEST_ETAG
        assert file_size == TEST_FILE_SIZE
        mock_minio_client.put_object.assert_called_once()

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_upload_file_with_prefix(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client,
            mock_upload_file
    ):
        """It should successfully upload a file with the specified prefix."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True
        prefix = 'upload/'
        service = MinioService(minio_config)
        object_name, etag, file_size = await service.upload_file(
            file_data=mock_upload_file,
            bucket_name=TEST_BUCKET_NAME,
            object_name_prefix=prefix,
        )
        assert object_name.startswith(prefix)
        assert etag == TEST_ETAG
        assert file_size == TEST_FILE_SIZE
        mock_minio_client.put_object.assert_called_once()

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_upload_file_s3error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should raise a FileStorageError when an S3Error occurs during
        the file upload, ensuring that the error message and retry logic
        are correctly handled.
        """
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True
        # Create a new mock upload file with properly configured async methods
        file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=len(TEST_FILE_CONTENT),
            filename=TEST_FILE_NAME,
            content_type=TEST_CONTENT_TYPE
        )

        # Set up the mock to fail with an S3Error
        mock_error = S3Error(
            message='Upload Error',
            resource='test',
            request_id='test',
            host_id='test',
            response='test',
            code='TestError'
        )
        mock_minio_client.put_object.side_effect = mock_error

        service = MinioService(minio_config)
        with pytest.raises(FileStorageError) as excinfo:
            try:
                await service.upload_file(
                    file_data=file_data,
                    bucket_name=TEST_BUCKET_NAME,
                )
            except Exception as err:
                if hasattr(err, '__cause__') and isinstance(
                        err.__cause__,
                        FileStorageError
                ):
                    raise err.__cause__
                raise
        # The error message should be from the S3Error
        assert f"MinIO S3 upload failed for '{TEST_FILE_NAME}'" in str(
            excinfo.value
        )
        # Verify that put_object was called 3 times (max retries)
        assert mock_minio_client.put_object.call_count == 3

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_upload_file_no_filename(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should raise a ValueError if the FileUploadData object has
        no filename."""
        mock_minio_class.return_value = mock_minio_client
        file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=len(TEST_FILE_CONTENT),
            filename=""
        )
        service = MinioService(minio_config)
        with pytest.raises(ValueError) as excinfo:
            await service.upload_file(
                file_data=file_data,
                bucket_name=TEST_BUCKET_NAME,
            )
        assert 'Filename is required for upload to determine extension.' in str(
            excinfo.value
        )

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_upload_file_general_error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should raise a FileStorageError with an informative message
        when a general exception (not an S3Error) occurs during the file
        upload, ensuring that the retry mechanism is invoked and the
        original exception details are included in the raised error."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True
        # Create a new mock upload file with properly configured async methods
        file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=len(TEST_FILE_CONTENT),
            filename=TEST_FILE_NAME,
            content_type=TEST_CONTENT_TYPE
        )

        # Set up the mock to fail all retry attempts with a general error
        mock_minio_client.put_object.side_effect = Exception(
            'General Upload Error'
        )

        service = MinioService(minio_config)
        with pytest.raises(FileStorageError) as excinfo:
            try:
                await service.upload_file(
                    file_data=file_data,
                    bucket_name=TEST_BUCKET_NAME,
                )
            except Exception as err:
                if hasattr(err, '__cause__') and isinstance(
                        err.__cause__,
                        FileStorageError
                ):
                    raise err.__cause__
                raise
        # The error message should be from the FileStorageError
        assert (
                   f"Unexpected error during MinIO put_object for '{TEST_FILE_NAME}'"
               ) in str(
            excinfo.value
        )
        # Verify that put_object was called 3 times (max retries)
        assert mock_minio_client.put_object.call_count == 3

    @patch('minio.Minio')
    def test_minio_service_get_file_url_http(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should construct the correct HTTP URL for a given bucket and
        object name based on the MinioConfig."""
        mock_minio_class.return_value = mock_minio_client
        service = MinioService(minio_config)
        url = service.get_file_url(TEST_BUCKET_NAME, TEST_FILE_NAME)
        expected_url = f"{TEST_ENDPOINT}/{TEST_BUCKET_NAME}/{TEST_FILE_NAME}"
        assert url == expected_url

    @patch('minio.Minio')
    def test_minio_service_get_file_url_https(
            self,
            mock_minio_client
    ):
        """It should construct the correct HTTPS URL when the MinioConfig
        specifies the use of SSL."""
        https_config = MinioConfig(
            endpoint='https://testminio:9000',
            access_key=TEST_ACCESS_KEY,
            secret_key=TEST_SECRET_KEY,
            use_ssl=True,
        )
        with patch(
                'cba_core_lib.storage.services.Minio',
                return_value=mock_minio_client
        ):
            service = MinioService(https_config)
            url = service.get_file_url(TEST_BUCKET_NAME, TEST_FILE_NAME)
            expected_url = f"https://testminio:9000/{TEST_BUCKET_NAME}/{TEST_FILE_NAME}"
            assert url == expected_url

    @patch('minio.Minio')
    def test_minio_service_get_file_url_endpoint_with_trailing_slash(
            self,
            mock_minio_client
    ):
        """It should correctly construct the file URL even when the endpoint
        in the MinioConfig includes a trailing slash."""
        config_with_slash = MinioConfig(
            endpoint=f"{TEST_ENDPOINT}/",
            access_key=TEST_ACCESS_KEY,
            secret_key=TEST_SECRET_KEY,
            use_ssl=False,
        )
        with patch(
                'cba_core_lib.storage.services.Minio',
                return_value=mock_minio_client
        ):
            service = MinioService(config_with_slash)
            url = service.get_file_url(TEST_BUCKET_NAME, TEST_FILE_NAME)
            expected_url = f"{TEST_ENDPOINT}/{TEST_BUCKET_NAME}/{TEST_FILE_NAME}"
            assert url == expected_url

    @patch('minio.Minio')
    def test_minio_service_get_file_url_encoding(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should correctly URL-encode object names containing spaces and
        special characters to ensure the generated URL is valid."""
        mock_minio_class.return_value = mock_minio_client
        special_chars = 'test file with spaces & special chars.txt'
        service = MinioService(minio_config)
        url = service.get_file_url(TEST_BUCKET_NAME, special_chars)
        expected_url = f"{TEST_ENDPOINT}/{TEST_BUCKET_NAME}/{quote(special_chars)}"
        assert url == expected_url

    def test_minio_service_get_file_url_failure(
            self,
            minio_config,
            mock_minio_client
    ):
        """It should raise a FileStorageError if an empty bucket name is
        provided when constructing the file URL."""
        with patch(
                'cba_core_lib.storage.services.Minio',
                return_value=mock_minio_client
        ):
            service = MinioService(minio_config)
            with pytest.raises(ValueError) as excinfo:
                service.get_file_url('', TEST_FILE_NAME)
            assert 'Bucket name cannot be empty' in str(excinfo.value)

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_upload_file_with_content_type(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client,
            mock_upload_file
    ):
        """It should correctly pass the content type from the FileUploadData's
        headers to the MinIO client during the upload process."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True
        service = MinioService(minio_config)
        _, _, _ = await service.upload_file(
            file_data=mock_upload_file,
            bucket_name=TEST_BUCKET_NAME,
        )
        mock_minio_client.put_object.assert_called_once()
        call_args = mock_minio_client.put_object.call_args[1]
        assert call_args.get('content_type') == TEST_CONTENT_TYPE

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_upload_file_without_content_type(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should default to 'application/octet-stream' as the content type
        when the FileUploadData object does not provide any content type in its
        headers during the upload."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True
        file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=len(TEST_FILE_CONTENT),
            filename=TEST_FILE_NAME
        )
        service = MinioService(minio_config)
        object_name, etag, file_size = await service.upload_file(
            file_data=file_data,
            bucket_name=TEST_BUCKET_NAME,
        )
        assert object_name is not None
        assert etag is not None
        assert file_size == len(TEST_FILE_CONTENT)
        mock_minio_client.put_object.assert_called_once()
        call_args = mock_minio_client.put_object.call_args[1]
        assert call_args['bucket_name'] == TEST_BUCKET_NAME
        assert call_args['object_name'] == object_name
        # Read from BytesIO before comparing
        assert call_args['data'].read() == TEST_FILE_CONTENT
        assert call_args['length'] == len(TEST_FILE_CONTENT)
        # Default content type
        assert call_args['content_type'] == 'application/octet-stream'

    @patch('minio.Minio')
    def test_minio_service_is_file_exists_true(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should return True if the MinIO client's stat_object method
        executes without raising an error, indicating that the file exists."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.stat_object.return_value = MagicMock()
        service = MinioService(minio_config)
        assert service.is_file_exists(
            TEST_BUCKET_NAME,
            TEST_FILE_NAME
        ) is True
        mock_minio_client.stat_object.assert_called_once_with(
            TEST_BUCKET_NAME,
            TEST_FILE_NAME
        )

    @patch('minio.Minio')
    def test_minio_service_is_file_exists_false(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should return False if the MinIO client's stat_object method
        raises an S3Error with the code 'NoSuchKey', indicating that the
        specified file does not exist."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.stat_object.side_effect = S3Error(
            message='Not Found',
            resource='test',
            request_id='test',
            host_id='test',
            response='test',
            code='NoSuchKey'
        )
        service = MinioService(minio_config)
        assert service.is_file_exists(
            TEST_BUCKET_NAME,
            TEST_FILE_NAME
        ) is False
        mock_minio_client.stat_object.assert_called_once_with(
            TEST_BUCKET_NAME,
            TEST_FILE_NAME
        )

    @patch('minio.Minio')
    def test_minio_service_list_buckets_success(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should successfully retrieve a list of bucket names from the
        MinIO server and return them as a Python list."""
        mock_minio_class.return_value = mock_minio_client
        # Create mock buckets with proper name attributes
        mock_bucket1 = MagicMock()
        mock_bucket1.name = TEST_BUCKET_NAME
        mock_bucket2 = MagicMock()
        mock_bucket2.name = 'another-bucket'
        mock_buckets = [mock_bucket1, mock_bucket2]

        mock_minio_client.list_buckets.return_value = mock_buckets
        service = MinioService(minio_config)
        # Reset mock to clear the call from __init__
        mock_minio_client.list_buckets.reset_mock()
        buckets = service.list_buckets()
        assert len(buckets) == 2
        assert TEST_BUCKET_NAME in buckets
        assert 'another-bucket' in buckets
        mock_minio_client.list_buckets.assert_called_once()

    @patch('minio.Minio')
    def test_minio_service_list_files_success(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should successfully list files in a bucket with the specified prefix,
        returning a list of dictionaries containing file metadata including name,
        size, last modified date, etag, and is_dir flag."""
        mock_minio_class.return_value = mock_minio_client
        mock_objects = [
            MagicMock(
                object_name=TEST_FILE_NAME,
                size=TEST_FILE_SIZE,
                last_modified='2024-01-01',
                etag=TEST_ETAG,
                is_dir=False
            ),
            MagicMock(
                object_name='another-file.txt',
                size=100,
                last_modified='2024-01-02',
                etag='another-etag',
                is_dir=False
            )
        ]
        mock_minio_client.list_objects.return_value = mock_objects
        service = MinioService(minio_config)
        files = service.list_files(TEST_BUCKET_NAME, prefix='test/')
        assert len(files) == 2
        assert files[0]['name'] == TEST_FILE_NAME
        assert files[0]['size'] == TEST_FILE_SIZE
        assert files[0]['last_modified'] == '2024-01-01'
        assert files[0]['etag'] == TEST_ETAG
        assert not files[0]['is_dir']
        mock_minio_client.list_objects.assert_called_once_with(
            bucket_name=TEST_BUCKET_NAME,
            prefix='test/',
            recursive=True
        )

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_delete_file_success(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should successfully delete a file from the specified bucket,
        verifying that the remove_object method is called with the correct
        bucket and object names."""
        mock_minio_class.return_value = mock_minio_client
        service = MinioService(minio_config)
        await service.delete_file(TEST_BUCKET_NAME, TEST_FILE_NAME)
        mock_minio_client.remove_object.assert_called_once_with(
            TEST_BUCKET_NAME,
            TEST_FILE_NAME
        )

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_delete_file_s3error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should raise a FileStorageError when an S3Error occurs during
        file deletion, ensuring that the error message includes both the
        filename and the original S3Error details."""
        mock_minio_class.return_value = mock_minio_client
        mock_error = S3Error(
            message='Delete Error',
            resource='test',
            request_id='test',
            host_id='test',
            response='test',
            code='NoSuchKey'
        )
        mock_minio_client.remove_object.side_effect = mock_error
        service = MinioService(minio_config)
        with pytest.raises(FileStorageError) as excinfo:
            await service.delete_file(TEST_BUCKET_NAME, TEST_FILE_NAME)
        assert f"Failed to delete file '{TEST_FILE_NAME}': {mock_error}" in str(
            excinfo.value
        )

    @pytest.mark.asyncio
    @patch('minio.Minio')
    async def test_minio_service_delete_file_general_error(
            self,
            mock_minio_class,
            minio_config,
            mock_minio_client
    ):
        """It should raise a FileStorageError when a general exception occurs
        during file deletion, ensuring that the error message includes both
        the filename and the original error details."""
        mock_minio_class.return_value = mock_minio_client
        mock_minio_client.remove_object.side_effect = Exception(
            'General Delete Error'
        )
        service = MinioService(minio_config)
        with pytest.raises(FileStorageError) as excinfo:
            await service.delete_file(TEST_BUCKET_NAME, TEST_FILE_NAME)
        assert f"Failed to delete file '{TEST_FILE_NAME}': General Delete Error" in str(
            excinfo.value
        )
