"""
MinioService Class Integration Test Suite using testcontainers.

It verifies the MinioService class interacts correctly with a
real MinIO server running inside a Docker container, testing its
core functionalities like uploading, retrieving URLs, handling
prefixes and content types, managing buckets, checking file existence,
listing files and buckets, and ensuring the retry mechanism works
as expected for transient errors.

Test cases can be run with the following:
  pytest -v --with-integration --log-cli-level=DEBUG tests/integration
"""
import os
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from minio.error import S3Error

from cba_core_lib.storage.schemas import FileUploadData, SimpleFileData
from cba_core_lib.storage.services import MinioService
from tests.conftest import (
    TEST_BUCKET_NAME,
    TEST_FILES_FOLDER,
    TEST_FILE_NAME,
    TEST_FILE_CONTENT,
    TEST_CONTENT_TYPE,
    TEST_ETAG
)


######################################################################
#  MINIO SERVICE INTEGRATION TEST CASES
######################################################################
@pytest.mark.integration
class TestMinioServiceIntegration:
    """The MinioService Class Integration Tests."""

    @pytest.mark.asyncio
    async def test_upload_and_get_file_url(
            self,
            minio_service: MinioService,
            test_upload_file: FileUploadData
    ):
        """It should upload a file to MinIO and then successfully
        retrieve the public URL for that uploaded file."""
        # Upload the file
        object_name, etag, file_size = await minio_service.upload_file(
            test_upload_file,
            TEST_BUCKET_NAME
        )

        # Verify the upload was successful
        assert object_name is not None
        assert etag is not None
        assert file_size == len(TEST_FILE_CONTENT)

        # Get the file URL
        file_url = minio_service.get_file_url(TEST_BUCKET_NAME, object_name)
        assert file_url is not None
        assert TEST_BUCKET_NAME in file_url
        assert object_name in file_url

    @pytest.mark.asyncio
    async def test_upload_file_with_prefix(
            self,
            minio_service: MinioService,
            test_upload_file: FileUploadData
    ):
        """It should upload a file to MinIO with the specified
        object name prefix."""
        prefix = 'test-prefix/'
        object_name, etag, file_size = await minio_service.upload_file(
            test_upload_file,
            TEST_BUCKET_NAME,
            object_name_prefix=prefix
        )

        assert object_name.startswith(prefix)
        assert etag is not None
        assert file_size == len(TEST_FILE_CONTENT)

    @pytest.mark.asyncio
    async def test_upload_file_with_content_type(
            self,
            minio_service: MinioService,
            test_upload_file: FileUploadData
    ):
        """It should upload a file to MinIO and preserve the
        specified content type."""
        object_name, _, _ = await minio_service.upload_file(
            test_upload_file,
            TEST_BUCKET_NAME
        )

        # Get the file from MinIO to verify content type
        client = minio_service.client
        stat = client.stat_object(TEST_BUCKET_NAME, object_name)
        assert stat.content_type == TEST_CONTENT_TYPE

    @pytest.mark.asyncio
    async def test_upload_file_without_content_type(
            self,
            minio_service: MinioService
    ):
        """It should upload a file to MinIO and default to
        'application/octet-stream' when no content type is provided."""
        file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=len(TEST_FILE_CONTENT),
            filename=TEST_FILE_NAME
        )
        object_name, _, _ = await minio_service.upload_file(
            file_data,
            TEST_BUCKET_NAME
        )

        # Get the file from MinIO to verify content type
        client = minio_service.client
        stat = client.stat_object(TEST_BUCKET_NAME, object_name)
        assert stat.content_type == 'application/octet-stream'

    @pytest.mark.asyncio
    async def test_upload_file_no_filename(
            self,
            minio_service: MinioService
    ):
        """It should raise a ValueError when attempting to upload
        a file with an empty filename."""
        file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=len(TEST_FILE_CONTENT),
            filename=""
        )
        with pytest.raises(ValueError) as excinfo:
            await minio_service.upload_file(
                file_data,
                TEST_BUCKET_NAME
            )
        assert 'Filename is required for upload to determine extension.' in str(
            excinfo.value
        )

    @pytest.mark.asyncio
    async def test_upload_file_to_nonexistent_bucket(
            self,
            minio_service: MinioService,
            test_upload_file: FileUploadData
    ):
        """It should automatically create the specified bucket if it
        does not exist and then successfully upload the file."""
        nonexistent_bucket = f"nonexistent-bucket-{uuid.uuid4()}"
        object_name, etag, file_size = await minio_service.upload_file(
            test_upload_file,
            nonexistent_bucket
        )

        # Verify the bucket was created and the file was uploaded
        assert object_name is not None
        assert etag is not None
        assert file_size == len(TEST_FILE_CONTENT)

        # Verify the bucket exists
        client = minio_service.client
        assert client.bucket_exists(nonexistent_bucket)

    @pytest.mark.asyncio
    async def test_delete_file(
            self,
            minio_service: MinioService,
            test_upload_file: FileUploadData
    ):
        """It should successfully delete a file from the specified
        bucket in MinIO."""
        # First upload a file
        object_name, _, _ = await minio_service.upload_file(
            test_upload_file,
            TEST_BUCKET_NAME
        )

        # Verify the file exists
        assert minio_service.is_file_exists(TEST_BUCKET_NAME, object_name)

        # Delete the file
        await minio_service.delete_file(TEST_BUCKET_NAME, object_name)

        # Verify the file no longer exists
        assert not minio_service.is_file_exists(TEST_BUCKET_NAME, object_name)

    @pytest.mark.asyncio
    async def test_file_exists(
            self,
            minio_service: MinioService,
            test_upload_file: FileUploadData
    ):
        """It should correctly identify whether a file exists in the
        specified bucket in MinIO."""
        # First upload a file
        object_name, _, _ = await minio_service.upload_file(
            test_upload_file,
            TEST_BUCKET_NAME
        )

        # Verify the file exists
        assert minio_service.is_file_exists(TEST_BUCKET_NAME, object_name)

        # Verify a non-existent file returns False
        assert not minio_service.is_file_exists(
            TEST_BUCKET_NAME,
            'nonexistent-file.txt'
        )

    @pytest.mark.asyncio
    async def test_list_buckets(
            self,
            minio_service: MinioService
    ):
        """It should return a list of all bucket names present
        in the MinIO server."""
        # Create a new bucket
        new_bucket = f"test-bucket-{uuid.uuid4()}"
        minio_service.ensure_bucket_exists(new_bucket)

        # List all buckets
        buckets = minio_service.list_buckets()

        # Verify our test buckets are in the list
        assert TEST_BUCKET_NAME in buckets
        assert new_bucket in buckets

    @pytest.mark.asyncio
    # pylint: disable=R0914
    async def test_list_files(
            self,
            minio_service: MinioService
    ):
        """It should return a list of file information (name, size,
        content type) for files in the specified bucket, optionally
        filtered by a prefix."""
        # Upload files with different prefixes
        prefix1 = 'test-prefix-1/'
        prefix2 = 'test-prefix-2/'

        # Create test files
        integration_tests_dir_path = Path(os.path.dirname(__file__))
        tests_dir = str(integration_tests_dir_path.parent)
        test_file_path1 = os.path.join(
            tests_dir,
            TEST_FILES_FOLDER,
            'test_file1.txt'
        )
        test_file_path2 = os.path.join(
            tests_dir,
            TEST_FILES_FOLDER,
            'test_file2.txt'
        )

        try:
            # Create first test file
            with open(test_file_path1, 'wb') as file_obj:
                file_obj.write(TEST_FILE_CONTENT)

            # Create second test file
            with open(test_file_path2, 'wb') as file_obj:
                file_obj.write(TEST_FILE_CONTENT)

            # Upload first file
            file1 = SimpleFileData(
                content_bytes=TEST_FILE_CONTENT,
                size=len(TEST_FILE_CONTENT),
                filename='test_file1.txt',
                content_type=TEST_CONTENT_TYPE
            )
            object_name1, _, _ = await minio_service.upload_file(
                file1,
                TEST_BUCKET_NAME,
                object_name_prefix=prefix1
            )

            # Upload second file
            file2 = SimpleFileData(
                content_bytes=TEST_FILE_CONTENT,
                size=len(TEST_FILE_CONTENT),
                filename='test_file2.txt',
                content_type=TEST_CONTENT_TYPE
            )
            object_name2, _, file_size1 = await minio_service.upload_file(
                file2,
                TEST_BUCKET_NAME,
                object_name_prefix=prefix2
            )

            # List all files
            all_files = minio_service.list_files(TEST_BUCKET_NAME)
            assert len(all_files) >= 2

            # List files with prefix1
            prefix1_files = minio_service.list_files(
                TEST_BUCKET_NAME,
                prefix=prefix1
            )
            assert len(prefix1_files) == 1
            assert prefix1_files[0]['name'] == object_name1

            # List files with prefix2
            prefix2_files = minio_service.list_files(
                TEST_BUCKET_NAME,
                prefix=prefix2
            )
            assert len(prefix2_files) == 1
            assert prefix2_files[0]['name'] == object_name2

            # Verify file metadata
            file_info = prefix1_files[0]
            assert file_info['size'] == file_size1
            assert file_info['last_modified'] is not None
            assert file_info['etag'] is not None
            assert not file_info['is_dir']
        finally:
            # Clean up test files
            if os.path.exists(test_file_path1):
                os.remove(test_file_path1)
            if os.path.exists(test_file_path2):
                os.remove(test_file_path2)

    @pytest.mark.asyncio
    async def test_retry_mechanism(
            self,
            minio_service: MinioService,
            monkeypatch
    ):
        """It should retry the file upload operation up to the configured
        number of times if transient S3 errors occur, and succeed if the
        operation eventually becomes successful."""
        attempt_count = 0

        def mock_put_object(*args, **kwargs):  # pylint: disable=W0613
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count <= 2:
                raise S3Error(
                    message='Transient error',
                    resource='test',
                    response='test',
                    request_id='test',
                    host_id='test',
                    code='TestError'
                )
            # Create a mock result that matches what the original
            # put_object would return
            mock_result = MagicMock()
            mock_result.etag = TEST_ETAG
            return mock_result

        monkeypatch.setattr(
            minio_service.client,
            'put_object',
            mock_put_object
        )

        # Create test file data
        file_data = SimpleFileData(
            content_bytes=TEST_FILE_CONTENT,
            size=len(TEST_FILE_CONTENT),
            filename='retry_test_file.txt',
            content_type=TEST_CONTENT_TYPE
        )

        try:
            # Upload should succeed after retries
            object_name, etag, file_size = await minio_service.upload_file(
                file_data,
                TEST_BUCKET_NAME
            )
            assert object_name is not None
            assert etag == TEST_ETAG
            assert file_size == len(TEST_FILE_CONTENT)
            assert attempt_count == 3  # Two failures + one success
        finally:
            # Clean up the test file
            test_file_path = os.path.join(
                str(Path(os.path.dirname(__file__)).parent),
                TEST_FILES_FOLDER,
                'retry_test_file.txt'
            )
            if os.path.exists(test_file_path):
                os.remove(test_file_path)
