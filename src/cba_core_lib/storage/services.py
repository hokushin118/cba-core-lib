"""
File Storage Services.

This module provides the file storage services.
"""
import logging
import os
import uuid
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Tuple, List, Dict, Any, Optional
from urllib.parse import quote

from minio import Minio
from minio.error import S3Error
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from cba_core_lib.storage.configs import MinioConfig
from cba_core_lib.storage.errors import FileStorageError
from cba_core_lib.storage.schemas import FileUploadData
from cba_core_lib.utils.enums import HTTPSchema

logger = logging.getLogger(__name__)


######################################################################
#  FILE STORAGE SERVICE
######################################################################
class FileStorageService(ABC):
    """Abstract Base Class defining the interface for storage services
    (like MinIO, AWS S3, Azure Blob, etc.).
    """

    @abstractmethod
    def check_connection(self) -> bool:
        """Checks the connection and authentication to the storage server.

        Returns:
            True if the connection is successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def ensure_bucket_exists(
            self,
            bucket_name: str
    ) -> None:
        """Ensures that the specified storage bucket exists, creating
        it if necessary.

        Args:
            bucket_name: The name of the bucket to check or create.

        Raises:
            FileStorageError: If an S3Error occurs during bucket operations or
            if an unexpected error happens.
        """
        raise NotImplementedError

    @abstractmethod
    def get_file_url(
            self,
            bucket_name: str,
            object_name: str
    ) -> str:
        """Constructs the publicly accessible URL for the given object.

        Args:
            bucket_name: The name of the bucket.
            object_name: The name of the object within the bucket.

        Returns:
            The full URL string to access the object.

        Raises:
            FileStorageError: If URL construction fails (e.g., missing config).
        """
        raise NotImplementedError

    @abstractmethod
    def is_file_exists(
            self,
            bucket_name: str,
            object_name: str
    ) -> bool:
        """Checks if a file exists in the storage.

        Args:
            bucket_name: The name of the bucket to check.
            object_name: The name of the object to check.

        Returns:
            True if the file exists, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def list_buckets(self) -> List[str]:
        """Lists all available buckets.

        Returns:
            A list of bucket names.

        Raises:
            FileStorageError: If listing buckets fails.
        """
        raise NotImplementedError

    @abstractmethod
    def list_files(
            self,
            bucket_name: str,
            prefix: str = ''
    ) -> List[Dict[str, Any]]:
        """Lists files in a bucket with optional prefix.

        Args:
            bucket_name: The name of the bucket to list files from.
            prefix: Optional prefix to filter files.

        Returns:
            A list of dictionaries containing file information.

        Raises:
            FileStorageError: If listing files fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def upload_file(
            self,
            file_data: FileUploadData,
            bucket_name: str,
            object_name_prefix: str = ''
    ) -> tuple[str, str, int]:
        """Uploads a file to the specified bucket.

        Args:
            file_data: An object conforming to the FileUploadData protocol,
            containing the file's content, size, name, and type.
            bucket_name: The name of the bucket to upload to.
            object_name_prefix: Optional prefix for the object name.

        Returns:
            A tuple containing the object name, ETag, and file size.

        Raises:
            FileStorageError: If the upload fails.
            ValueError: If the input file object is invalid.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_file(
            self,
            bucket_name: str,
            object_name: str
    ) -> None:
        """Deletes a file from the storage.

        Args:
            bucket_name: The name of the bucket containing the file.
            object_name: The name of the object to delete.

        Raises:
            FileStorageError: If the deletion fails.
        """
        raise NotImplementedError


######################################################################
#  MINIO SERVICE
######################################################################
class MinioService(FileStorageService):
    """A service class for interacting with a MinIO server.

    This class provides methods for connecting to MinIO, checking connection
    status, ensuring bucket existence, and uploading files.  It encapsulates
    the MinIO client and handles potential errors, raising custom exceptions
    where appropriate.
    """
    client: Optional[Minio]

    def __init__(
            self,
            config: MinioConfig
    ) -> None:
        """Initializes the MinioService instance and sets up the MinIO client.

        This constructor performs the following actions:

        -   Validates the provided `config` object, ensuring it is an instance
            of `MinioConfig`.
        -   Stores the configuration for later use.
        -   Initializes the MinIO client using the provided configuration
            parameters (endpoint, access key, secret key, SSL setting).
        -   Calls `check_connection` to verify that the client is correctly
            set up and can connect to the MinIO server.
        -   Logs the successful initialization of the client and the endpoint
            it is connected to.

        Args:
            config: A `MinioConfig` object containing the necessary
                configuration settings for connecting to the MinIO server.

        Raises:
            ValueError: If the provided `config` is not a valid
                `MinioConfig` instance.
            FileStorageError: If the MinIO client fails to initialize due to
                invalid configuration, connection problems, or other errors
                during the setup process.
        """
        if not isinstance(
                config,
                MinioConfig
        ):
            raise ValueError(
                'Invalid configuration type provided for MinioService.'
            )
        self.config = config
        self.client = None
        try:
            endpoint = str(config.endpoint)
            endpoint = (
                endpoint.replace(HTTPSchema.HTTP.value, '')
                .replace(HTTPSchema.HTTPS.value, '')
                .rstrip('/')
            )
            self.client = Minio(
                endpoint,
                access_key=config.access_key.get_secret_value(),
                secret_key=config.secret_key.get_secret_value(),
                secure=config.use_ssl
            )
            self.check_connection()
            logger.info(
                "MinIO client initialized and connection "
                "verified for endpoint: %s",
                endpoint
            )

        except (
                S3Error,
                Exception
        ) as err:
            error_message = f"Failed to initialize MinIO client: {err}"
            logger.error(error_message, exc_info=True)
            self.client = None
            raise FileStorageError(error_message) from err

    def _ensure_connection(self) -> None:
        """Verifies that the MinIO client has been successfully initialized.

        This internal method checks if the `self.client` attribute is set.  If
        it is None, it indicates that the MinIO client was not properly
        initialized, and a FileStorageError is raised.  This method should be
        called by other methods to ensure that a valid connection to MinIO
        exists before attempting any operations.

        Raises:
            FileStorageError: If the `self.client` attribute is None,
                indicating that the MinIO client failed to initialize.
        """
        if self.client is None:
            error_message = 'MinIO service client was not initialized ' \
                            'successfully.'
            logger.error(error_message)
            raise FileStorageError(error_message)

    def check_connection(self) -> bool:
        """Checks the connection to the MinIO server and verifies
        authentication.

        This method attempts to list buckets on the MinIO server to confirm
        that the application can successfully connect and authenticate using the
        provided credentials.

        Returns:
            bool: True if the connection and authentication are successful,
                False otherwise.
        """
        self._ensure_connection()
        try:
            self.client.list_buckets()
            logger.info('MinIO connection successful.')
            return True

        except S3Error as err:
            logger.error(
                "MinIO connection check failed: %s",
                err
            )
            return False
        except Exception as err:  # pylint: disable=W0703
            logger.error(
                "MinIO general error during connection check: %s",
                err
            )
            return False

    def ensure_bucket_exists(
            self,
            bucket_name: str
    ) -> None:
        """Ensures that the specified MinIO bucket exists.

        This method checks if a bucket with the given name exists in MinIO.
        If the bucket does not exist, it creates the bucket.

        Args:
            bucket_name (str): The name of the MinIO bucket to check or create.

        Raises:
            FileStorageError: If an S3Error occurs during the bucket existence
                check or creation, or if an unexpected error happens during
                interaction with MinIO.
        """
        self._ensure_connection()
        try:
            found = self.client.bucket_exists(bucket_name)
            if not found:
                self.client.make_bucket(bucket_name)
                logger.info(
                    "Bucket '%s' created.",
                    bucket_name
                )
            else:
                logger.debug(
                    "Bucket '%s' already exists.",
                    bucket_name
                )

        except S3Error as err:
            error_message = f"Could not ensure MinIO bucket '{bucket_name}' " \
                            f"exists: {str(err)}"
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err
        except Exception as err:  # pylint: disable=W0703
            error_message = f"Unexpected error interacting with" \
                            f" bucket '{bucket_name}': {str(err)}"
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err

    def get_file_url(
            self,
            bucket_name: str,
            object_name: str
    ) -> str:
        """Constructs the publicly accessible URL for a given object in a
        MinIO bucket.

        This method generates the HTTP or HTTPS URL that can be used to access
        the specified object, based on the MinIO configuration. It handles
        encoding of special characters in the object name to ensure a
        valid URL.

        Args:
            bucket_name (str): The name of the MinIO bucket containing the
            object.
            object_name (str): The name (key) of the object for which to
            generate the URL.

        Returns:
            str: The fully constructed public URL for the specified MinIO object.

        Raises:
            ValueError: If either `bucket_name` or `object_name` is empty.
            FileStorageError: If the MinIO configuration or endpoint is missing,
                preventing URL generation, or if an unexpected error occurs
                during the URL construction process.
        """
        if not bucket_name:
            raise ValueError(
                'Bucket name cannot be empty.'
            )
        if not object_name:
            raise ValueError(
                'Object name cannot be empty.'
            )
        if not self.config or not self.config.endpoint:
            raise FileStorageError(
                'MinIO configuration or endpoint missing for URL generation.'
            )
        try:
            schema = HTTPSchema.HTTPS if self.config.use_ssl else HTTPSchema.HTTP
            # Convert endpoint to string and remove protocol and trailing slash
            endpoint = str(self.config.endpoint)
            endpoint = (
                endpoint.replace(HTTPSchema.HTTP.value, '')
                .replace(HTTPSchema.HTTPS.value, '')
                .rstrip('/')
            )
            encoded_object_name = quote(object_name)
            url = f"{schema.value}{endpoint}/{bucket_name}/{encoded_object_name}"
            logger.debug(
                "Constructed MinIO file URL: %s",
                url
            )
            return url
        except Exception as err:  # pylint: disable=W0703
            error_message = (
                f"Failed to construct MinIO URL for {bucket_name}/{object_name}: {err}"
            )
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err

    def is_file_exists(
            self,
            bucket_name: str,
            object_name: str
    ) -> bool:
        """Checks whether a specific file (object) exists in the given
        MinIO bucket.

        Args:
            bucket_name (str): The name of the MinIO bucket to check within.
            object_name (str): The name (key) of the file (object) to
            check for.

        Returns:
            bool: True if the file exists in the specified bucket, False
            otherwise.

        Raises:
            ValueError: If either `bucket_name` or `object_name` is empty.
            FileStorageError: If an unexpected error occurs while attempting to
                check the file's existence in MinIO (e.g., connection issues).
                Note that 'NoSuchKey' errors are handled internally and do not
                raise this exception.
        """
        if not bucket_name:
            raise ValueError(
                'Bucket name cannot be empty.'
            )
        if not object_name:
            raise ValueError(
                'Object name cannot be empty.'
            )
        try:
            self._ensure_connection()
            self.client.stat_object(bucket_name, object_name)
            logger.debug(
                "File '%s' exists in bucket '%s'.",
                object_name,
                bucket_name
            )
            return True
        except S3Error as err:
            if err.code == 'NoSuchKey':
                return False
            error_message = f"Failed to check if file exists: {err}"
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err
        except Exception as err:  # pylint: disable=W0703
            error_message = f"Failed to check if file exists: {err}"
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err

    def list_buckets(self) -> List[str]:
        """Retrieves a list of all bucket names available in the MinIO service.

        This method connects to the configured MinIO server and fetches the
        names of all existing buckets.

        Returns:
            List[str]: A list containing the names of all buckets found
            in MinIO.

        Raises:
            FileStorageError: If an error occurs while attempting to list the
                buckets, such as a connection failure or an issue with the
                MinIO server.
        """
        try:
            buckets = self.client.list_buckets()
            bucket_names = [bucket.name for bucket in buckets]
            logger.info(
                "Found %s MinIO buckets.",
                len(bucket_names)
            )
            return bucket_names
        except Exception as err:  # pylint: disable=W0703
            error_message = 'Failed to list buckets'
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err

    def list_files(
            self,
            bucket_name: str,
            prefix: str = ''
    ) -> List[Dict[str, Any]]:
        """Lists the files (objects) within a specified MinIO bucket.

        This method retrieves a list of all files in the given bucket. You can
        optionally provide a prefix to filter the results to only include files
        whose names start with the specified prefix. The listing is recursive,
        meaning it will include files in all sub-directories within the bucket
        and under the given prefix.

        Args:
            bucket_name (str): The name of the MinIO bucket to list files from.
            prefix (str, optional): An optional prefix to filter the listed
                files. Only files with names starting with this prefix will be
                returned. Defaults to an empty string, which lists all files
                in the bucket.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                represents a file (object) and contains the following keys:
                - 'name' (str): The full object name (key).
                - 'size' (int): The size of the file in bytes.
                - 'last_modified' (datetime): The last modification timestamp of
                  the file.
                - 'etag' (str): The ETag (entity tag) of the file.
                - 'is_dir' (bool): Indicates whether the object is a directory
                  (True) or a file (False).

        Raises:
            ValueError: If the `bucket_name` is empty.
            FileStorageError: If an error occurs while attempting to list the
                files in the MinIO bucket, such as connection issues or
                permission problems.
        """
        if not bucket_name:
            raise ValueError(
                'Bucket name cannot be empty.'
            )
        try:
            self._ensure_connection()
            objects = self.client.list_objects(
                bucket_name=bucket_name,
                prefix=prefix,
                recursive=True  # Include objects in sub-directories
            )
            file_list = [
                {
                    'name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified,
                    'etag': obj.etag,
                    'is_dir': obj.is_dir
                } for obj in objects if obj.size is not None
            ]
            logger.info(
                "Found %s objects in '%s' with prefix '%s'.",
                len(file_list),
                bucket_name,
                prefix
            )
            return file_list
        except Exception as err:  # pylint: disable=W0703
            error_message = f"Failed to list files in bucket '{bucket_name}'"
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err

    @retry(
        retry=retry_if_exception_type(FileStorageError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        reraise=True
    )
    def _upload_with_retry(
            self,
            bucket_name: str,
            object_name: str,
            content_bytes: bytes,
            file_size: int,
            content_type: Optional[str],
            filename_for_log: str
    ) -> Tuple[str, str, int]:
        """Internal method to upload a file to MinIO with retry logic.

        This method encapsulates the file upload operation to MinIO, including
        error handling and retry mechanisms. It is decorated with the `retry`
        decorator to automatically retry the upload if it fails with a
        `FileStorageError`.

        Args:
            bucket_name (str): The name of the MinIO bucket to upload the
            file to.
            object_name (str): The desired name (object key) for the file
            in MinIO.
            content_bytes (bytes): The byte content of the file to be uploaded.
            file_size (int): The size of the file content in bytes.
            content_type (Optional[str]): The MIME type of the file. If None,
                'application/octet-stream' is used as a default.
            filename_for_log (str): The original filename of the file.  Used for
                logging purposes.

        Returns:
            Tuple[str, str, int]: A tuple containing:
                - The object name (key) of the uploaded file in MinIO.
                - The ETag (entity tag) of the uploaded file, which can be used
                  for verifying data integrity.
                - The size of the uploaded file in bytes.

        Raises:
            FileStorageError: If the file upload to MinIO fails after all retry
                attempts.  This exception may wrap an underlying `S3Error` or
                a general `Exception` from the MinIO client.
        """
        try:
            file_data_stream = BytesIO(content_bytes)
            # Get content type from headers if not provided
            effective_content_type = content_type or 'application/octet-stream'

            result = self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file_data_stream,
                length=file_size,
                content_type=effective_content_type
            )
            # Strip quotes from ETag if present
            etag = result.etag.strip('"') if result.etag else ''

            logger.info(
                "File '%s' uploaded successfully to '%s/%s'. ETag: %s",
                filename_for_log or 'unnamed file',
                bucket_name,
                object_name,
                etag
            )

            return object_name, etag, file_size

        except S3Error as err:
            error_message = (
                f"MinIO S3 upload failed for '{filename_for_log}'"
                f" (object: {object_name}): {err}"
            )
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err
        except Exception as err:  # pylint: disable=W0703
            error_message = (
                f"Unexpected error during MinIO put_object for "
                f"'{filename_for_log}' (object: {object_name}): {err}"
            )
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err

    async def upload_file(
            self,
            file_data: FileUploadData,
            bucket_name: str,
            object_name_prefix: str = ''
    ) -> Tuple[str, str, int]:
        """Uploads a file to the specified MinIO bucket.

        Args:
            file_data (FileUploadData): An object conforming to the FileUploadData
                protocol, containing the file's content, size, filename, and
                content type.
            bucket_name (str): The name of the MinIO bucket where the file will
                be uploaded.
            object_name_prefix (str, optional): An optional prefix to be added
                to the generated object name in MinIO. Defaults to ''.

        Returns:
            Tuple[str, str, int]: A tuple containing:
                - The generated object name in MinIO.
                - The ETag of the uploaded object.
                - The size of the uploaded file in bytes.

        Raises:
            ValueError: If the `bucket_name` is empty, the `file_data` object
                does not conform to the `FileUploadData` protocol, the
                `file_data.content_bytes` is empty, or the `file_data.filename`
                is missing (required for determining the file extension).
            FileStorageError: If the upload to MinIO fails after retries due to
                connection issues or other S3-related errors.
        """
        if not bucket_name:
            raise ValueError(
                'Bucket name cannot be empty.'
            )
        if not isinstance(
                file_data,
                FileUploadData
        ):
            raise ValueError(
                'Invalid file_data object provided.'
            )
        if not file_data.content_bytes:
            raise ValueError(
                'File data content cannot be empty.'
            )

        # Use provided filename or fallback for logging/extension extraction
        source_filename = file_data.filename
        log_filename = source_filename or "[no filename provided]"

        if not source_filename:
            raise ValueError(
                'Filename is required for upload to determine extension.'
            )

        self.ensure_bucket_exists(bucket_name)

        try:
            # Generate a unique object name using UUID and original extension
            file_extension = os.path.splitext(file_data.filename)[
                1] if file_data.filename else ''
            unique_id = uuid.uuid4()
            object_name = f"{object_name_prefix}{unique_id}{file_extension}"

            logger.debug(
                "Attempting MinIO upload: '%s' (%s bytes, type: %s) to bucket '%s'.",
                object_name,
                file_data.size,
                file_data.content_type or 'unknown',
                bucket_name
            )

            # Upload the file with retries
            return self._upload_with_retry(
                bucket_name=bucket_name,
                object_name=object_name,
                content_bytes=file_data.content_bytes,
                file_size=file_data.size,
                content_type=file_data.content_type,
                filename_for_log=log_filename
            )

        except ValueError as value_err:
            raise value_err
        except FileStorageError as fse:
            raise fse
        except Exception as err:  # pylint: disable=W0703
            error_message = f"Unexpected error preparing upload for '': {err}"
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err

    async def delete_file(
            self,
            bucket_name: str,
            object_name: str
    ) -> None:
        """Deletes a file (object) from the specified MinIO bucket.

        Args:
            bucket_name (str): The name of the bucket from which to delete
            the file.
            object_name (str): The name of the file (object key) to delete.

        Raises:
            ValueError: If either `bucket_name` or `object_name` is empty.
            FileStorageError: If the deletion operation fails due to an
                underlying MinIO error or a connection issue.
        """
        self._ensure_connection()
        if not bucket_name:
            raise ValueError(
                'Bucket name cannot be empty.'
            )
        if not object_name:
            raise ValueError(
                'Object name cannot be empty.'
            )
        try:
            self.client.remove_object(bucket_name, object_name)
            logger.info(
                "File '%s' deleted from bucket '%s'",
                object_name,
                bucket_name
            )
        except Exception as err:  # pylint: disable=W0703
            error_message = f"Failed to delete file '{object_name}': {err}"
            logger.error(error_message, exc_info=True)
            raise FileStorageError(error_message) from err
