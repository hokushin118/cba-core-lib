"""
Framework-Agnostic File Upload Data Protocol.

This module defines the `FileUploadData` protocol, specifying a standard
structure for representing uploaded file data *after* its content has been
read into memory. This approach decouples file storage logic from specific web
framework implementations (like FastAPI, Flask, etc.).

By depending on this protocol, services can process file uploads consistently,
regardless of the source framework.

It also includes `SimpleFileData`, a basic Pydantic implementation of the
protocol, useful for testing or simple data transfer.

Example Usage (FastAPI):
    ```python
    import fastapi
    from fastapi import Depends, UploadFile
    from pydantic import BaseModel
    from typing import Optional

    # Assuming FileStorageService is defined elsewhere and expects FileUploadData
    from your_storage_module import FileStorageService
    # Import the protocol and implementation from this module
    from this_module import FileUploadData, SimpleFileData

    # The endpoint receives the framework-specific UploadFile
    async def upload_endpoint(
        file: UploadFile,
        storage_service: FileStorageService = Depends(...)
    ):
        # 1. Read content and close the framework's file stream
        contents = await file.read()
        await file.close()

        # 2. Create an object conforming to the protocol
        file_data: FileUploadData = SimpleFileData(
            content_bytes=contents,
            size=len(contents),
            filename=file.filename,
            content_type=file.content_type
        )

        # 3. Pass the protocol-conformant object to the service
        object_name, etag, size = await storage_service.upload_file(
            file_data=file_data,
            bucket_name="your-target-bucket"
        )

        return {
            "message": "Upload successful",
            "filename": file_data.filename,
            "object_name": object_name,
            "etag": etag,
            "size": size
        }
    ```
"""
from typing import Optional, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, model_validator


######################################################################
#  FILE STORAGE SCHEMAS
######################################################################
@runtime_checkable
class FileUploadData(Protocol):
    """Protocol defining the structure of uploaded file data once its
    content has been read into memory. This abstraction enables the
    file storage service to operate independently of specific web
    frameworks.

    The  FileUploadData protocol describes the structure of data representing
    a file after its content has been read.  A concrete class implementing
    this protocol is expected to provide the following attributes.

    Attributes:
        filename (Optional[str]): The original name of the uploaded file,
            if provided by the client.
        content_type (Optional[str]): The MIME type of the file, as
            detected by the client or server, if available.
        content_bytes (bytes): The complete byte content of the uploaded file
            read into memory.
        size (int): The total size of the file content in bytes.

    Implementer Notes:
        Classes implementing this protocol should ensure that instances of the
        class have these four attributes.
    """
    filename: Optional[str]
    content_type: Optional[str]
    content_bytes: bytes
    size: int


class SimpleFileData(BaseModel):
    """Model for upload file. Simple implementation of the
    FileUploadData protocol.

    This class provides a basic data structure to represent an uploaded file,
    conforming to the FileUploadData protocol. It's designed to hold the
    essential file information after the file's content has been read into
    memory.

    Attributes:
        content_bytes (bytes): The raw byte content of the uploaded file.
        size (int): The size of the file content in bytes.
        filename (Optional[str], optional): The original name of the uploaded
        file. Defaults to None.
        content_type (Optional[str], optional): The MIME type of the file.
        Defaults to None.
    """
    content_bytes: bytes
    size: int
    filename: Optional[str] = None
    content_type: Optional[str] = None

    model_config = ConfigDict(
        #  Extra fields in the input data will be
        #  silently ignored
        extra='ignore',
        # pylint: disable=too-few-public-methods
        from_attributes=True,
        # Allows the model to be populated by aliases
        populate_by_name=True,
        # True. Instances of this class are immutable
        frozen=True,
    )

    @model_validator(mode='after')
    def check_size_matches_content(self) -> 'SimpleFileData':
        """Ensures the provided 'size' matches the actual length of
        'content_bytes'."""
        if self.size != len(self.content_bytes):
            raise ValueError(
                f"Inconsistent file data: provided size ({self.size}) "
                f"does not match content_bytes length ({len(self.content_bytes)})."
            )
        return self
