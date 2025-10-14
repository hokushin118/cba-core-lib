"""
Schemas for File Metadata.

All file metadata schemas are stored in this module.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ConfigDict, model_serializer


######################################################################
#  FILE METADATA SCHEMAS
######################################################################
class BaseMetadataDTO(BaseModel):
    """Base schema for metadata stored in MongoDB,
    includes auto _id and timestamps."""
    id: Optional[UUID] = Field(
        default_factory=uuid4,
        alias='_id',
        description='Document identification number.',
        examples=['a1b2c3d4-e5f6-7890-1234-567890abcdef']
    )
    created_at: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description='Created at.',
        examples=['2024-08-28T10:00:00Z']
    )
    updated_at: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description='Updated at.',
        examples=['2024-08-28T12:30:00Z']
    )

    model_config = ConfigDict(
        # Allow using "_id" or "id"
        populate_by_name=True,
        extra='allow',
        # True. Instances of this class are immutable
        frozen=True
    )

    @model_serializer
    def serialize_datetimes(self) -> dict:
        """Custom serializer to ensure datetime objects are ISO formatted
        and timezone-aware."""
        data = self.model_dump()
        if data['created_at']:
            data['created_at'] = data['created_at'].isoformat()
        if data['updated_at']:
            data['updated_at'] = data['updated_at'].isoformat()
        return data


class UploadMetadataDTO(BaseMetadataDTO):
    """Schema for metadata associated with an uploaded file.
    Inherits _id and timestamps from BaseMetadata.
    """
    original_filename: str = Field(
        ...,
        description='The original filename provided during upload.',
        examples=['vacation_photo.png']
    )
    object_name: str = Field(
        ...,
        description='The final name (key) of the object stored in the bucket.',
        examples=['user_data/vacation_photo_uuid123.png']
    )
    bucket_name: str = Field(
        ...,
        description='Name of the storage bucket.',
        examples=['test-bucket']
    )
    content_type: Optional[str] = Field(
        None,
        description='The MIME type of the file.',
        examples=['text/plain']
    )
    size: int = Field(
        ...,
        ge=0,
        description='The size of the file content in bytes.',
        examples=[5242880]  # 5 MB example
    )
    etag: Optional[str] = Field(
        None,
        description='The ETag (Entity Tag) of the uploaded object from storage.',
        examples=['fba9dede5f27731c9771645a39863328']
    )
    uploaded_by: Optional[str] = Field(
        None,
        description='Identifier of the user who uploaded the file.',
        examples=['13b914bb-443f-4319-97a9-7a7bb5fdb9f3']
    )

    model_config = ConfigDict(
        # Extra fields in the input data will be silently ignored
        extra='ignore',
        # Allow model creation from attributes
        from_attributes=True,
        # Allows the model to be populated by aliases
        populate_by_name=True,
        # True. Instances of this class are immutable
        frozen=True,
    )


class UpdateMetadataDTO(BaseModel):
    """Schema for partially updating file metadata.
    All fields are optional; only provided fields will be updated.
    """
    original_filename: Optional[str] = Field(
        None,
        description='New original filename.',
        examples=['vacation_photo.png']
    )
    object_name: Optional[str] = Field(
        None,
        description='New name (key) of the object stored in the bucket.',
        examples=['user_data/vacation_photo_uuid123.png']
    )
    content_type: Optional[str] = Field(
        None,
        description='Updated MIME type of the file.',
        examples=['text/plain']
    )
    etag: Optional[str] = Field(
        None,
        description='Updated ETag (Entity Tag) of the object from storage.',
        examples=['fba9dede5f27731c9771645a39863328']
    )

    model_config = ConfigDict(
        # Raise an exception if extra fields are provided
        extra='forbid',
        # Allow model creation from attributes
        from_attributes=True,
        # Allows the model to be populated by aliases
        populate_by_name=True,
        # True. Instances of this class are immutable
        frozen=True,
    )
