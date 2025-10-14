"""
File Metadata Storage Services.

This module provides the file metadata storage services.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import timezone, datetime
from typing import Optional, Dict, Any, List, Union
from uuid import UUID

from cba_core_lib.metadata.configs import MongoConfig
from cba_core_lib.metadata.errors import MetadataError
from cba_core_lib.metadata.schemas import (
    UploadMetadataDTO,
    UpdateMetadataDTO
)

logger = logging.getLogger(__name__)


######################################################################
#  FILE METADATA STORAGE SERVICE
######################################################################
class MetadataStorageService(ABC):
    """Abstract Base Class defining the CRUD interface for asynchronous metadata
    storage services (e.g., MongoDB, PostgreSQL).
    """

    @abstractmethod
    async def check_connection(self) -> bool:
        """Asynchronously checks the connection and authentication to the
        metadata storage server. Primarily intended for diagnostic or
        startup checks.

        Returns:
            True if the connection is successful, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    async def save_metadata(
            self,
            metadata: UploadMetadataDTO,
            collection_or_table_name: str
    ) -> str:
        """Saves a new metadata record.

        Args:
            metadata: The UploadMetadataDTO object to save.
            collection_or_table_name: Target collection or table name.

        Returns:
            The string representation of the unique ID (_id/id) assigned to the record.

        Raises:
            MetadataError: If saving fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_metadata_by_id(
            self,
            metadata_id: Union[str, UUID],
            collection_or_table_name: str
    ) -> Optional[UploadMetadataDTO]:
        """Retrieves a metadata record by its unique ID.

        Args:
            metadata_id: The unique ID (string or UUID) of the record.
            collection_or_table_name: Target collection or table name.

        Returns:
            The found UploadMetadataDTO object, or None if not found.

        Raises:
            MetadataError: If retrieval fails for reasons other than not found.
        """
        raise NotImplementedError

    @abstractmethod
    async def find_metadata(
            self,
            query: Dict[str, Any],
            collection_or_table_name: str,
            skip: int = 0,
            limit: Optional[int] = None
    ) -> List[UploadMetadataDTO]:
        """Finds metadata records matching a given query, with pagination.
        Note: The structure and capabilities of the 'query' dict depend heavily
        on the underlying database implementation (e.g., MongoDB query operators
        vs. simple key-value for SQL WHERE or JSONB operators).

        Args:
            query: A dictionary representing the query criteria.
            collection_or_table_name: Target collection or table name.
            skip: Number of records to skip (for pagination).
            limit: Maximum number of records to return (for pagination).

        Returns:
            A list of matching UploadMetadataDTO objects.

        Raises:
            MetadataError: If the find operation fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def update_metadata(
            self,
            metadata_id: Union[str, UUID],
            update_data: UpdateMetadataDTO,
            collection_or_table_name: str
    ) -> Optional[UploadMetadataDTO]:
        """Updates an existing metadata record identified by its ID.
        Performs a partial update based on fields present in update_data.

        Args:
            metadata_id: The unique ID (string or UUID) of the record to update.
            update_data: An UpdateMetadataDTO object containing fields to update.
            collection_or_table_name: Target collection or table name.

        Returns:
            The updated UploadMetadataDTO object, or None if the record was not found.

        Raises:
            MetadataError: If the update operation fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_metadata(
            self,
            metadata_id: Union[str, UUID],
            collection_or_table_name: str
    ) -> bool:
        """Deletes a metadata record by its unique ID.

        Args:
            metadata_id: The unique ID (string or UUID) of the record to delete.
            collection_or_table_name: Target collection or table name.

        Returns:
            True if a record was successfully deleted, False otherwise
            (e.g., not found).

        Raises:
            MetadataError: If the delete operation fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        """Closes connections or releases resources."""
        raise NotImplementedError


######################################################################
#  MONGO SERVICE
######################################################################

def _raise_motor_import_error():
    raise ImportError(
        "Dependency 'motor' not found. Install with "
        "'pip install cba_core_lib[storage]' to enable "
        "logic for MongoService."
    )


# --- Lazy import motor dependencies ---
_MOTOR_AVAILABLE = False
try:
    # pylint: disable=W0611
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
        RetryError as TenacityRetryError
    )
    # pylint: disable=W0611
    from motor.motor_asyncio import (
        AsyncIOMotorClient,
        AsyncIOMotorDatabase,
        AsyncIOMotorCollection
    )
    from pymongo.errors import (
        ConnectionFailure,
        OperationFailure,
        ConfigurationError
    )
    from pymongo.results import (
        InsertOneResult,
        DeleteResult,
        UpdateResult
    )
    from pymongo import ReturnDocument

    _MOTOR_AVAILABLE = True
except ImportError:
    _raise_motor_import_error()
    stop_after_attempt = Any
    wait_exponential = Any
    retry_if_exception_type = Any
    AsyncIOMotorClient = Any
    AsyncIOMotorDatabase = Any
    AsyncIOMotorCollection = Any
    ConnectionFailure = Exception
    OperationFailure = Exception
    ConfigurationError = Exception
    InsertOneResult = Any
    DeleteResult = Any
    UpdateResult = Any
    ReturnDocument = Any


class MongoService(MetadataStorageService):
    """MongoDB implementation of the MetadataStorageService using the async
    'motor' driver. Handles CRUD operations for file metadata.
    """
    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None

    def __init__(
            self,
            config: MongoConfig
    ) -> None:
        """Initializes the MongoService with configuration. Connection is lazy.

        Args:
            config: A MongoConfig object with connection details.

        Raises:
            ImportError: If the 'motor' package is not installed.
            ValueError: If the provided config is invalid.
        """
        if not _MOTOR_AVAILABLE:
            _raise_motor_import_error()
        if not isinstance(config, MongoConfig):
            raise ValueError(
                'Invalid MongoConfig object provided.'
            )

        self.config = config
        logger.info(
            "MongoService initialized for database: %s (lazy connection)",
            self.config.db_name
        )

    ######################################################################
    #  CONNECTION MANAGEMENT
    ######################################################################
    @retry(
        retry=retry_if_exception_type(MetadataError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        reraise=True
    )
    async def _get_db(self) -> AsyncIOMotorDatabase:
        """Gets the Motor database instance, establishing connection
        if needed."""
        if self._client is None:
            await self._initialize_client()

        if self._db is None:
            if self._client:
                self._db = self._client[self.config.DB_NAME]
            else:
                raise MetadataError(
                    'MongoDB client is None, cannot get database.'
                )

        if self._db is None:
            raise MetadataError(
                'MongoDB database instance is not available.'
            )

        return self._db

    async def _initialize_client(self):
        """Creates the motor client and selects the database."""
        logger.info("Initializing motor client and connecting to MongoDB...")
        uri = self.config.URI.get_secret_value()
        try:
            # Can add pool options from config here: minPoolSize, maxPoolSize etc.
            self._client = AsyncIOMotorClient(
                uri,
                uuidRepresentation='standard'
            )
            # Ping to verify connection early
            await self._client.admin.command('ping')
            self._db = self._client[self.config.DB_NAME]
            logger.info(
                "Motor client connected successfully to database: %s",
                self.config.db_name
            )
        except (
                ConnectionFailure,
                ConfigurationError
        ) as err:
            logger.critical(
                "Motor client connection/configuration failed: %s",
                err,
                exc_info=True
            )
            self._client = None
            self._db = None
            raise MetadataError(
                f"Could not connect to MongoDB: {err}",
                original_exception=err
            ) from err
        except Exception as err:
            logger.critical(
                "Unexpected error initializing motor client: %s",
                err,
                exc_info=True
            )
            self._client = None
            self._db = None
            raise MetadataError(
                f"Unexpected error connecting to MongoDB: {err}",
                original_exception=err
            ) from err

    async def _get_collection(
            self,
            collection_name: str
    ) -> AsyncIOMotorCollection:
        """Gets a specific Motor collection instance."""
        if not collection_name:
            raise ValueError(
                "Collection name cannot be empty."
            )
        db = await self._get_db()
        return db[collection_name]

    async def close(self):
        """Closes the MongoDB client connection."""
        if self._client:
            logger.info(
                'Closing motor client connection.'
            )
            self._client.close()
            self._client = None
            self._db = None
            logger.info(
                'Motor client connection closed.'
            )

    async def check_connection(self) -> bool:
        """Checks connection using ping."""
        try:
            if self._client is None:
                await self._get_db()
                return True
            else:
                await self._client.admin.command('ping')
                logger.debug(
                    'MongoDB ping successful.'
                )
                return True
        except (
                ConnectionFailure,
                OperationFailure,
                MetadataError
        ) as err:
            logger.error(
                'MongoDB connection check failed: %s',
                err,
                exc_info=True
            )
            return False
        except Exception as err:
            logger.error(
                "Unexpected error during MongoDB connection check: %s",
                err,
                exc_info=True
            )
            return False

    ######################################################################
    #  CRUD OPERATIONS
    ######################################################################
    async def save_metadata(
            self,
            metadata: UploadMetadataDTO,
            collection_name: str
    ) -> str:
        if not isinstance(metadata, UploadMetadataDTO):
            raise ValueError(
                "Invalid metadata object provided. Expected UploadMetadataDTO."
            )

        try:
            collection = await self._get_collection(collection_name)

            # Prepare dict using Pydantic validation to populate defaults
            # Use model_dump for V2
            validated_metadata = UploadMetadataDTO.model_validate(
                metadata.model_dump()
            )
            metadata_dict = validated_metadata.model_dump(
                by_alias=True,
                exclude_unset=True
            )

            record_id = validated_metadata.id

            logger.debug(
                "Inserting document with ID %s into collection '%s'",
                record_id,
                collection_name
            )
            result: InsertOneResult = await collection.insert_one(
                metadata_dict
            )

            if result.acknowledged and result.inserted_id:
                logger.info(
                    "MongoDB insert success: ID %s into '%s'",
                    record_id,
                    collection_name
                )
                return str(record_id)
            else:
                logger.error(
                    "Insert operation into '%s' failed (not acknowledged or no ID).",
                    collection_name
                )
                raise MetadataError(
                    f"Insert operation into '{collection_name}' failed (not acknowledged)."
                )

        except (
                OperationFailure,
                ConnectionFailure
        ) as err:
            logger.error(
                "MongoDB save error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"MongoDB save failed: {getattr(err, 'details', str(err))}",
                original_exception=err
            ) from err
        except Exception as err:
            logger.error(
                "Unexpected save error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"Unexpected error saving metadata: {err}",
                original_exception=err
            ) from err

    async def get_metadata_by_id(
            self,
            metadata_id: Union[str, UUID],
            collection_name: str
    ) -> Optional[UploadMetadataDTO]:
        try:
            search_id = UUID(str(metadata_id))
        except ValueError:
            logger.warning(
                "Invalid UUID format provided for get_metadata_by_id: %s",
                metadata_id
            )
            return None

        try:
            collection = await self._get_collection(collection_name)
            logger.debug(
                "MongoDB finding doc ID %s in '%s'",
                search_id,
                collection_name
            )

            doc = await collection.find_one({'_id': search_id})

            if doc:
                logger.info(
                    "MongoDB found doc ID %s in '%s'",
                    search_id,
                    collection_name
                )
                try:
                    return UploadMetadataDTO.model_validate(doc)
                except Exception as val_err:
                    logger.error(
                        "DB data validation failed for ID %s: %s",
                        search_id,
                        val_err,
                        exc_info=True
                    )
                    raise MetadataError(
                        f"Failed to validate metadata from DB for ID {search_id}",
                        original_exception=val_err
                    )
            else:
                logger.debug(
                    "MongoDB doc ID %s not found in '%s'",
                    search_id,
                    collection_name
                )
                return None
        except (
                OperationFailure,
                ConnectionFailure
        ) as err:
            logger.error(
                "MongoDB get error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"MongoDB get failed: {getattr(err, 'details', str(err))}",
                original_exception=err
            ) from err
        except Exception as err:
            logger.error(
                "Unexpected get error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"Unexpected error getting metadata: {err}",
                original_exception=err
            ) from err

    async def find_metadata(
            self,
            query: Dict[str, Any],
            collection_name: str,
            skip: int = 0,
            limit: Optional[int] = None
    ) -> List[UploadMetadataDTO]:
        if skip < 0:
            raise ValueError('Skip cannot be negative.')
        if limit is not None and limit < 0:
            raise ValueError(
                'Limit cannot be negative.'
            )

        try:
            collection = await self._get_collection(collection_name)
            cursor = collection.find(query).skip(skip)
            if limit is not None:
                cursor = cursor.limit(limit)

            results = []
            async for doc in cursor:
                try:
                    results.append(UploadMetadataDTO.model_validate(doc))
                except Exception as val_err:
                    logger.warning(
                        "DB data validation failed during find for doc ID %s: %s",
                        doc.get('_id', '[N/A]'),
                        val_err
                    )
            logger.info(
                "MongoDB find in '%s' returned %d results (skip=%d, limit=%s)",
                collection_name,
                len(results),
                skip,
                limit or 'None'
            )
            return results
        except (
                OperationFailure,
                ConnectionFailure
        ) as err:
            logger.error(
                "MongoDB find error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"MongoDB find failed: {getattr(err, 'details', str(err))}",
                original_exception=err
            ) from err
        except Exception as err:
            logger.error(
                "Unexpected find error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"Unexpected error finding metadata: {err}",
                original_exception=err
            ) from err

    async def update_metadata(
            self,
            metadata_id: Union[str, UUID],
            update_data: UpdateMetadataDTO,
            collection_name: str
    ) -> Optional[UploadMetadataDTO]:
        try:
            search_id = UUID(str(metadata_id))
        except ValueError:
            logger.warning(
                "Invalid UUID format provided for update_metadata: %s",
                metadata_id
            )
            return None

        update_payload_raw = update_data.model_dump(
            exclude_unset=True
        )
        if not update_payload_raw:
            logger.warning(
                "Update called for ID %s with no fields to update.",
                search_id
            )
            return await self.get_metadata_by_id(
                search_id,
                collection_name
            )

        update_payload = {
            '$set': {**update_payload_raw,
                     'updated_at': datetime.now(timezone.utc)}
        }

        try:
            collection = await self._get_collection(collection_name)
            logger.debug(
                "MongoDB updating doc ID %s in '%s'",
                search_id,
                collection_name
            )
            updated_doc = await collection.find_one_and_update(
                {'_id': search_id},
                update_payload,
                return_document=ReturnDocument.AFTER
            )

            if updated_doc:
                logger.info(
                    "MongoDB updated doc ID %s in '%s'",
                    search_id,
                    collection_name
                )
                try:
                    return UploadMetadataDTO.model_validate(updated_doc)
                except Exception as val_err:
                    logger.error(
                        "DB data validation failed after update for ID %s: %s",
                        search_id,
                        val_err,
                        exc_info=True
                    )
                    raise MetadataError(
                        f"Failed to validate updated metadata from DB for ID {search_id}",
                        original_exception=val_err
                    )
            else:
                logger.warning(
                    "MongoDB doc ID %s not found for update in '%s'",
                    search_id,
                    collection_name
                )
                return None
        except (
                OperationFailure,
                ConnectionFailure
        ) as err:
            logger.error(
                "MongoDB update error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"MongoDB update failed: {getattr(err, 'details', str(err))}",
                original_exception=err
            ) from err
        except Exception as err:
            logger.error(
                "Unexpected update error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"Unexpected error updating metadata: {err}",
                original_exception=err
            ) from err

    async def delete_metadata(
            self,
            metadata_id: Union[str, UUID],
            collection_name: str
    ) -> bool:
        try:
            search_id = UUID(str(metadata_id))
        except ValueError:
            logger.warning(
                "Invalid UUID format provided for delete_metadata: %s",
                metadata_id
            )
            return False

        try:
            collection = await self._get_collection(collection_name)
            logger.debug(
                "MongoDB deleting doc ID %s from '%s'",
                search_id,
                collection_name
            )
            result: DeleteResult = await collection.delete_one(
                {"_id": search_id}
            )

            was_deleted = result.deleted_count > 0
            if was_deleted:
                logger.info(
                    "MongoDB deleted doc ID %s from '%s'",
                    search_id,
                    collection_name
                )
            else:
                logger.warning(
                    "MongoDB doc ID %s not found for deletion in '%s'",
                    search_id,
                    collection_name
                )
            return was_deleted
        except (
                OperationFailure,
                ConnectionFailure
        ) as err:
            logger.error(
                "MongoDB delete error in '%s': %s",
                collection_name,
                err,
                exc_info=True
            )
            raise MetadataError(
                f"MongoDB delete failed: {getattr(err, 'details', str(err))}",
                original_exception=err
            ) from err
        except Exception as err:
            logger.error("Unexpected delete error in '%s': %s",
                         collection_name,
                         err,
                         exc_info=True
                         )
            raise MetadataError(
                f"Unexpected error deleting metadata: {err}",
                original_exception=err
            ) from err
