"""
Enums Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import pytest

from cba_core_lib.utils.enums import (
    UserRole,
    Status,
    HTTPSchema,
    StorageProviderType, MetadataProviderType
)


######################################################################
#  ENUMS UNIT TEST CASES
######################################################################
class TestUserRoleEnum:
    """UserRole Enum Tests."""

    def test_user_role_values(self):
        """It should have the correct string values for each enum member."""
        assert UserRole.USER.value == 'ROLE_USER'
        assert UserRole.MODERATOR.value == 'ROLE_MODERATOR'
        assert UserRole.ADMIN.value == 'ROLE_ADMIN'

    def test_user_role_role_str_method(self):
        """It should verify that converting HTTPSchema members to strings
        returns their correct values."""
        assert str(UserRole.USER) == 'ROLE_USER'
        assert str(UserRole.MODERATOR) == 'ROLE_MODERATOR'
        assert str(UserRole.ADMIN) == 'ROLE_ADMIN'

    def test_user_role_enum_creation_from_value(self):
        """It should create enum members from their string values."""
        assert UserRole('ROLE_USER') == UserRole.USER
        assert UserRole('ROLE_MODERATOR') == UserRole.MODERATOR
        assert UserRole('ROLE_ADMIN') == UserRole.ADMIN

    def test_user_role_enum_comparison(self):
        """It should allow comparison of enum members."""
        assert UserRole.USER == UserRole.USER
        assert UserRole.USER != UserRole.MODERATOR
        assert UserRole.MODERATOR != UserRole.ADMIN

    def test_user_role_name_attribute(self):
        """It should return the correct name attribute of enum members."""
        assert UserRole.USER.name == 'USER'
        assert UserRole.MODERATOR.name == 'MODERATOR'
        assert UserRole.ADMIN.name == 'ADMIN'

    def test_user_role_invalid_value(self):
        """It should raise ValueError when creating an enum member with
        an invalid string."""
        with pytest.raises(ValueError):
            UserRole('INVALID_ROLE')


class TestStatusEnum:
    """Status Enum Tests."""

    def test_status_values(self):
        """It should have the correct string values for each enum member."""
        assert Status.SUCCESS.value == 'SUCCESS'
        assert Status.FAILURE.value == 'FAILURE'
        assert Status.PENDING.value == 'PENDING'
        assert Status.RUNNING.value == 'RUNNING'
        assert Status.COMPLETED.value == 'COMPLETED'

    def test_status_status_str_method(self):
        """It should verify that converting HTTPSchema members to strings
        returns their correct values."""
        assert str(Status.SUCCESS) == 'SUCCESS'
        assert str(Status.FAILURE) == 'FAILURE'
        assert str(Status.PENDING) == 'PENDING'
        assert str(Status.RUNNING) == 'RUNNING'
        assert str(Status.COMPLETED) == 'COMPLETED'

    def test_status_enum_creation_from_value(self):
        """It should create enum members from their string values."""
        assert Status('SUCCESS') == Status.SUCCESS
        assert Status('FAILURE') == Status.FAILURE
        assert Status('PENDING') == Status.PENDING
        assert Status('RUNNING') == Status.RUNNING
        assert Status('COMPLETED') == Status.COMPLETED

    def test_status_enum_comparison(self):
        """It should allow comparison of enum members."""
        assert Status.SUCCESS == Status.SUCCESS
        assert Status.SUCCESS != Status.FAILURE
        assert Status.FAILURE == Status.FAILURE
        assert Status.FAILURE != Status.PENDING
        assert Status.PENDING == Status.PENDING
        assert Status.PENDING != Status.RUNNING
        assert Status.RUNNING == Status.RUNNING
        assert Status.COMPLETED != Status.SUCCESS
        assert Status.COMPLETED == Status.COMPLETED

    def test_status_name_attribute(self):
        """It should return the correct name attribute of enum members."""
        assert Status.SUCCESS.name == 'SUCCESS'
        assert Status.FAILURE.name == 'FAILURE'
        assert Status.PENDING.name == 'PENDING'
        assert Status.RUNNING.name == 'RUNNING'
        assert Status.COMPLETED.name == 'COMPLETED'

    def test_status_invalid_value(self):
        """It should raise ValueError when creating an enum member with
        an invalid string."""
        with pytest.raises(ValueError):
            Status('INVALID_STATUS')


class TestHTTPSchema:
    """The HTTPSchema Enum Tests."""

    def test_httpschema_values(self):
        """It should verify the string values of the HTTPSchema enum members."""
        assert HTTPSchema.HTTP.value == 'http://'
        assert HTTPSchema.HTTPS.value == 'https://'

    def test_httpschema_str_method(self):
        """It should verify that converting HTTPSchema members to strings
        returns their correct values."""
        assert str(HTTPSchema.HTTP) == 'http://'
        assert str(HTTPSchema.HTTPS) == 'https://'

    def test_httpschema_enum_creation_from_value(self):
        """It should create enum members from their string values."""
        assert HTTPSchema('http://') == HTTPSchema.HTTP
        assert HTTPSchema('https://') == HTTPSchema.HTTPS

    def test_httpschema_enum_comparison(self):
        """It should allow comparison of enum members."""
        assert HTTPSchema.HTTP == HTTPSchema.HTTP
        assert HTTPSchema.HTTP != HTTPSchema.HTTPS
        assert HTTPSchema.HTTPS == HTTPSchema.HTTPS

    def test_httpschema_name_attribute(self):
        """It should return the correct name attribute of enum members."""
        assert HTTPSchema.HTTP.name == 'HTTP'
        assert HTTPSchema.HTTPS.name == 'HTTPS'

    def test_httpschema_invalid_value(self):
        """It should raise ValueError when creating an enum member with
        an invalid string."""
        with pytest.raises(ValueError):
            HTTPSchema('INVALID_SCHEMA')


class TestStorageProviderType:
    """The StorageProviderType Enum Tests."""

    def test_storageprovidertype_values(self):
        """It should verify the string values of the
        StorageProviderType enum members."""
        assert StorageProviderType.MINIO.value == 'minio'
        assert StorageProviderType.AWS.value == 'aws'
        assert StorageProviderType.AZURE.value == 'azure'

    def test_storageprovidertype_str_method(self):
        """It should verify that converting StorageProviderType
        members to strings returns their correct values."""
        assert str(StorageProviderType.MINIO) == 'minio'
        assert str(StorageProviderType.AWS) == 'aws'
        assert str(StorageProviderType.AZURE) == 'azure'

    def test_storageprovidertype_enum_creation_from_value(self):
        """It should create enum members from their string values."""
        assert StorageProviderType('minio') == StorageProviderType.MINIO
        assert StorageProviderType('aws') == StorageProviderType.AWS
        assert StorageProviderType('azure') == StorageProviderType.AZURE

    def test_storageprovidertype_enum_comparison(self):
        """It should allow comparison of enum members."""
        assert StorageProviderType.MINIO == StorageProviderType.MINIO
        assert StorageProviderType.MINIO != StorageProviderType.AWS
        assert StorageProviderType.AWS == StorageProviderType.AWS
        assert StorageProviderType.AWS != StorageProviderType.AZURE
        assert StorageProviderType.AZURE == StorageProviderType.AZURE
        assert StorageProviderType.AZURE != StorageProviderType.MINIO

    def test_storageprovidertype_name_attribute(self):
        """It should return the correct name attribute of enum members."""
        assert StorageProviderType.MINIO.name == 'MINIO'
        assert StorageProviderType.AWS.name == 'AWS'
        assert StorageProviderType.AZURE.name == 'AZURE'

    def test_storageprovidertype_invalid_value(self):
        """It should raise ValueError when creating an enum member with
        an invalid string."""
        with pytest.raises(ValueError):
            StorageProviderType('INVALID_SCHEMA')


class TestMetadataProviderType:
    """The MetadataProviderType Enum Tests."""

    def test_metadataprovidertype_values(self):
        """It should verify the string values of the MetadataProviderType
        enum members."""
        assert MetadataProviderType.MONGO.value == 'mongo'
        assert MetadataProviderType.POSTGRES.value == 'postgres'

    def test_metadataprovidertype_str_method(self):
        """It should verify that converting MetadataProviderType members to
        strings returns their correct values."""
        assert str(MetadataProviderType.MONGO) == 'mongo'
        assert str(MetadataProviderType.POSTGRES) == 'postgres'

    def test_metadataprovidertype_enum_creation_from_value(self):
        """It should create enum members from their string values."""
        assert MetadataProviderType('mongo') == MetadataProviderType.MONGO
        assert MetadataProviderType(
            'postgres'
        ) == MetadataProviderType.POSTGRES

    def test_metadataprovidertype_enum_comparison(self):
        """It should allow comparison of enum members."""
        assert MetadataProviderType.MONGO == MetadataProviderType.MONGO
        assert MetadataProviderType.MONGO != MetadataProviderType.POSTGRES
        assert MetadataProviderType.POSTGRES == MetadataProviderType.POSTGRES

    def test_metadataprovidertype_name_attribute(self):
        """It should return the correct name attribute of enum members."""
        assert MetadataProviderType.MONGO.name == 'MONGO'
        assert MetadataProviderType.POSTGRES.name == 'POSTGRES'

    def test_metadataprovidertype_invalid_value(self):
        """It should raise ValueError when creating an enum member with
        an invalid string."""
        with pytest.raises(ValueError):
            MetadataProviderType('INVALID_SCHEMA')
