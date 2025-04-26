"""
File Storage Configs Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=service --cov-report=term-missing --cov-branch
"""
import os

import pytest
from pydantic import ValidationError

from cba_core_lib.storage.configs import MinioConfig
from cba_core_lib.utils.env_utils import get_bool_from_env
from tests.conftest import (
    TEST_ENDPOINT,
    TEST_ACCESS_KEY,
    TEST_SECRET_KEY,
    TEST_USE_SSL
)


######################################################################
#  MINIO CONFIG UNIT TEST CASES
######################################################################
class TestMinioConfig:
    """The MinioConfig Class Tests."""

    @pytest.fixture
    def minio_config(self):
        """Fixture to create an MinioConfig instance."""
        return MinioConfig(
            endpoint=TEST_ENDPOINT,
            access_key=TEST_ACCESS_KEY,
            secret_key=TEST_SECRET_KEY,
            use_ssl=TEST_USE_SSL,
        )

    def test_minio_config_defaults(
            self,
            minio_config
    ):
        """It should verify default MinioConfig attribute values."""
        assert str(minio_config.endpoint) == f"{TEST_ENDPOINT}/"
        assert minio_config.access_key.get_secret_value() == TEST_ACCESS_KEY
        assert minio_config.secret_key.get_secret_value() == TEST_SECRET_KEY
        assert minio_config.use_ssl is False

    def test_minio_config_custom_values(
            self,
            monkeypatch
    ):
        """It should verify MinioConfig attributes with custom
        environment variables."""
        monkeypatch.setenv('MINIO_ENDPOINT', TEST_ENDPOINT)
        monkeypatch.setenv('MINIO_ACCESS_KEY', TEST_ACCESS_KEY)
        monkeypatch.setenv('MINIO_SECRET_KEY', TEST_SECRET_KEY)
        monkeypatch.setenv('MINIO_USE_SSL', TEST_USE_SSL)

        minio_config = MinioConfig()

        assert str(minio_config.endpoint) == f"{TEST_ENDPOINT}/"
        assert minio_config.access_key.get_secret_value() == TEST_ACCESS_KEY
        assert minio_config.secret_key.get_secret_value() == TEST_SECRET_KEY
        assert minio_config.use_ssl is TEST_USE_SSL

    def test_minio_config_immutability(
            self,
            minio_config
    ):
        """It should ensure MinioConfig instance is immutable."""
        with pytest.raises(ValidationError):
            minio_config.endpoint = 'new-endpoint'
        with pytest.raises(ValidationError):
            minio_config.access_key = 'new-access-key'
        with pytest.raises(ValidationError):
            minio_config.secret_key = 'new-secret-key'
        with pytest.raises(ValidationError):
            minio_config.use_ssl = True

    def test_minio_config_init_sets_attributes(
            self,
            monkeypatch,
            minio_config
    ):
        """It should verify attribute existence after init with custom
        env vars."""
        monkeypatch.setenv('MINIO_ENDPOINT', TEST_ENDPOINT)
        monkeypatch.setenv('MINIO_ACCESS_KEY', TEST_ACCESS_KEY)
        monkeypatch.setenv('MINIO_SECRET_KEY', TEST_SECRET_KEY)
        monkeypatch.setenv('MINIO_USE_SSL', TEST_USE_SSL)

        assert hasattr(minio_config, 'endpoint')
        assert hasattr(minio_config, 'access_key')
        assert hasattr(minio_config, 'secret_key')
        assert hasattr(minio_config, 'use_ssl')

    def test_minio_config_init_correct_values(
            self,
            minio_config
    ):
        """It should verify init sets attributes to correct env var
        values."""
        expected_endpoint_part = os.getenv(
            'MINIO_ENDPOINT',
            TEST_ENDPOINT
        )
        assert expected_endpoint_part in str(minio_config.endpoint)
        assert minio_config.access_key.get_secret_value() == os.getenv(
            'MINIO_ACCESS_KEY',
            TEST_ACCESS_KEY
        )
        assert minio_config.secret_key.get_secret_value() == os.getenv(
            'MINIO_SECRET_KEY',
            TEST_SECRET_KEY
        )
        assert minio_config.use_ssl == get_bool_from_env(
            'MINIO_USE_SSL',
            TEST_USE_SSL
        )

    def test_minio_config_invalid_type_env_var(
            self,
            monkeypatch
    ):
        """It should raise ValidationError for invalid data types."""
        monkeypatch.setenv('MINIO_ENDPOINT', 'this-is-not-a-valid-url')

        with pytest.raises(ValidationError) as excinfo:
            MinioConfig()

        assert (
                'Input should be a valid URL' in str(excinfo.value) or
                'invalid URL format' in str(excinfo.value)
        )

    def test_minio_config_secure_true_variations(
            self,
            monkeypatch
    ):
        """It should set 'use_ssl' flag correctly for 'true'/'1'."""
        true_values = ['true', 'True', '1', 'TRUE']
        for true_val in true_values:
            monkeypatch.setenv('MINIO_USE_SSL', true_val)
            config = MinioConfig(
                endpoint=TEST_ENDPOINT,
                access_key=TEST_ACCESS_KEY,
                secret_key=TEST_SECRET_KEY,
                use_ssl=true_val,
            )
            assert config.use_ssl is True, f"Failed for MINIO_SECURE='{true_val}'"

    def test_minio_config_secure_false_variations(
            self,
            monkeypatch
    ):
        """It should set 'use_ssl' flag correctly for 'false'/'0'."""
        false_values = ['false', 'False', '0', 'FALSE']
        for false_val in false_values:
            monkeypatch.setenv('MINIO_USE_SSL', false_val)
            config = MinioConfig(
                endpoint=TEST_ENDPOINT,
                access_key=TEST_ACCESS_KEY,
                secret_key=TEST_SECRET_KEY,
                use_ssl=false_val,
            )
            assert config.use_ssl is False, f"Failed for MINIO_SECURE='{false_val}'"
