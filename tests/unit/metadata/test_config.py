"""
File Metadata Configs Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=service --cov-report=term-missing --cov-branch
"""
import os

import pytest
from pydantic import ValidationError

from cba_core_lib.metadata.configs import MongoConfig
from tests.conftest import (
    TEST_MONGO_URI,
    TEST_MONGO_DB_NAME,
)


######################################################################
#  MONGODB CONFIG UNIT TEST CASES
######################################################################
class TestMongoConfig:
    """The MongoConfig Class Tests."""

    @pytest.fixture
    def mongo_config(self):
        """Fixture to create an MongoConfig instance."""
        return MongoConfig(
            uri=TEST_MONGO_URI,
            db_name=TEST_MONGO_DB_NAME,
        )

    def test_mongo_config_defaults(
            self,
            mongo_config
    ):
        """It should verify default MongoConfig attribute values."""
        assert mongo_config.uri.get_secret_value() == TEST_MONGO_URI
        assert mongo_config.db_name == TEST_MONGO_DB_NAME

    def test_mongo_config_custom_values(
            self,
            monkeypatch
    ):
        """It should verify MongoConfig attributes with custom
        environment variables."""
        monkeypatch.setenv('MONGO_URI', TEST_MONGO_URI)
        monkeypatch.setenv('MONGO_DB_NAME', TEST_MONGO_DB_NAME)

        mongo_config = MongoConfig()

        assert mongo_config.uri.get_secret_value() == TEST_MONGO_URI
        assert mongo_config.db_name == TEST_MONGO_DB_NAME

    def test_mongo_config_init_sets_attributes(
            self,
            monkeypatch,
            mongo_config
    ):
        """It should verify attribute existence after init with custom
        env vars."""
        monkeypatch.setenv('MONGO_URI', TEST_MONGO_URI)
        monkeypatch.setenv('MONGO_DB_NAME', TEST_MONGO_DB_NAME)

        assert hasattr(mongo_config, 'uri')
        assert hasattr(mongo_config, 'db_name')

    def test_mongo_config_init_correct_values(
            self,
            mongo_config
    ):
        """It should verify init sets attributes to correct env var
        values."""
        assert mongo_config.uri.get_secret_value() == os.getenv(
            'MONGO_URI',
            TEST_MONGO_URI
        )
        assert mongo_config.db_name == os.getenv(
            'MONGO_DB_NAME',
            TEST_MONGO_DB_NAME
        )

    @pytest.mark.parametrize(
        'field_name, new_value',
        [
            ('uri', 'new-url'),
            ('db_name', 'new-db_name'),
        ],
    )
    def test_mongo_config_immutability(
            self,
            mongo_config,
            field_name,
            new_value
    ):
        """It should ensure schema instance is immutable."""
        with pytest.raises(ValidationError):
            setattr(mongo_config, field_name, new_value)
