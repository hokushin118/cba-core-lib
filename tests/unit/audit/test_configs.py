"""
Audit Configs Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
from dataclasses import FrozenInstanceError
from typing import Optional

import pytest

from cba_core_lib import AuditConfig
from tests.conftest import (
    TEST_TOPIC,
    TEST_SERVICE_NAME,
    TEST_USER_ID
)


######################################################################
#  AUDIT CONFIGS UNIT TEST CASES
######################################################################
class TestAuditConfig:
    """The AuditConfig Class Tests."""

    def test_valid_initialization(
            self,
            audit_config
    ):
        """It should initialize all attributes correctly when
        provided valid values."""
        assert audit_config.audit_topic == TEST_TOPIC
        assert audit_config.event_source == TEST_SERVICE_NAME
        assert audit_config.user_identifier_func is None

    def test_audit_topic_validation(self):
        """It should raise ValueError if audit_topic is not provided."""
        with pytest.raises(
                ValueError,
                match='audit_topic must be provided'
        ) as exc_info:
            AuditConfig(
                audit_topic=''
            )
        assert 'audit_topic must be provided' in str(exc_info.value)

        with pytest.raises(
                ValueError
        ) as exc_info:
            AuditConfig(
                audit_topic=None
            )
        assert 'audit_topic must be provided' in str(exc_info.value)

    def test_event_source_user_identifier_func_default_value(self):
        """It should set event_source and user_identifier_func to None
        if not provided."""
        config = AuditConfig(
            audit_topic=TEST_TOPIC
        )
        assert config.event_source is None
        assert config.user_identifier_func is None

    def test_audit_config_with_user_identifier_func(self):
        """It should create an AuditConfig instance with a
        user_identifier_func."""

        def dummy_user_id_func() -> Optional[str]:
            return TEST_USER_ID

        config = AuditConfig(
            audit_topic=TEST_TOPIC,
            user_identifier_func=dummy_user_id_func
        )
        assert config.audit_topic == TEST_TOPIC
        assert config.user_identifier_func

    def test_attribute_types(
            self,
            audit_config
    ):
        """It should have the correct types for each attribute."""
        assert isinstance(audit_config.audit_topic, str)
        if audit_config.event_source is not None:  # Check if it's not None
            assert isinstance(audit_config.event_source, str)

    def test_immutability(self, audit_config):
        """It should be immutable."""
        with pytest.raises(FrozenInstanceError):
            audit_config.audit_topic = 'new_topic'
        with pytest.raises(FrozenInstanceError):
            audit_config.event_source = 'new_service'
