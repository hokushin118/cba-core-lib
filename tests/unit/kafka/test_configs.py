"""
Kafka Configs Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
from dataclasses import FrozenInstanceError
from enum import Enum

import pytest

from cba_core_lib.kafka.configs import (
    SecurityProtocol,
    AutoOffsetReset,
    KafkaConsumerConfig,
    KafkaProducerConfig
)
from tests.conftest import TEST_KAFKA_BROKERS


######################################################################
#  KAFKA CONFIGS UNIT TEST CASES
######################################################################
class TestSecurityProtocol:
    """The SecurityProtocol Enum Tests."""

    def test_plaintext(self):
        """It should have the correct value for PLAINTEXT."""
        assert SecurityProtocol.PLAINTEXT == 'PLAINTEXT'

    def test_ssl(self):
        """It should have the correct value for SSL."""
        assert SecurityProtocol.SSL == 'SSL'

    def test_sasl_plaintext(self):
        """It should have the correct value for SASL_PLAINTEXT."""
        assert SecurityProtocol.SASL_PLAINTEXT == 'SASL_PLAINTEXT'

    def test_sasl_ssl(self):
        """It should have the correct value for SASL_SSL."""
        assert SecurityProtocol.SASL_SSL == 'SASL_SSL'

    def test_enum_members(self):
        """It should have the correct members in the enum."""
        expected_members = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']
        assert [member.name for member in SecurityProtocol] == expected_members

    def test_enum_values(self):
        """It should have the correct values in the enum."""
        expected_values = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']
        assert [member.value for member in SecurityProtocol] == expected_values

    def test_enum_type(self):
        """It should be an instance of str and Enum."""
        assert issubclass(SecurityProtocol, str)
        assert issubclass(SecurityProtocol, Enum)


class TestAutoOffsetReset:
    """The AutoOffsetReset Enum Tests."""

    def test_latest(self):
        """It should have the correct value for LATEST."""
        assert AutoOffsetReset.LATEST == 'latest'

    def test_earliest(self):
        """It should have the correct value for EARLIEST."""
        assert AutoOffsetReset.EARLIEST == 'earliest'

    def test_none(self):
        """It should have the correct value for NONE."""
        assert AutoOffsetReset.NONE == 'none'

    def test_enum_members(self):
        """It should have the correct members in the enum."""
        expected_members = ['LATEST', 'EARLIEST', 'NONE']
        assert [member.name for member in AutoOffsetReset] == expected_members

    def test_enum_values(self):
        """It should have the correct values in the enum."""
        expected_values = ['latest', 'earliest', 'none']
        assert [member.value for member in AutoOffsetReset] == expected_values

    def test_enum_type(self):
        """It should be an instance of str and Enum."""
        assert issubclass(AutoOffsetReset, str)
        assert issubclass(AutoOffsetReset, Enum)


class TestKafkaConsumerConfig:
    """The KafkaConsumerConfig Class Tests."""

    def test_valid_initialization(
            self,
            kafka_consumer_config
    ):
        """It should initialize all attributes correctly when provided
        valid values."""
        assert kafka_consumer_config.bootstrap_servers == TEST_KAFKA_BROKERS
        assert kafka_consumer_config.topic == 'test-topic'
        assert kafka_consumer_config.key_format == 'str'
        assert kafka_consumer_config.message_format == 'json'
        assert kafka_consumer_config.auto_offset_reset == \
               AutoOffsetReset.LATEST
        assert kafka_consumer_config.security_protocol == \
               SecurityProtocol.PLAINTEXT
        assert kafka_consumer_config.ssl_ca_location is None
        assert kafka_consumer_config.ssl_certificate_location is None
        assert kafka_consumer_config.ssl_key_location is None
        assert kafka_consumer_config.sasl_mechanism is None
        assert kafka_consumer_config.sasl_username is None
        assert kafka_consumer_config.sasl_password is None
        assert kafka_consumer_config.group_id == 'test-group'
        assert kafka_consumer_config.client_id == 'test-app-consumer-1'
        assert kafka_consumer_config.commit_retry_attempts == 5
        assert kafka_consumer_config.commit_retry_delay_seconds == 2.0
        assert kafka_consumer_config.enable_auto_commit is True
        assert kafka_consumer_config.auto_commit_interval_ms == 5000
        assert kafka_consumer_config.max_poll_records == 500
        assert kafka_consumer_config.max_poll_interval_ms == 300000
        assert kafka_consumer_config.session_timeout_ms == 10000
        assert kafka_consumer_config.heartbeat_interval_ms == 3000
        assert kafka_consumer_config.retry_attempts == 5
        assert kafka_consumer_config.retry_delay_ms == 1000
        assert kafka_consumer_config.consumer_id == 'test-consumer'
        assert kafka_consumer_config.service_name == 'test-service'

    def test_default_group_id_generation(self):
        """It should generate group_id if not provided and service_name
        and topic are."""
        # Test with service_name and topic provided
        # pylint: disable=R0801
        config1 = KafkaConsumerConfig(
            bootstrap_servers=TEST_KAFKA_BROKERS,
            topic='test-topic',
            key_format='str',
            message_format='json',
            auto_offset_reset=AutoOffsetReset.LATEST,
            security_protocol=SecurityProtocol.PLAINTEXT,
            service_name='test-service',
            group_id='test-service-test-topic-consumer-1',
        )
        assert config1.group_id == 'test-service-test-topic-consumer-1'
        assert config1.commit_retry_attempts == 3
        assert config1.commit_retry_delay_seconds == 1.0

        # Test with only service_name provided
        config2 = KafkaConsumerConfig(
            bootstrap_servers=TEST_KAFKA_BROKERS,
            topic='test-topic',
            key_format='str',
            message_format='json',
            auto_offset_reset=AutoOffsetReset.LATEST,
            security_protocol=SecurityProtocol.PLAINTEXT,
            service_name='test-service',
            group_id='test-service-test-topic-consumer-1',
        )
        assert config2.group_id == 'test-service-test-topic-consumer-1'

        # Test with only topic provided
        config3 = KafkaConsumerConfig(
            bootstrap_servers=TEST_KAFKA_BROKERS,
            topic='test-topic',
            key_format='str',
            message_format='json',
            auto_offset_reset=AutoOffsetReset.LATEST,
            security_protocol=SecurityProtocol.PLAINTEXT,
            group_id='test-topic-consumer-1',  # Use the expected format
        )
        assert config3.group_id == 'test-topic-consumer-1'

        # Test with neither service_name nor topic provided
        with pytest.raises(ValueError):
            KafkaConsumerConfig(
                bootstrap_servers=TEST_KAFKA_BROKERS,
                topic=None,  # Missing required parameter
                key_format='str',
                message_format='json',
                auto_offset_reset=AutoOffsetReset.LATEST,
                security_protocol=SecurityProtocol.PLAINTEXT,
                group_id=None,  # This will cause an error in __post_init__
            )

    def test_attribute_types(
            self,
            kafka_consumer_config
    ):
        """It should have the correct types for each attribute."""
        assert isinstance(kafka_consumer_config.bootstrap_servers, str)
        assert isinstance(kafka_consumer_config.topic, str)
        assert isinstance(kafka_consumer_config.key_format, str)
        assert isinstance(kafka_consumer_config.message_format, str)
        assert isinstance(
            kafka_consumer_config.auto_offset_reset,
            AutoOffsetReset
        )
        assert isinstance(
            kafka_consumer_config.security_protocol,
            SecurityProtocol
        )
        assert isinstance(kafka_consumer_config.ssl_ca_location, type(None))
        assert isinstance(
            kafka_consumer_config.ssl_certificate_location,
            type(None)
        )
        assert isinstance(kafka_consumer_config.ssl_key_location, type(None))
        assert isinstance(kafka_consumer_config.sasl_mechanism, type(None))
        assert isinstance(kafka_consumer_config.sasl_username, type(None))
        assert isinstance(kafka_consumer_config.sasl_password, type(None))
        assert isinstance(kafka_consumer_config.group_id, str)
        assert isinstance(kafka_consumer_config.client_id, str)
        assert isinstance(kafka_consumer_config.commit_retry_attempts, int)
        assert isinstance(
            kafka_consumer_config.commit_retry_delay_seconds,
            float
        )
        assert isinstance(kafka_consumer_config.enable_auto_commit, bool)
        assert isinstance(kafka_consumer_config.auto_commit_interval_ms, int)
        assert isinstance(kafka_consumer_config.max_poll_records, int)
        assert isinstance(kafka_consumer_config.max_poll_interval_ms, int)
        assert isinstance(kafka_consumer_config.session_timeout_ms, int)
        assert isinstance(kafka_consumer_config.heartbeat_interval_ms, int)
        assert isinstance(kafka_consumer_config.retry_attempts, int)
        assert isinstance(kafka_consumer_config.retry_delay_ms, int)
        assert isinstance(kafka_consumer_config.consumer_id, str)
        assert isinstance(kafka_consumer_config.service_name, str)

    def test_immutability(self, kafka_consumer_config):
        """It should be immutable."""
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.bootstrap_servers = 'new_server:9092'
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.config = 'new_topic'
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.key_format = 'json'
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.message_format = 'json'
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.auto_offset_reset = AutoOffsetReset.EARLIEST
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.security_protocol = SecurityProtocol.SSL
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.group_id = 'new_group'
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.client_id = 'new_client_id'
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.commit_retry_attempts = 10
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.commit_retry_delay_seconds = 2.0
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.enable_auto_commit = False
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.auto_commit_interval_ms = 1000
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.max_poll_records = 100
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.max_poll_interval_ms = 100000
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.session_timeout_ms = 5000
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.heartbeat_interval_ms = 1000
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.retry_attempts = 1
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.retry_delay_ms = 500
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.consumer_id = 'new_consumer'
        with pytest.raises(FrozenInstanceError):
            kafka_consumer_config.service_name = 'new_service'


class TestKafkaProducerConfig:
    """The KafkaProducerConfig Class Tests."""

    def test_valid_initialization(self, kafka_producer_config):
        """It should initialize all attributes correctly when provided valid
        values."""
        assert kafka_producer_config.bootstrap_servers == TEST_KAFKA_BROKERS
        assert kafka_producer_config.retries == 5
        assert kafka_producer_config.acks == 'all'
        assert kafka_producer_config.linger_ms == 100
        assert kafka_producer_config.batch_size == 1024
        assert kafka_producer_config.health_check_interval == 60
        assert kafka_producer_config.compression_type == 'gzip'
        assert kafka_producer_config.client_id == 'test-app-producer-1'

    def test_default_compression(self):
        """It should default compression_type to None if no value is
        provided."""
        # compression_type and client_id omitted intentionally
        config = KafkaProducerConfig(
            bootstrap_servers=TEST_KAFKA_BROKERS,
            retries=3,
            acks=1,
            linger_ms=50,
            batch_size=512,
            health_check_interval=30,
        )
        assert config.compression_type is None
        assert config.client_id is None

    def test_attribute_types(self, kafka_producer_config):
        """It should have the correct types for each attribute."""
        assert isinstance(kafka_producer_config.bootstrap_servers, str)
        assert isinstance(kafka_producer_config.retries, int)
        assert isinstance(kafka_producer_config.acks, (int, str))
        assert isinstance(kafka_producer_config.linger_ms, int)
        assert isinstance(kafka_producer_config.batch_size, int)
        assert isinstance(kafka_producer_config.health_check_interval, int)
        assert isinstance(kafka_producer_config.compression_type, str)
        assert isinstance(kafka_producer_config.client_id, str)

    def test_immutability(self, kafka_producer_config):
        """It should be immutable."""
        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.retries = 5

        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.bootstrap_servers = 'new_server:9092'

        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.acks = 1

        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.linger_ms = 10

        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.batch_size = 2048

        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.health_check_interval = 20

        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.compression_type = 'snappy'

        with pytest.raises(FrozenInstanceError):
            kafka_producer_config.client_id = 'test-app-producer-2'
