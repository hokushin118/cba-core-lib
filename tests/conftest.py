"""
Pytest configuration and fixtures.
"""
import logging
from unittest.mock import MagicMock

from kafka import KafkaProducer
from pytest import fixture

from cba_core_lib.audit.configs import AuditConfig
from cba_core_lib.audit.core import AuditLogger
from cba_core_lib.kafka.configs import (
    KafkaProducerConfig,
    KafkaConsumerConfig,
    AutoOffsetReset,
    SecurityProtocol

)
from cba_core_lib.kafka.producer import KafkaProducerManager

TEST_KAFKA_BROKERS = 'kafka1:9092,kafka2:9092'
TEST_TOPIC = 'audit-topic'
TEST_SERVICE_NAME = 'test-service'
TEST_USER_ID = '4243d65c-4feb-47a5-8571-89e3f2615bdf'


######################################################################
#  HELPER FUNCTIONS
######################################################################
def get_handler_by_stream(logger: logging.Logger, stream):
    """Finds a StreamHandler associated with a specific stream."""
    for handler in logger.handlers:
        if isinstance(
                handler,
                logging.StreamHandler
        ) and handler.stream == stream:
            return handler
    return None


######################################################################
#  FIXTURES
######################################################################
@fixture
def test_logger(request):
    """Provides a clean logger instance for each test.

    This fixture creates a unique logger for each test function.
    It ensures that each test operates with a fresh logger instance,
    preventing interference between tests due to shared logger state.
    The logger is configured to level DEBUG and its propagation to
    the root logger is disabled by default, giving each test fine-grained
    control over its logging output.

    Yields:
        logging.Logger: A configured logger instance unique to the test.
    """
    # Use the test name to ensure a unique logger name
    logger_name = f"test_logger_{request.node.name}"
    logger = logging.getLogger(logger_name)
    # Reset logger settings before each test
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)  # Start with a permissive level
    logger.propagate = False  # Avoid interference from root logger handlers
    yield logger


@fixture
def kafka_consumer_config():
    """Sets up a KafkaConsumerConfig instance for testing."""
    return KafkaConsumerConfig(
        bootstrap_servers=TEST_KAFKA_BROKERS,
        topic='test-topic',
        key_format='str',
        message_format='json',
        auto_offset_reset=AutoOffsetReset.LATEST,
        security_protocol=SecurityProtocol.PLAINTEXT,
        ssl_ca_location=None,
        ssl_certificate_location=None,
        ssl_key_location=None,
        sasl_mechanism=None,
        sasl_username=None,
        sasl_password=None,
        group_id='test-group',
        client_id='test-app-consumer-1',
        commit_retry_attempts=5,
        commit_retry_delay_seconds=2.0,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_records=500,
        max_poll_interval_ms=300000,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        retry_attempts=5,
        retry_delay_ms=1000,
        consumer_id='test-consumer',
        service_name='test-service',
    )


@fixture
def mock_deserializer():
    """Provides a mock deserializer function for testing.

    This fixture creates a `MagicMock` instance that simulates a
    deserializer function. By default, it returns the string
    'deserialized_value' when called. Tests can use this mock to
    verify how deserialization results are handled without needing
    actual deserialization logic.

    Returns:
        MagicMock: A mock object simulating a deserializer function.
    """
    return MagicMock(
        return_value='deserialized_value'
    )


@fixture
def mock_message_handler():
    """Provides a mock message handler callable for testing.

    This fixture creates a `MagicMock` instance that simulates a
    message handler function. Tests can use this mock to observe
    how consumed messages are passed to the handler and to assert
    on the number of times the handler is called and with what
    arguments.

    Returns:
        MagicMock: A mock object simulating a message handler function.
    """
    return MagicMock()


@fixture
def kafka_producer_config():
    """Sets up a KafkaProducerConfig instance for testing."""
    return KafkaProducerConfig(
        bootstrap_servers=TEST_KAFKA_BROKERS,
        retries=5,
        acks="all",
        linger_ms=100,
        batch_size=1024,
        health_check_interval=60,
        compression_type="gzip",
        client_id='test-app-producer-1',
    )


@fixture
def mock_serializer():
    """Provides a mock serializer function for testing.

    This fixture creates a `MagicMock` instance that simulates a
    serializer function. Tests can use this mock to
    verify how serialization results are handled without needing
    actual serialization logic.

    Returns:
        MagicMock: A mock object simulating a serializer function.
    """
    return MagicMock()


@fixture
def mock_kafka_producer():
    """Fixture to create a mock KafkaProducer instance.

    This fixture creates a MagicMock object that is configured to
    mimic the interface of the `kafka.KafkaProducer` class. This allows
    tests to interact with a simulated Kafka producer without needing
    a real Kafka broker connection. Methods of the mock producer can
    be asserted against (e.g., `send()`, `flush()`, `close()`) to verify
    the behavior of the code that uses a Kafka producer.

    Returns:
        MagicMock: A mock object behaving like a `kafka.KafkaProducer`.
    """
    mock = MagicMock(spec=KafkaProducer)
    return mock


@fixture
def audit_config():
    """Sets up an AuditConfig instance for testing."""
    return AuditConfig(
        audit_topic=TEST_TOPIC,
        event_source=TEST_SERVICE_NAME,
    )


@fixture
def audit_logger():
    """Sets up an AuditLogger instance for testing."""
    config = AuditConfig(
        audit_topic=TEST_TOPIC,
        event_source=TEST_SERVICE_NAME,
        user_identifier_func=lambda: TEST_USER_ID
    )
    producer_manager = KafkaProducerManager(kafka_producer_config)
    return AuditLogger(config, producer_manager)
