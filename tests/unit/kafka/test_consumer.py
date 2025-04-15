"""
Kafka Consumer Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import time
from unittest.mock import patch

from kafka.errors import NoBrokersAvailable

from cba_core_lib import KafkaConsumerManager


def test_initialization_success(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should initialize the KafkaConsumerManager with
    provided arguments."""
    manager = KafkaConsumerManager(
        config=kafka_consumer_config,
        key_deserializer=mock_deserializer,
        value_deserializer=mock_deserializer,
        message_handler=mock_message_handler,
        health_check_interval_seconds=5
    )
    assert manager.config == kafka_consumer_config
    assert manager.key_deserializer == mock_deserializer
    assert manager.value_deserializer == mock_deserializer
    assert manager.message_handler == mock_message_handler
    assert manager.health_check_interval_seconds == 5
    assert manager._stop_event.is_set() is False
    assert manager._init_thread is not None
    assert manager._health_check_thread is not None
    manager.shutdown(timeout_seconds=0.1)


def test_context_manager_success(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should allow the KafkaConsumerManager to be used as a
    context manager."""
    with KafkaConsumerManager(
            config=kafka_consumer_config,
            key_deserializer=mock_deserializer,
            value_deserializer=mock_deserializer,
            message_handler=mock_message_handler,
            health_check_interval_seconds=1
    ) as manager:
        assert isinstance(manager, KafkaConsumerManager)
        assert manager._stop_event.is_set() is False
    assert manager._stop_event.is_set() is True


def test_start_already_started_success(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should not start new threads if the manager is already started."""
    manager = KafkaConsumerManager(
        config=kafka_consumer_config,
        key_deserializer=mock_deserializer,
        value_deserializer=mock_deserializer,
        message_handler=mock_message_handler,
        health_check_interval_seconds=1
    )
    initial_init_thread = manager._init_thread
    manager.start()
    assert manager._init_thread is initial_init_thread
    manager.shutdown(timeout_seconds=0.1)


def test_shutdown_success(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should signal background threads to stop and close the
    consumer on shutdown."""
    manager = KafkaConsumerManager(
        config=kafka_consumer_config,
        key_deserializer=mock_deserializer,
        value_deserializer=mock_deserializer,
        message_handler=mock_message_handler,
        health_check_interval_seconds=1
    )
    assert manager._stop_event.is_set() is False
    manager.shutdown(timeout_seconds=0.1)
    assert manager._stop_event.is_set() is True
    if manager._init_thread and manager._init_thread.is_alive():
        manager._init_thread.join(timeout=0.1)
    if manager._consume_thread and manager._consume_thread.is_alive():
        manager._consume_thread.join(timeout=0.1)
    if manager._health_check_thread and manager._health_check_thread.is_alive():
        manager._health_check_thread.join(timeout=0.1)
    assert manager._consumer is None


def test_wait_for_ready_timeout(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should return False if the timeout is reached
    before initialization."""
    with patch.object(
            KafkaConsumerManager,
            '_initialize_consumer'
    ) as mock_init:
        mock_init.side_effect = lambda is_reinit=False: time.sleep(2)
        manager = KafkaConsumerManager(
            config=kafka_consumer_config,
            key_deserializer=mock_deserializer,
            value_deserializer=mock_deserializer,
            message_handler=mock_message_handler,
            health_check_interval_seconds=0.1
        )
        assert manager.wait_for_ready(timeout_seconds=0.1) is False
        manager.shutdown(timeout_seconds=0.1)


def test_get_consumer_not_ready(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should return None if the consumer has not been initialized yet."""
    manager = KafkaConsumerManager(
        config=kafka_consumer_config,
        key_deserializer=mock_deserializer,
        value_deserializer=mock_deserializer,
        message_handler=mock_message_handler,
        health_check_interval_seconds=1
    )
    assert manager.get_consumer() is None
    manager.shutdown(timeout_seconds=0.1)


def test_get_consumer_initialization_failed(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should return None if initialization failed."""
    manager = KafkaConsumerManager(
        config=kafka_consumer_config,
        key_deserializer=mock_deserializer,
        value_deserializer=mock_deserializer,
        message_handler=mock_message_handler,
        health_check_interval_seconds=1
    )
    manager._init_exception = NoBrokersAvailable("Failed")
    assert manager.get_consumer() is None
    manager.shutdown(timeout_seconds=0.1)


def test_close_consumer_not_initialized(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should not attempt to close if the consumer is not initialized."""
    manager = KafkaConsumerManager(
        config=kafka_consumer_config,
        key_deserializer=mock_deserializer,
        value_deserializer=mock_deserializer,
        message_handler=mock_message_handler,
        health_check_interval_seconds=1
    )
    manager.close_consumer()
    assert manager._consumer is None
    assert not manager._init_event.is_set()
    manager.shutdown(timeout_seconds=0.1)


def test_is_consumer_healthy_not_initialized(
        kafka_consumer_config,
        mock_deserializer,
        mock_message_handler
):
    """It should return False if the consumer is not initialized."""
    manager = KafkaConsumerManager(
        config=kafka_consumer_config,
        key_deserializer=mock_deserializer,
        value_deserializer=mock_deserializer,
        message_handler=mock_message_handler,
        health_check_interval_seconds=1
    )
    assert manager.is_consumer_healthy() is False
