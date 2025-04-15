"""
Kafka Producer Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import threading
import time
from unittest.mock import patch

from kafka.errors import (
    NoBrokersAvailable,
    KafkaConnectionError,
    KafkaError,
)

from cba_core_lib import KafkaProducerManager


def test_initialization_success(
        kafka_producer_config,
        mock_serializer
):
    """It should initialize the KafkaProducerManager
    with provided arguments."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    assert manager.config == kafka_producer_config
    assert manager.key_serializer == mock_serializer
    assert manager.value_serializer == mock_serializer
    assert manager.health_check_interval == 60
    assert manager._producer is None
    assert isinstance(manager._stop_event, threading.Event)
    assert isinstance(manager._init_event, threading.Event)
    assert manager._init_exception is None
    assert manager._init_thread is not None
    assert manager._health_check_thread is not None
    manager.shutdown(timeout_seconds=0.1)


def test_context_manager_success(
        kafka_producer_config,
        mock_serializer
):
    """It should allow the KafkaProducerManager to be used
    as a context manager."""
    with KafkaProducerManager(
            config=kafka_producer_config,
            key_serializer=mock_serializer,
            value_serializer=mock_serializer,
    ) as manager:
        assert isinstance(manager, KafkaProducerManager)
        assert manager._stop_event.is_set() is False
    assert manager._stop_event.is_set() is True


def test_start_success(
        kafka_producer_config,
        mock_serializer
):
    """It should start the initialization and health check threads."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    assert manager._init_thread is not None
    assert manager._health_check_thread is not None
    assert manager._init_thread.is_alive()
    assert manager._health_check_thread.is_alive()
    manager.shutdown(timeout_seconds=0.1)


def test_start_already_started(
        kafka_producer_config,
        mock_serializer
):
    """It should not start new threads if the manager is already started."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    initial_init_thread = manager._init_thread
    initial_health_check_thread = manager._health_check_thread
    manager.start()
    assert manager._init_thread is initial_init_thread
    assert manager._health_check_thread is initial_health_check_thread
    manager.shutdown(timeout_seconds=0.1)


def test_wait_for_ready_timeout(
        kafka_producer_config,
        mock_serializer
):
    """It should return False if the timeout is reached
    before initialization."""
    with patch.object(
            KafkaProducerManager,
            '_initialize_producer'
    ) as mock_init:
        mock_init.side_effect = lambda: time.sleep(2)
        manager = KafkaProducerManager(
            config=kafka_producer_config,
            key_serializer=mock_serializer,
            value_serializer=mock_serializer,
        )
        assert manager.wait_for_ready(timeout_seconds=0.1) is False
        manager.shutdown(timeout_seconds=0.1)


def test_get_producer_not_ready(
        kafka_producer_config,
        mock_serializer
):
    """It should return None if the producer has not been initialized yet."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    assert manager.get_producer() is None
    manager.shutdown(timeout_seconds=0.1)


def test_get_producer_ready(
        kafka_producer_config,
        mock_serializer,
        mock_kafka_producer
):
    """It should return the KafkaProducer instance if
    initialization is successful."""
    with patch.object(
            KafkaProducerManager,
            '_build_producer_kwargs',
            return_value={}
    ):
        with patch(
                'kafka.KafkaProducer',
                return_value=mock_kafka_producer
        ):
            manager = KafkaProducerManager(
                config=kafka_producer_config,
                key_serializer=mock_serializer,
                value_serializer=mock_serializer,
            )
            manager._init_event.set()
            manager._producer = mock_kafka_producer
            assert manager.get_producer() is mock_kafka_producer
            manager.shutdown(timeout_seconds=0.1)


def test_get_producer_initialization_failed(
        kafka_producer_config,
        mock_serializer
):
    """It should return None if initialization failed."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    manager._init_exception = NoBrokersAvailable('Failed')
    assert manager.get_producer() is None
    manager.shutdown(timeout_seconds=0.1)


def test_close_producer_not_initialized(
        kafka_producer_config,
        mock_serializer
):
    """It should not attempt to close if the producer is not initialized."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    manager.close_producer()
    # Assert no exceptions and no calls to a non-existent producer
    assert manager._producer is None
    assert not manager._init_event.is_set()
    manager.shutdown(timeout_seconds=0.1)


def test_shutdown_already_shutdown(
        kafka_producer_config,
        mock_serializer
):
    """It should not perform actions if shutdown is already in progress."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    manager._stop_event.set()
    with (patch.object(
            manager,
            'close_producer'
    ) as mock_close, \
            patch.object(
                manager._init_thread,
                'join'
            ) as mock_init_join, \
            patch.object(
                manager._health_check_thread,
                'join'
            ) as mock_health_join):
        manager.shutdown(timeout_seconds=0.1)
        mock_close.assert_not_called()
        mock_init_join.assert_not_called()
        mock_health_join.assert_not_called()
    manager.shutdown(timeout_seconds=0.1)


def test_is_producer_healthy_not_initialized(
        kafka_producer_config,
        mock_serializer
):
    """It should return False if the producer is not initialized."""
    manager = KafkaProducerManager(
        config=kafka_producer_config,
        key_serializer=mock_serializer,
        value_serializer=mock_serializer,
    )
    assert manager.is_producer_healthy() is False
    manager.shutdown(timeout_seconds=0.1)


def test_is_producer_healthy_connection_error(
        kafka_producer_config,
        mock_serializer,
        mock_kafka_producer
):
    """It should return False if partitions_for raises a connection error."""
    with patch.object(
            KafkaProducerManager,
            '_build_producer_kwargs',
            return_value={}
    ):
        with patch(
                'kafka.KafkaProducer',
                return_value=mock_kafka_producer
        ):
            manager = KafkaProducerManager(
                config=kafka_producer_config,
                key_serializer=mock_serializer,
                value_serializer=mock_serializer,
            )
            manager._producer = mock_kafka_producer
            mock_kafka_producer.partitions_for.side_effect = KafkaConnectionError(
                'Connection failed'
            )
            assert manager.is_producer_healthy() is False
            manager.shutdown(timeout_seconds=0.1)


def test_is_producer_healthy_kafka_error(
        kafka_producer_config,
        mock_serializer,
        mock_kafka_producer
):
    """It should return False if partitions_for raises a generic KafkaError."""
    with patch.object(
            KafkaProducerManager,
            '_build_producer_kwargs',
            return_value={}
    ):
        with patch(
                'kafka.KafkaProducer',
                return_value=mock_kafka_producer
        ):
            manager = KafkaProducerManager(
                config=kafka_producer_config,
                key_serializer=mock_serializer,
                value_serializer=mock_serializer,
            )
            manager._producer = mock_kafka_producer
            mock_kafka_producer.partitions_for.side_effect = KafkaError(
                'Generic Kafka error'
            )
            assert manager.is_producer_healthy() is False
            manager.shutdown(timeout_seconds=0.1)


def test_is_producer_healthy_unexpected_error(
        kafka_producer_config,
        mock_serializer,
        mock_kafka_producer
):
    """It should return False if partitions_for raises an unexpected error."""
    with patch.object(
            KafkaProducerManager,
            '_build_producer_kwargs',
            return_value={}
    ):
        with patch(
                'kafka.KafkaProducer',
                return_value=mock_kafka_producer
        ):
            manager = KafkaProducerManager(
                config=kafka_producer_config,
                key_serializer=mock_serializer,
                value_serializer=mock_serializer,
            )
            manager._producer = mock_kafka_producer
            mock_kafka_producer.partitions_for.side_effect = ValueError(
                'Unexpected error'
            )
            assert manager.is_producer_healthy() is False
            manager.shutdown(timeout_seconds=0.1)


def test_initialize_producer_no_brokers(
        kafka_producer_config,
        mock_serializer
):
    """It should handle NoBrokersAvailable and not set the ready event."""
    with patch.object(
            KafkaProducerManager,
            '_build_producer_kwargs',
            return_value={}
    ):
        with patch(
                'kafka.KafkaProducer',
                side_effect=NoBrokersAvailable('No brokers')
        ):
            manager = KafkaProducerManager(
                config=kafka_producer_config,
                key_serializer=mock_serializer,
                value_serializer=mock_serializer,
            )
            manager._initialize_producer()
            assert manager._producer is None
            assert not manager._init_event.is_set()
            assert isinstance(manager._init_exception, NoBrokersAvailable)
            manager.shutdown(timeout_seconds=0.1)
