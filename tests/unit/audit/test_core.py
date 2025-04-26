"""
Audit Core Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from kafka.errors import KafkaError

from cba_core_lib.audit import AuditLogger
from cba_core_lib.audit.schemas import AuditEvent
from cba_core_lib.kafka import KafkaProducerManager


######################################################################
#  AUDIT CORE UNIT TEST CASES
######################################################################
class TestAuditLogger:
    """The AuditLogger Class Tests."""

    @pytest.fixture
    def mock_producer_manager(
            self,
            kafka_producer_config
    ):
        """Provides a mock KafkaProducerManager instance."""
        producer_manager = KafkaProducerManager(kafka_producer_config)
        producer_manager.get_producer = MagicMock()
        return producer_manager

    @pytest.fixture
    def mock_producer(self):
        """Provides a mock Kafka producer."""
        producer = MagicMock()
        producer.send = MagicMock()
        return producer

    @pytest.fixture
    def mock_event(self):
        """Provides a sample AuditEvent instance."""
        now = datetime.now()
        return AuditEvent(
            timestamp=now,
            correlation_id='test_correlation_id',
            event_type='test_event_type',
            status='success',
            duration_ms=100.0,
        )

    def test_initialization(
            self,
            audit_config,
            mock_producer_manager
    ):
        """It should initialize the AuditLogger with the given config
        and producer manager."""
        audit_logger = AuditLogger(
            config=audit_config,
            producer_manager=mock_producer_manager
        )
        assert audit_logger.config == audit_config
        assert audit_logger.producer_manager == mock_producer_manager

    def test_log_event_success(
            self,
            audit_config,
            mock_producer_manager,
            mock_producer,
            mock_event
    ):
        """It should log an event successfully to Kafka."""
        mock_producer_manager.get_producer.return_value = mock_producer
        audit_logger = AuditLogger(
            config=audit_config,
            producer_manager=mock_producer_manager
        )

        audit_logger.log_event(mock_event)

        mock_producer.send.assert_called_once_with(
            audit_config.audit_topic,
            key=None,
            value=mock_event.model_dump_json(),
        )

    def test_log_event_producer_not_available(
            self,
            audit_config,
            mock_producer_manager,
            mock_event
    ):
        """It should handle the case where the Kafka producer
        is not available."""
        mock_producer_manager.get_producer.return_value = None
        audit_logger = AuditLogger(
            config=audit_config,
            producer_manager=mock_producer_manager
        )

        audit_logger.log_event(mock_event)

        # Ensure send is not called
        mock_producer_manager.get_producer.send.assert_not_called()

    def test_on_send_error_callback(
            self,
            audit_config,
            mock_producer_manager,
            mock_event
    ):
        """It should call the error callback when a message fails to send."""
        audit_logger = AuditLogger(
            config=audit_config,
            producer_manager=mock_producer_manager
        )
        kafka_exception = KafkaError('Failed to send message')
        # Call private method
        audit_logger._on_send_error(
            kafka_exception,
            mock_event,
            kafka_key=b'test_key'
        )

    def test_log_event_kafka_error(
            self,
            audit_config,
            mock_producer_manager,
            mock_producer,
            mock_event
    ):
        """It should handle KafkaError during sending."""
        mock_producer_manager.get_producer.return_value = mock_producer
        mock_producer.send.side_effect = KafkaError('Kafka error')
        audit_logger = AuditLogger(
            config=audit_config,
            producer_manager=mock_producer_manager
        )

        audit_logger.log_event(mock_event)

    def test_log_event_general_exception(
            self,
            audit_config,
            mock_producer_manager,
            mock_producer,
            mock_event
    ):
        """It should handle general exceptions during sending."""
        mock_producer_manager.get_producer.return_value = mock_producer
        mock_producer.send.side_effect = Exception('General exception')
        audit_logger = AuditLogger(
            config=audit_config,
            producer_manager=mock_producer_manager
        )

        audit_logger.log_event(mock_event)
