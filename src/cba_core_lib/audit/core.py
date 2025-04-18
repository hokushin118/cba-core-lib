"""
Audit logger.

This module defines audit logger.
"""
import logging
from typing import Optional, Any

from kafka.errors import KafkaError
from kafka.producer.future import RecordMetadata

from cba_core_lib.audit.configs import AuditConfig
from cba_core_lib.audit.schemas import AuditEvent
from cba_core_lib.kafka.producer import KafkaProducerManager

logger = logging.getLogger(__name__)

ANONYMOUS_USER = 'Anonymous'


######################################################################
# AUDIT LOGGER
######################################################################
class AuditLogger:
    """Core, framework-independent audit logger using AuditEvent.

    The AuditLogger class is responsible for logging audit events to a
    Kafka topic. It encapsulates the logic for sending structured AuditEvent
    messages to Kafka, using a provided KafkaProducerManager.  It handles
    serialization of the AuditEvent, error handling, and asynchronous
    message sending.
    """

    def __init__(
            self,
            config: AuditConfig,
            producer_manager: KafkaProducerManager
    ) -> None:
        """Initializes the AuditLogger.

        Args:
            config:  Configuration settings for audit logging, including the
                Kafka topic.
            producer_manager:  Manages the Kafka producer instance.
        Raises:
            TypeError: If config is not an instance of AuditConfig or
                producer_manager is not a KafkaProducerManager.
        """
        self.config = config
        self.producer_manager = producer_manager
        logger.info(
            "Core AuditLogger initialized for topic %s",
            self.config.audit_topic
        )

    def _on_send_success(
            self,
            record_metadata: RecordMetadata,
            event: AuditEvent,
            kafka_key: Optional[Any]
    ) -> None:
        """Internal callback executed upon successful Kafka message delivery.
        Logs details at DEBUG level.

        Args:
            record_metadata: Metadata about the successfully sent message
                (topic, partition, offset).
            event: The AuditEvent being sent.
            kafka_key: The key used when sending the message.
        """
        key_repr = kafka_key
        # Safely decode key for logging if it's bytes
        if isinstance(kafka_key, bytes):
            try:
                key_repr = kafka_key.decode(
                    'utf-8',
                    errors='replace'
                )
            except Exception:  # pylint: disable=W0703
                key_repr = str(kafka_key)

        logger.debug(
            "Audit message sent successfully "
            "correlation_id: %s. Topic: %s, Key: %s, Partition: %s, Offset: %s",
            event.correlation_id,
            record_metadata.topic,
            key_repr,
            record_metadata.partition,
            record_metadata.offset,
        )

    def _on_send_error(
            self,
            kafka_exception: Exception,
            event: AuditEvent,
            kafka_key: Optional[Any]
    ) -> None:
        """Internal callback for failed Kafka message delivery
        (asynchronous errors). Logs details at ERROR level.

        Args:
            kafka_exception: The exception that occurred during the
            send operation.
            event: The AuditEvent being sent.
            kafka_key: The key used when sending the message.
        """
        key_repr = kafka_key
        if isinstance(kafka_key, bytes):
            try:
                key_repr = kafka_key.decode(
                    'utf-8',
                    errors='replace'
                )
            except Exception:  # pylint: disable=W0703
                key_repr = str(kafka_key)

        logger.error(
            "Error sending audit message asynchronously to Kafka, "
            "correlation_id: %s, key: %s: %s",
            event.correlation_id,
            key_repr,
            kafka_exception,
            exc_info=isinstance(
                kafka_exception,
                KafkaError
            )
        )

    def log_event(
            self,
            event: AuditEvent,
            kafka_key: Optional[Any] = None
    ) -> None:
        """Logs an audit event to Kafka.

        The AuditEvent is serialized and sent to the configured Kafka topic.
        The sending is done asynchronously.

        Args:
            event: The AuditEvent to log.
            kafka_key:  Optional key for the Kafka message (for partitioning).
        """
        # Create a Kafka producer on-demand, or return the cached producer
        producer = self.producer_manager.get_producer()
        if not producer:
            logger.error(
                "Audit log failed: Kafka producer not available "
                "(correlation_id: %s)",
                event.correlation_id
            )
            return

        try:
            audit_data = event.model_dump_json()
            logger.debug("Audit log data: %s", audit_data)

            # Send audit log to Kafka asynchronously with callbacks
            producer.send(
                self.config.audit_topic,
                key=kafka_key,
                value=audit_data,
            ).add_callback(
                lambda metadata: self._on_send_success(
                    metadata,
                    event=event,
                    kafka_key=kafka_key
                )
            ).add_errback(
                lambda exc: self._on_send_error(
                    exc,
                    event=event,
                    kafka_key=kafka_key
                )
            )

            logger.info(
                "Audit event %s queued for Kafka, correlation_id: %s",
                audit_data,
                event.correlation_id
            )

        except KafkaError as err:
            self._on_send_error(err, event=event, kafka_key=kafka_key)

        except Exception as err:  # pylint: disable=W0703
            logger.error(
                "Unexpected synchronous error calling producer."
                "send for audit event correlation_id: %s: %s",
                event.correlation_id,
                err
            )
            self._on_send_error(err, event=event, kafka_key=kafka_key)
