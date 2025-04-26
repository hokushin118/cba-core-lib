"""
Configs.

This module defines configuration classes and enums for Kafka consumers
and producers. It provides a structured way to manage Kafka settings,
ensuring consistency and readability across the application.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Union, Optional, List

logger = logging.getLogger(__name__)


######################################################################
# KAFKA CONFIGURATION
######################################################################
class SecurityProtocol(str, Enum):
    """Enum for Kafka security protocols.

    This enum defines the various security protocols available for Kafka
    brokers. These protocols determine how data is transmitted between
    clients and brokers, including whether encryption and/or authentication
    is applied.

    Attributes:
        PLAINTEXT: Unencrypted, plain text communication.
        SSL: Communication secured using SSL/TLS encryption.
        SASL_PLAINTEXT: SASL authentication over an unencrypted connection.
        SASL_SSL: SASL authentication over an SSL/TLS encrypted connection.
    """
    PLAINTEXT = 'PLAINTEXT'
    SSL = 'SSL'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'


class AutoOffsetReset(str, Enum):
    """Enum for Kafka consumer auto offset reset policies.

    This enum defines the policies for resetting the consumer offset
    automatically when there is no initial offset or if the current offset
    does not exist on the server.

    Attributes:
        LATEST: Automatically reset the offset to the latest offset
        (i.e., only new messages). EARLIEST: Automatically reset the offset
        to the earliest offset available. NONE: Do not automatically reset
        the offset; instead, an error will be raised.
    """
    LATEST = 'latest'
    EARLIEST = 'earliest'
    NONE = 'none'


@dataclass(frozen=True)
class KafkaConsumerConfig:
    """Encapsulates Kafka consumer configuration settings.

    This class provides a read-only container for Kafka consumer settings,
    ensuring that the configuration remains consistent throughout the
    application.

    This class is immutable.
    """

    bootstrap_servers: Union[str, List[str]]
    """A comma-separated string or list specifying the Kafka broker
    addresses."""

    topic: str
    """The Kafka topic to consume from."""

    key_format: str
    """Format of the message key (e.g., 'str', 'json')."""

    message_format: str
    """Format of the message value (e.g., 'str', 'json')."""

    auto_offset_reset: AutoOffsetReset.LATEST
    """Auto offset reset policy. Defaults to LATEST."""

    security_protocol: SecurityProtocol.PLAINTEXT
    """Security protocol for Kafka connection. Defaults to PLAINTEXT."""

    commit_retry_attempts: int = 3
    """Kafka consumer commit retry attempts."""

    commit_retry_delay_seconds: float = 1.0
    """Kafka consumer commit retry delay."""

    group_id: Optional[str] = None
    """Kafka consumer group ID."""

    client_id: Optional[str] = None
    """Kafka consumer client ID."""

    ssl_ca_location: Optional[str] = None
    """Location of SSL CA certificate."""

    ssl_certificate_location: Optional[str] = None
    """Location of SSL client certificate."""

    ssl_key_location: Optional[str] = None
    """Location of SSL client key."""

    sasl_mechanism: Optional[str] = None
    """SASL mechanism for authentication."""

    sasl_username: Optional[str] = None
    """SASL username for authentication."""

    sasl_password: Optional[str] = None
    """SASL password for authentication."""

    enable_auto_commit: Optional[bool] = None
    """Enable auto commit of offsets."""

    auto_commit_interval_ms: Optional[int] = None
    """Auto commit interval in milliseconds."""

    max_poll_records: Optional[int] = None
    """Maximum number of records to poll in a single request."""

    max_poll_interval_ms: Optional[int] = None
    """Maximum interval in milliseconds between poll requests."""

    session_timeout_ms: Optional[int] = None
    """Session timeout in milliseconds."""

    heartbeat_interval_ms: Optional[int] = None
    """Heartbeat interval in milliseconds."""

    retry_attempts: Optional[int] = None
    """Number of retry attempts for consumer operations."""

    retry_delay_ms: Optional[int] = None
    """Delay in milliseconds between retry attempts."""

    consumer_id: Optional[str] = None
    """Optional consumer ID."""

    service_name: Optional[str] = None
    """Optional service name associated with the consumer."""

    def __post_init__(self) -> None:
        """Post-initialization to set derived attributes and validate
        configuration.

        Sets the service_name, and group_id attributes.

        Raises:
            ValueError: If the group_id cannot be constructed.
        """
        if self.service_name:
            object.__setattr__(self, 'service_name', self.service_name)
        else:
            logger.error(
                'SERVICE_NAME environment variable not set.'
            )

        if self.group_id is None:
            if self.service_name and self.topic:
                generated_group_id = KafkaConsumerConfig._generate_consumer_id(
                    self.service_name,
                    self.topic
                )
                object.__setattr__(self, 'group_id', generated_group_id)
                logger.info(
                    'Generated group_id: %s', generated_group_id
                )
            else:
                error_message = (
                    'group_id cannot be generated without service_name and '
                    'topic.'
                )
                logger.error(error_message)
                raise ValueError(error_message)

    @staticmethod
    def _generate_consumer_id(
            service_name: str,
            topic_name: str
    ) -> str:
        """Generate a unique consumer ID.

        This method constructs a consumer identifier by combining the service
        name, topic name, and a randomly generated UUID. The resulting
        string follows the format: '{service_name}-{topic_name}-consumer-{
        uuid}', ensuring that each consumer
        has a unique identifier.

        Args:
            service_name (str): The name of the service.
            topic_name (str): The Kafka topic name.

        Returns:
            str: A unique consumer ID.
        """
        instance_id = 1
        return f"{service_name}-{topic_name}-consumer-{instance_id}"


@dataclass(frozen=True)
class KafkaProducerConfig:
    """Encapsulates Kafka producer configuration settings.

    This class provides a read-only container for Kafka producer settings,
    ensuring that the configuration remains consistent throughout the
    application.

    This class is immutable.
    """

    bootstrap_servers: Union[str, List[str]]
    """A comma-separated string specifying the Kafka broker addresses."""

    retries: int
    """The number of retry attempts when sending messages."""

    acks: Union[int, str]
    """The acknowledgment policy to ensure message durability.
    Valid values:
        - 0: No acknowledgments.
        - 1: Acknowledgment from the leader only.
        - 'all' (-1): Acknowledgment from all in-sync replicas.
    """

    linger_ms: int
    """The number of milliseconds to buffer data before sending a batch.
    This can help improve throughput at the expense of latency.
    """

    batch_size: int
    """The size (in bytes) of the batch of messages to be sent. Larger values
    may increase throughput."""

    health_check_interval: int
    """The interval, in seconds, to perform connection health checks."""

    security_protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
    """Security protocol for Kafka connection. Defaults to PLAINTEXT."""

    client_id: Optional[str] = None
    """Kafka producer client ID."""

    compression_type: Optional[str] = None
    """The compression algorithm for the messages.

    Options include None, 'gzip', 'snappy', 'lz4', or 'zstd'. Defaults to None.
    """

    ssl_ca_location: Optional[str] = None
    """Location of SSL CA certificate."""

    ssl_certificate_location: Optional[str] = None
    """Location of SSL client certificate."""

    ssl_key_location: Optional[str] = None
    """Location of SSL client key."""

    sasl_mechanism: Optional[str] = None
    """Optional SASL mechanism for authentication (e.g., 'PLAIN', 'GSSAPI')."""

    sasl_username: Optional[str] = None
    """SASL username for authentication."""

    sasl_password: Optional[str] = None
    """SASL password for authentication."""
