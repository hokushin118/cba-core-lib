"""
Kafka Producer Manager Module.

This module provides the KafkaProducerManager class that encapsulates the logic
for lazy initialization, retrieval, and proper closure of a KafkaProducer
instance. It allows message sending to Kafka with configurable retries and
handles any errors that may occur during initialization, such as broker unavailability,
network issues, or misconfigurations. The module leverages Python's logging
module to capture and log the relevant information and errors.
"""
import logging
import threading
from types import TracebackType
from typing import Optional, Union, Type, Dict, Any, Callable

from kafka import KafkaProducer  # pylint: disable=E0401
from kafka.errors import (  # pylint: disable=E0401
    NoBrokersAvailable,
    NodeNotReadyError,
    KafkaTimeoutError,
    InvalidConfigurationError,
    UnsupportedVersionError,
    KafkaConnectionError,
    KafkaError,
)

from .configs import KafkaProducerConfig

logger = logging.getLogger(__name__)


class KafkaProducerManager:
    """Manages the Kafka producer instance lifecycle.

    It should encapsulate the logic for lazy initialization,
    retrieval, and proper closure of a KafkaProducer used for sending messages.
    """
    # The __consumer_offsets is an internal Kafka topic that stores the offsets
    # of consumer group. Kafka automatically creates this topic when the
    # first consumer group commits an offset
    CONSUMER_OFFSETS_TOPIC = '__consumer_offsets'

    def __init__(
            self,
            config: KafkaProducerConfig,
            key_serializer: Optional[Callable[[Any], bytes]] = None,
            value_serializer: Optional[Callable[[Any], bytes]] = None
    ):
        """Initializes the KafkaProducerManager with the provided configuration.

        Args:
            config: Kafka producer configuration dataclass.
            key_serializer: Optional function to serialize message keys.
                            Defaults to None (sends raw bytes).
            value_serializer: Optional function to serialize message values.
                              Defaults to None (sends raw bytes).
                              Example for JSON:
                              lambda v: json.dumps(v).encode('utf-8')
        """
        # Keep initial validation
        self._validate_config(config)
        self.config = config
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.health_check_interval = max(
            1,
            config.health_check_interval or 10
        )

        self._producer: Optional[KafkaProducer] = None
        self._producer_lock = threading.Lock()
        self._stop_event = threading.Event()

        # Initialization state
        # Signals successful initialization
        self._init_event = threading.Event()
        self._init_exception: Optional[
            Exception
        ] = None
        self._init_thread: Optional[threading.Thread] = None
        self._health_check_thread: Optional[threading.Thread] = None

        logger.info('KafkaProducerManager initializing...')
        self.start()

    def __enter__(self) -> "KafkaProducerManager":
        """Allows KafkaProducerManager to be used as a context manager."""
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_value: Optional[BaseException],
            traceback: Optional[TracebackType]
    ) -> None:
        """Ensures graceful shutdown when exiting the context manager.

        This method is automatically called when the `with` block associated
        with the KafkaProducerManager is exited. It triggers the `shutdown()`
        method of the manager, ensuring that any pending messages are flushed
        and resources are properly released before the context ends. This
        guarantees a clean termination of the producer's background threads
        and connections.

        Args:
            exc_type (Optional[Type[BaseException]]): The type of the exception
                that caused the context manager to exit, if any. `None` if the
                context exited without an exception.
            exc_value (Optional[BaseException]): The instance of the exception
                that caused the context manager to exit, if any. `None` if the
                context exited without an exception.
            traceback (Optional[TracebackType]): A traceback object describing
                where the exception occurred, if any. `None` if the context
                exited without an exception.
        """
        logger.debug(
            'KafkaProducerManager exiting context.'
        )
        # pylint: disable=R0801
        self.shutdown()  # Ensure graceful shutdown

    def start(self) -> None:
        """Starts background threads for initialization and health checks."""
        # pylint: disable=R1732
        if self._init_thread is not None or self._stop_event.is_set():
            logger.warning(
                'KafkaProducerManager already started or shutting down.'
            )
            return

        logger.info(
            'Starting KafkaProducerManager background tasks...'
        )
        self._stop_event.clear()
        self._init_event.clear()
        self._init_exception = None

        self._init_thread = threading.Thread(
            target=self._initialize_producer,
            daemon=True,
            name='KafkaProducerInitThread'
        )
        self._init_thread.start()

        # Health check thread starts immediately, it will wait for
        # init internally if needed
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True,
            name='KafkaProducerHealthThread'
        )
        self._health_check_thread.start()

    def wait_for_ready(
            self,
            timeout_seconds: Optional[float] = None
    ) -> bool:
        """Blocks until the producer is successfully initialized or a
        timeout occurs.

        This method waits for the internal initialization event to be set,
        indicating that the Kafka producer has been successfully connected
        and is ready to produce messages. If the timeout is reached before
        the event is set, or if an exception occurred during initialization,
        this method will return or raise accordingly.

        Args:
            timeout_seconds (Optional[float]): An optional number of seconds to
                wait for the producer to become ready. If None, the method will
                block indefinitely until ready.

        Returns:
            bool: True if the producer initialized successfully within the
                timeout, False otherwise (if a timeout occurred).

        Raises:
            Exception: The exception captured during initialization, if it
            failed.
        """
        ready = self._init_event.wait(
            timeout=timeout_seconds
        )
        if self._init_exception:
            # If init failed, raise the captured exception
            raise self._init_exception
        return ready

    def get_producer(self) -> Optional[KafkaProducer]:
        """Get (and lazily create) a KafkaProducer instance.

        This method is non-blocking. It returns the producer if it has been
        successfully initialized. If initialization is ongoing, failed, or
        not started, it returns None. Use wait_for_ready() to ensure
        initialization is complete before calling this if needed.

        Returns:
            Optional[KafkaProducer]: The initialized KafkaProducer instance if
            it's ready and initialization was successful, otherwise None.
        """
        # Check if already initialized and ready
        if self._init_event.is_set() and self._producer:
            return self._producer

        # Check if initialization failed
        if self._init_exception:
            logger.debug("Producer initialization previously failed.")
            return None

        # Initialization might be in progress or not started yet
        logger.debug("Producer not ready or initialization pending.")
        return None

    def close_producer(self) -> None:
        """Closes the Kafka producer instance if it's currently initialized.

        This method safely closes the connection to the Kafka broker for the
        underlying `KafkaProducer` object.

        Returns:
            None
        """
        producer_to_close: Optional[KafkaProducer] = None
        with self._producer_lock:
            if self._producer:
                producer_to_close = self._producer
                self._producer = None  # Mark as closed within lock
                self._init_event.clear()  # Mark as not ready

        if producer_to_close:
            logger.info('Closing Kafka producer...')
            try:
                # Flush messages and close connection
                producer_to_close.flush(timeout=10)
                producer_to_close.close(timeout=10)
                logger.info('Kafka producer closed successfully.')
            except KafkaError as err:
                logger.error(
                    "Kafka error while closing producer: %s",
                    err
                )
            except Exception as err:  # pylint: disable=W0703
                logger.exception(
                    "Unexpected error while closing Kafka producer: %s",
                    err
                )
        else:
            logger.debug(
                'Close producer called, but instance was already None.'
            )

    def shutdown(
            self,
            timeout_seconds: Optional[float] = None
    ) -> None:
        """Signals all background threads to stop and waits for
        them to exit.

        This method sets the internal stop event, signaling the health check,
        and initialization threads to terminate their loops. It then
        waits for each of these threads to finish execution, with an optional
        timeout. Finally, it ensures the underlying Kafka producer instance is
        closed.

        Args:
            timeout_seconds (Optional[float]): An optional number of seconds to
                wait for each background thread to exit. If None, the threads
                will be allowed to finish without a specific timeout.

        Returns:
            None
        """
        if self._stop_event.is_set():
            logger.info('Shutdown already in progress or completed.')
            return

        logger.info('Shutting down KafkaProducerManager...')
        # pylint: disable=R0801
        # Signal all loops to stop
        self._stop_event.set()

        thread_timeout = timeout_seconds

        # Wait for threads to finish
        if self._health_check_thread and self._health_check_thread.is_alive():
            logger.debug(
                'Waiting for health check thread to exit...'
            )
            self._health_check_thread.join(timeout=thread_timeout)
            if self._health_check_thread.is_alive():
                logger.warning(
                    'Health check thread did not exit within timeout.'
                )

        # pylint: disable=R1732
        if self._init_thread and self._init_thread.is_alive():
            logger.debug(
                'Waiting for init thread to exit...'
            )
            # Init thread might be blocked connecting, give it reasonable time
            self._init_thread.join(
                timeout=max(2.0, thread_timeout or 2.0)
            )
            if self._init_thread.is_alive():
                logger.warning(
                    'Init thread did not exit within timeout.'
                )

        # Ensure producer is closed *after* threads are stopped/timed out
        self.close_producer()

        logger.info(
            'KafkaProducerManager shutdown complete.'
        )

    def is_producer_healthy(self) -> bool:
        """Check if the Kafka producer is healthy.

        Uses partitions_for on a known topic as a basic connectivity check.

        Returns:
            bool: True if the producer is initialized and able to
            communicate with the Kafka cluster; False otherwise.
        """
        # Use the internal getter which requires the lock implicitly
        producer = self.get_producer()
        if not producer:
            logger.debug(
                'Health check: Producer not initialized or ready.'
            )
            return False

        try:
            # Check connectivity. Note: Requires appropriate
            # ACLs for the producer.
            producer.partitions_for(self.CONSUMER_OFFSETS_TOPIC)
            return True
        except (
                KafkaConnectionError,
                KafkaTimeoutError,
                NodeNotReadyError
        ) as err:
            logger.warning(
                "Kafka producer health check failed (Connection/Timeout): %s",
                err
            )
            return False
        except KafkaError as err:
            logger.warning(
                "Kafka producer health check failed (KafkaError): %s",
                err
            )
            return False
        except Exception as err:  # pylint: disable=W0703
            logger.exception(
                "Unexpected error during producer health check: %s",
                err
            )
            return False

    def _initialize_producer(self) -> None:
        """Initializes the Kafka producer instance in a background thread.

        Runs in a background thread. Sets _init_event on success or
        _init_exception on failure.
        """
        with self._producer_lock:
            # Check if already initialized or shutdown requested
            if self._producer is not None or self._stop_event.is_set():
                if self._producer:
                    # Ensure event is set if already init
                    self._init_event.set()
                logger.debug(
                    'Initialization skipped: Producer exists or '
                    'shutdown requested.'
                )
                return

            logger.info(
                'Attempting to initialize Kafka producer...'
            )
            self._init_exception = None  # Clear any previous init error
            self._init_event.clear()  # Explicitly clear before attempt

            try:
                # Build kwargs dynamically based on config and provided
                # serializers
                producer_kwargs = self._build_producer_kwargs()

                new_producer = KafkaProducer(**producer_kwargs)

                # Assign only if successful
                self._producer = new_producer
                self._init_exception = None
                self._init_event.set()  # Signal successful initialization
                logger.info(
                    "Kafka producer initialized successfully. Config: %s",
                    producer_kwargs
                )
            # pylint: disable=R0801
            except (
                    NoBrokersAvailable,
                    NodeNotReadyError,
                    KafkaTimeoutError,
                    InvalidConfigurationError,
                    UnsupportedVersionError,
                    KafkaError
            ) as err:
                logger.error(
                    "Failed to initialize Kafka producer: %s",
                    err
                )
                self._producer = None
                self._init_exception = err
                self._init_event.clear()  # Ensure event is not set
            except Exception as err:  # pylint: disable=W0703
                logger.exception(
                    "Unexpected error during producer initialization: %s",
                    err
                )
                self._producer = None
                self._init_exception = err
                self._init_event.clear()

    def _build_producer_kwargs(self) -> Dict[str, Any]:  # noqa: C901
        """Builds the keyword arguments dictionary for KafkaProducer.

        This method constructs a dictionary of keyword arguments that are passed
        to the `KafkaProducer` constructor from the `kafka-python` library. It
        retrieves configuration parameters from the `self.config` attribute of
        the `KafkaProducerManager` and includes essential settings such as
        bootstrap servers, key and value serializers, and acknowledgement level (`acks`).

        It also conditionally adds optional Kafka producer configurations if they
        are defined in the `self.config`. These optional parameters include:

        - `retries`: Number of retries for sending messages.
        - `linger_ms`: Delay in milliseconds to wait for batch accumulation.
        - `batch_size`: Maximum size of message batches in bytes.
        - `compression_type`: Type of compression to use ('gzip', 'snappy', 'lz4', None).
        - Security-related parameters (`security_protocol`, `ssl_cafile`,
          `ssl_certfile`, `ssl_keyfile`, `ssl_password`) if SSL/TLS is configured.
        - SASL-related parameters (`sasl_mechanism`, `sasl_plain_username`,
          `sasl_plain_password`) if SASL authentication is configured.
        - `client_id`: An optional identifier for the producer instance.

        Finally, the method removes any key-value pairs from the dictionary
        where the value is `None`. This is because the `KafkaProducer`
        constructor in `kafka-python` often prefers that optional parameters
        are absent from the keyword arguments rather than being explicitly set
        to `None`.

        Returns:
            Dict[str, Any]: A dictionary containing the keyword arguments to be
            used when creating the `KafkaProducer` instance.
        """
        kwargs = {
            'bootstrap_servers': self.config.bootstrap_servers,
            'key_serializer': self.key_serializer,
            'value_serializer': self.value_serializer,
            'acks': self.config.acks,
            # Optional parameters from config
            'retries': self.config.retries,
            'linger_ms': self.config.linger_ms,
            'batch_size': self.config.batch_size,
            'compression_type': self.config.compression_type,
            # Add security parameters if present in config
            'security_protocol': getattr(
                self.config, 'security_protocol',
                'PLAINTEXT'
            ),
            # SSL Args
            'ssl_cafile': getattr(
                self.config,
                'ssl_ca_location',
                None
            ),
            'ssl_certfile': getattr(
                self.config,
                'ssl_certificate_location',
                None
            ),
            'ssl_keyfile': getattr(self.config, 'ssl_key_location', None),
            'ssl_password': getattr(self.config, 'ssl_password', None),
            # SASL Args
            'sasl_mechanism': getattr(self.config, 'sasl_mechanism', None),
            'sasl_plain_username': getattr(
                self.config,
                'sasl_plain_username',
                None
            ),
            'sasl_plain_password': getattr(
                self.config,
                'sasl_plain_password',
                None
            ),
            'client_id': getattr(self.config, 'client_id', None),
        }
        # Remove keys with None values as KafkaProducer prefers them absent
        return {k: v for k, v in kwargs.items() if v is not None}

    def _reinitialize_producer(self) -> None:
        """Reinitializes the Kafka producer after a health check failure."""
        # Avoid concurrent reinitialization attempts
        # pylint: disable=R1732
        if not self._producer_lock.acquire(
                blocking=False
        ):
            logger.debug(
                'Reinitialization already in progress, skipping.'
            )
            return

        try:
            # Check if already re-initialized by another thread
            # or shutdown requested
            if self._init_event.is_set() or self._stop_event.is_set():
                logger.debug(
                    'Skipping reinitialization, already healthy or stopping.'
                )
                return

            logger.warning(
                'Reinitializing Kafka producer due to health check failure...'
            )
            # Close existing broken producer (if any state remains) -
            # lock is already held
            if self._producer:
                try:
                    self._producer.close(timeout=1)  # Quick close attempt
                except Exception:  # pylint: disable=W0703
                    logger.debug(
                        "Exception during close in reinit, ignoring.",
                        exc_info=False
                    )
                finally:
                    self._producer = None
                    self._init_event.clear()

            # Attempt re-initialization directly (no new thread needed here)
            # This call is already protected by the acquired lock
            self._initialize_producer()

            if self._init_event.is_set():
                logger.info(
                    'Kafka producer reinitialized successfully.'
                )
            else:
                logger.error(
                    'Failed to reinitialize Kafka producer. Error: %s',
                    self._init_exception
                )
        finally:
            self._producer_lock.release()

    def _health_check_loop(self) -> None:
        """Periodically checks the health of the Kafka producer and triggers
        re-initialization if it's deemed unhealthy.
        """
        logger.debug('Producer health check loop started.')

        # Wait a bit initially for the first init attempt to likely complete
        if not self._stop_event.wait(self.health_check_interval / 2):

            while not self._stop_event.wait(self.health_check_interval):
                try:
                    if not self.is_producer_healthy():
                        self._reinitialize_producer()
                    # else:
                    #     logger.debug('Producer health check passed.')
                except Exception as err:  # pylint: disable=W0703
                    logger.exception(
                        "Unexpected error in producer health check loop: %s",
                        err
                    )
                    # Avoid tight loop on unexpected error
                    if self._stop_event.wait(self.health_check_interval):
                        break  # Exit if stop requested

        logger.info('Producer health check loop stopped.')

    @staticmethod
    def _validate_config(config: KafkaProducerConfig) -> None:
        """Validates the essential configuration parameters of a
        KafkaProducerConfig object.

        This method checks for the presence of required configurations such as
        'bootstrap_servers', and 'acks'. If any of these are missing, it raises
        a ValueError with a descriptive message.

        Args:
            config (KafkaProducerConfig): The configuration object to validate.

        Raises:
            ValueError: If any of the required configurations are None.
        """
        if config.bootstrap_servers is None:
            raise ValueError('bootstrap_servers must be provided.')
        if config.acks is None:
            raise ValueError('acks must be provided.')

    @staticmethod
    def _validate_non_negative(input_val: int) -> int:
        """Validates that the 'input_val' is a non-negative integer.

        It should ensure that the provided parameter is an integer that is zero or positive.

        Args:
            input_val (int): The value to validate.

        Returns:
            int: The validated, non-negative integer.

        Raises:
            ValueError: If the provided value is not an integer or is negative.
        """
        if not isinstance(input_val, int) or input_val < 0:
            raise ValueError(
                f"Invalid value: {input_val}. Value must be a "
                f"non-negative integer."
            )
        return input_val

    @staticmethod
    def _validate_acks(acks: Union[int, str]) -> Union[int, str]:
        """Validates the 'acks' parameter.

        It should ensure that the provided 'acks' parameter is one of
        the allowed values: 0, 1, -1, or 'all'.

        Args:
            acks (Union[int, str]): The acknowledgment setting to be validated.

        Returns:
            Union[int, str]: The validated 'acks' parameter.

        Raises:
            ValueError: If the provided value is not among the allowed options.
        """
        allowed_acks = {0, 1, -1, 'all'}
        if acks not in allowed_acks:
            raise ValueError(
                f"Invalid acks value: {acks}. Allowed values are {allowed_acks}.",
            )
        return acks
