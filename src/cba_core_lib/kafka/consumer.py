"""
Kafka Consumer Manager Module.

This module provides the KafkaConsumerManager class, which simplifies
the management of Kafka consumers within applications. It handles the
lifecycle of a KafkaConsumer, including initialization, message consumption,
health monitoring, and graceful shutdown.
"""
import logging
import threading
import time
from types import TracebackType
from typing import Optional, Type, Callable, Any, Dict, List

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import (
    NoBrokersAvailable,
    NodeNotReadyError,
    KafkaTimeoutError,
    InvalidConfigurationError,
    UnsupportedVersionError,
    KafkaConnectionError,
    KafkaError,
)
from kafka.structs import TopicPartition, OffsetAndMetadata

from .configs import KafkaConsumerConfig

logger = logging.getLogger(__name__)

# Configuration for re-initialization backoff
REINIT_BASE_DELAY_SECONDS = 2
REINIT_MAX_DELAY_SECONDS = 60
# Log a more severe warning after this many consecutive fails
REINIT_MAX_ATTEMPTS_BEFORE_WARN = 5


class KafkaConsumerManager:
    """Manages the Kafka consumer instance lifecycle.

    This class initializes and maintains a KafkaConsumer instance,
    monitors its health periodically, and can reinitialize it if necessary.
    It is usable both as a context manager and as a stand-alone manager.
    """

    def __init__(
            self,
            config: KafkaConsumerConfig,
            key_deserializer: Callable[[bytes], Any],
            value_deserializer: Callable[[bytes], Any],
            message_handler: Callable[[ConsumerRecord], None],
            health_check_interval_seconds: Optional[int] = None,
    ) -> None:
        """Initializes the KafkaConsumerManager with the provided configuration
        and message handler.

        Args:
            config: Kafka consumer configuration dataclass.
            key_deserializer: Function to deserialize message keys
            (bytes -> Any).
            value_deserializer: Function to deserialize message values
            (bytes -> Any).
            message_handler: Callable invoked with each consumed message
            (ConsumerRecord).
            health_check_interval_seconds: Interval (in seconds) for health
            checks. Defaults to config.session_timeout_ms / 3 / 1000.
        """
        # Keep initial validation
        self._validate_config(config)
        self.config = config
        self.key_deserializer = key_deserializer
        self.value_deserializer = value_deserializer
        self.message_handler = message_handler

        # Determine health check interval with a safer default logic
        self.health_check_interval_seconds = (
                health_check_interval_seconds or
                self._get_health_check_interval(
                    config.session_timeout_ms
                )
        )

        self._consumer: Optional[KafkaConsumer] = None
        self._consumer_lock = threading.Lock()
        # Signals shutdown request
        self._stop_event = threading.Event()
        # Signals successful initialization
        self._init_event = threading.Event()
        # Stores exception during init
        self._init_exception: Optional[Exception] = None

        # Thread handles - initialized when started
        self._init_thread: Optional[threading.Thread] = None
        self._consume_thread: Optional[threading.Thread] = None
        self._health_check_thread: Optional[threading.Thread] = None

        # Track re-initialization attempts for backoff
        self._reinit_attempts = 0

        logger.info(
            "KafkaConsumerManager initialized for topic '%s', "
            "group '%s'. Starting...",
            self.config.topic,
            self.config.group_id
        )
        self.start()  # Start the manager automatically on init

    def __enter__(self) -> "KafkaConsumerManager":
        """Allows KafkaConsumerManager to be used as a context manager."""
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> None:
        """Ensures graceful shutdown when exiting the context manager.

        Args:
            exc_type (Optional[Type[BaseException]]): The type of the exception that caused
            the context manager to exit, or None if no exception occurred.
            exc_value (Optional[BaseException]): The exception instance that caused the context
            manager to exit, or None if no exception occurred.
            traceback (Optional[TracebackType]): The traceback object associated with the exception,
                or None if no exception occurred.

        This method is automatically called upon exiting the context and delegates
        the task of releasing any established connections by invoking the
        close_consumer() method.
        """
        logger.debug(
            'KafkaConsumerManager exiting context.'
        )
        self.shutdown()  # Ensure graceful shutdown

    def start(self) -> None:
        """Starts the background threads for initialization, consumption,
        and health checks."""
        if self._init_thread is not None or self._stop_event.is_set():
            logger.warning(
                'KafkaConsumerManager already started or shutting down.'
            )
            return

        logger.info(
            'Starting KafkaConsumerManager background tasks...'
        )
        self._stop_event.clear()
        self._init_event.clear()
        self._init_exception = None

        self._init_thread = threading.Thread(
            target=self._initialize_consumer, daemon=True,
            name='KafkaInitThread'
        )
        self._init_thread.start()

        # Health check thread starts immediately, it will wait for
        # init internally if needed
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True,
            name='KafkaHealthThread'
        )
        self._health_check_thread.start()

    def wait_for_ready(
            self,
            timeout_seconds: Optional[float] = None
    ) -> bool:
        """Blocks until the consumer is successfully initialized or a
        timeout occurs.

        This method waits for the internal initialization event to be set,
        indicating that the Kafka consumer has been successfully connected
        and is ready to consume messages. If the timeout is reached before
        the event is set, or if an exception occurred during initialization,
        this method will return or raise accordingly.

        Args:
            timeout_seconds (Optional[float]): An optional number of seconds to
                wait for the consumer to become ready. If None, the method will
                block indefinitely until ready.

        Returns:
            bool: True if the consumer initialized successfully within the
                timeout, False otherwise (if a timeout occurred).

        Raises:
            Exception: The exception captured during initialization, if it
            failed.
        """
        ready = self._init_event.wait(timeout=timeout_seconds)
        if self._init_exception:
            # If init failed, raise the captured exception
            raise self._init_exception
        return ready

    def get_consumer(self) -> Optional[KafkaConsumer]:
        """Get (and lazily create) a KafkaConsumer instance.

        This method retrieves the KafkaConsumer instance. If the consumer has
        not been initialized yet (or if the previous initialization failed),
        it returns None after logging a debug message. The consumer is
        lazily created and stored internally upon successful initialization
        by the background initialization thread. The `_init_event` is
        checked to ensure that the initialization process has completed
        successfully at least once before returning the consumer. Access to
        the `_consumer` attribute is protected by a lock for thread safety.

        Returns:
            Optional[KafkaConsumer]: The initialized KafkaConsumer instance if
            it's ready and initialization was successful, otherwise None.
        """
        with self._consumer_lock:
            # Check the _init_event as well, ensures init
            # succeeded at least once
            if self._consumer and self._init_event.is_set():
                return self._consumer
        logger.debug(
            'get_consumer called but consumer is not ready or initialized.'
        )
        return None

    def close_consumer(self) -> None:
        """Closes the Kafka consumer instance if it's currently initialized.

        This method safely closes the connection to the Kafka broker for the
        underlying `KafkaConsumer` object. It first acquires a lock to safely
        access and potentially nullify the internal `_consumer` instance and
        clear the ready event. The actual closing of the consumer is done
        outside the lock to avoid holding it during potentially blocking
        network operations. Any `KafkaError` or unexpected exceptions during
        the closing process are caught and logged.

        Returns:
            None
        """
        consumer_to_close: Optional[KafkaConsumer] = None
        with self._consumer_lock:
            if self._consumer:
                consumer_to_close = self._consumer
                self._consumer = None  # Mark as closed within lock
                self._init_event.clear()  # Mark as not ready

        if consumer_to_close:
            logger.info("Closing Kafka consumer...")
            try:
                # Close outside the lock to avoid holding it during
                # potentially blocking call
                consumer_to_close.close()
                logger.info(
                    'Kafka consumer closed successfully.'
                )
            except KafkaError as err:
                logger.error(
                    "Kafka error while closing consumer: %s",
                    err
                )
            except Exception as err:  # pylint: disable=W0703
                logger.exception(
                    "Unexpected error while closing Kafka consumer: %s",
                    err
                )
        else:
            logger.debug(
                'Close consumer called, but instance was already None.'
            )

    def shutdown(
            self,
            timeout_seconds: Optional[float] = None
    ) -> None:
        """Signals all background threads to stop and waits for
        them to exit.

        This method sets the internal stop event, signaling the health check,
        consume, and initialization threads to terminate their loops. It then
        waits for each of these threads to finish execution, with an optional
        timeout. Finally, it ensures the underlying Kafka consumer instance is
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

        logger.info('Shutting down KafkaConsumerManager...')
        # Signal all loops to stop
        self._stop_event.set()

        # Wait for threads to finish
        thread_timeout = timeout_seconds  # Timeout for each join

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

        if self._consume_thread and self._consume_thread.is_alive():
            logger.debug(
                'Waiting for consume thread to exit...'
            )
            self._consume_thread.join(timeout=thread_timeout)
            if self._consume_thread.is_alive():
                logger.warning(
                    'Consume thread did not exit within timeout.'
                )

        # Init thread might be blocked waiting for Kafka, give it a chance
        if self._init_thread and self._init_thread.is_alive():
            logger.debug(
                'Waiting for init thread to exit...'
            )
            # Init thread might be blocked connecting, give it reasonable time
            self._init_thread.join(
                timeout=max(1.0, thread_timeout or 1.0)
            )
            if self._init_thread.is_alive():
                logger.warning(
                    'Init thread did not exit within timeout.'
                )

        # Ensure consumer is closed *after* threads are stopped/timed out
        self.close_consumer()

        logger.info(
            'KafkaConsumerManager shutdown complete.'
        )

    def is_consumer_healthy(self) -> bool:
        """Check if the Kafka consumer is healthy.

        This method checks the health of the underlying Kafka consumer instance
        by attempting a lightweight operation that requires communication with
        the Kafka cluster. It uses the internal `get_consumer()` method to
        retrieve the consumer instance (which implicitly checks if it's
        initialized and ready). If a consumer instance exists, it attempts to
        fetch the list of topics. A successful retrieval indicates a healthy
        connection. Connection-related exceptions are caught and logged as
        warnings, and the method returns False in such cases. If the health
        check succeeds after previous failures, the re-initialization attempt
        counter is reset.

        Returns:
            bool: True if the consumer is initialized and able to
            communicate with the Kafka cluster; False otherwise.
        """
        # Use the internal getter which requires the lock implicitly
        consumer = self.get_consumer()  # Check if ready and initialized
        if not consumer:
            logger.debug(
                'Health check: Consumer not initialized or ready.'
            )
            return False

        try:
            # topics() is relatively lightweight.
            consumer.topics()  # This forces connection check if needed
            # Reset re-init attempts if check is successful after failures
            if self._reinit_attempts > 0:
                logger.info(
                    "Kafka consumer health check successful after "
                    "%d re-init attempt(s).",
                    self._reinit_attempts
                )
                self._reinit_attempts = 0
            return True
        except (
                KafkaConnectionError,
                KafkaTimeoutError,
                NoBrokersAvailable
        ) as err:
            logger.warning(
                "Kafka consumer health check failed (connection/timeout): %s",
                err
            )
            return False
        except KafkaError as err:
            logger.warning(
                "Kafka consumer health check failed (KafkaError): %s",
                err
            )
            return False
        except Exception as err:  # pylint: disable=W0703
            logger.exception(
                "Unexpected error during Kafka consumer health check: %s",
                err
            )
            return False

    def _initialize_consumer(
            self,
            is_reinit: bool = False
    ) -> None:
        """Initializes the Kafka consumer instance in a background thread.

        This method attempts to create and configure the `KafkaConsumer` object.
        It handles initial setup and potential re-initialization attempts with
        an exponential backoff strategy. Upon successful initialization, it
        signals the `_init_event`, clears any stored initialization
        exception, resets the re-initialization attempt counter, and starts
        the message consumption thread. If initialization fails due to known
        Kafka-related errors or unexpected exceptions, it logs the error,
        sets the `_consumer` to None, stores the exception in
        `_init_exception`, and ensures the `_init_event` is not set.

        Args:
            is_reinit (bool): A flag indicating if this is a re-initialization
                attempt. If True, a backoff delay will be applied.
                Defaults to False.
        """
        # If this is a re-init attempt, wait with backoff
        if is_reinit and self._reinit_attempts > 0:
            delay = min(
                REINIT_BASE_DELAY_SECONDS * (2 ** (self._reinit_attempts - 1)),
                REINIT_MAX_DELAY_SECONDS
            )
            logger.info(
                "Waiting %.1f seconds before re-initialization attempt %d...",
                delay,
                self._reinit_attempts
            )
            time.sleep(delay)

        with self._consumer_lock:
            # Check if already initialized or shutdown requested
            if self._consumer is not None or self._stop_event.is_set():
                logger.debug(
                    "Initialization skipped: "
                    "Consumer exists or shutdown requested."
                )
                if self._consumer:
                    # Ensure event is set if already init
                    self._init_event.set()
                return

            logger.info(
                "Attempting to initialize Kafka consumer (attempt %d)...",
                self._reinit_attempts + 1
            )
            try:
                # Build kwargs dynamically, handling optional values
                consumer_kwargs = self._build_consumer_kwargs()

                new_consumer = KafkaConsumer(
                    self.config.topic,  # Subscribe to topic(s)
                    **consumer_kwargs
                )
                # Assign only if successful
                self._consumer = new_consumer
                self._init_exception = None  # Clear any previous init error
                self._init_event.set()  # Signal successful initialization
                self._reinit_attempts = 0  # Reset attempts on success
                logger.info('Kafka consumer initialized successfully.')

                # Start consumption thread ONLY after successful initialization
                self._start_consume_thread()

            except (
                    NoBrokersAvailable,
                    NodeNotReadyError,
                    KafkaTimeoutError,
                    InvalidConfigurationError,
                    UnsupportedVersionError,
                    KafkaError
            ) as err:
                logger.error(
                    "Failed to initialize Kafka consumer: %s",
                    err
                )
                self._consumer = None
                self._init_exception = err  # Store exception
                self._init_event.clear()  # Ensure it's not set on failure
            except Exception as err:  # pylint: disable=W0703
                logger.exception(
                    "Unexpected error during consumer initialization: %s",
                    err
                )
                self._consumer = None
                self._init_exception = err  # Store exception
                self._init_event.clear()

    def _build_consumer_kwargs(self) -> Dict[str, Any]:  # noqa: C901
        """Builds the keyword arguments dictionary for KafkaConsumer.

        This method constructs a dictionary of keyword arguments that are passed
        to the `KafkaConsumer` constructor. It retrieves configuration
        parameters from the `self.config` attribute and includes essential
        settings like bootstrap servers, group ID, offset reset policy,
        auto-commit behavior, security protocol, and deserializers for keys
        and values.

        It also conditionally adds optional Kafka consumer configurations
        if they are defined in the `self.config`. These optional parameters
        include:

        - `session_timeout_ms`: The timeout used to detect server crashes.
        - `heartbeat_interval_ms`: The expected time between heartbeats to the
          consumer coordinator.
        - `max_poll_interval_ms`: The maximum delay between invocations of
        poll() when using consumer group management.
        - `max_poll_records`: The maximum number of records returned in a single
          call to poll().
        - SSL-related arguments (`ssl_cafile`, `ssl_certfile`, `ssl_keyfile`,
          `ssl_password`): If SSL/TLS is configured.
        - SASL-related arguments (`sasl_mechanism`, `sasl_plain_username`,
          `sasl_plain_password`): If SASL authentication is configured.
        - `client_id`: An optional identifier for the consumer instance.

        Finally, the method removes any key-value pairs from the dictionary
        where the value is `None`. This is because the `KafkaConsumer`
        constructor in `kafka-python` often prefers that optional parameters
        are absent from the keyword arguments rather than being explicitly set
        to `None`.

        Returns:
            Dict[str, Any]: A dictionary containing the keyword arguments to be
            used when creating the `KafkaConsumer` instance.
        """
        kwargs = {
            'bootstrap_servers': self.config.bootstrap_servers,
            'group_id': self.config.group_id,
            'auto_offset_reset': self.config.auto_offset_reset.value,
            'enable_auto_commit': self.config.enable_auto_commit,
            'security_protocol': self.config.security_protocol.value,
            'key_deserializer': self.key_deserializer,
            'value_deserializer': self.value_deserializer,
        }
        # Add optional parameters if they are configured (not None)
        if self.config.session_timeout_ms is not None:
            kwargs['session_timeout_ms'] = self.config.session_timeout_ms
        if self.config.heartbeat_interval_ms is not None:
            kwargs['heartbeat_interval_ms'] = self.config.heartbeat_interval_ms
        if self.config.max_poll_interval_ms is not None:
            kwargs['max_poll_interval_ms'] = self.config.max_poll_interval_ms
        if self.config.max_poll_records is not None:
            kwargs['max_poll_records'] = self.config.max_poll_records
        # SSL Args
        if self.config.ssl_ca_location:
            kwargs['ssl_cafile'] = self.config.ssl_ca_location
        if self.config.ssl_certificate_location:
            kwargs['ssl_certfile'] = self.config.ssl_certificate_location
        if self.config.ssl_key_location:
            kwargs['ssl_keyfile'] = self.config.ssl_key_location
        if hasattr(
                self.config,
                'ssl_password'
        ) and self.config.ssl_password:
            kwargs['ssl_password'] = self.config.ssl_password
        # SASL Args
        if self.config.sasl_mechanism:
            kwargs['sasl_mechanism'] = self.config.sasl_mechanism
            if hasattr(
                    self.config,
                    'sasl_plain_username'
            ) and self.config.sasl_plain_username:
                kwargs['sasl_plain_username'] = self.config.sasl_plain_username
            if hasattr(
                    self.config,
                    'sasl_plain_password'
            ) and self.config.sasl_plain_password:
                kwargs['sasl_plain_password'] = self.config.sasl_plain_password
        if self.config.client_id:
            kwargs['client_id'] = self.config.client_id
        # Remove keys with None values as KafkaConsumer prefers them absent
        return {k: v for k, v in kwargs.items() if v is not None}

    def _start_consume_thread(self) -> None:
        """Starts the message consumption loop in a background thread.

        This method checks if the message consumption thread
        (`self._consume_thread`) is already running. If not, it creates a
        new `threading.Thread` that executes the `_consume_loop` method.
        The thread is configured as a daemon thread, meaning it will exit
        when the main program exits. A descriptive name ('KafkaConsumeThread')
        is assigned to the thread for easier identification in debugging
        and monitoring. The newly created thread is then started.
        """
        if self._consume_thread and self._consume_thread.is_alive():
            logger.debug('Consume thread already running.')
            return

        logger.info('Starting message consumption thread...')
        self._consume_thread = threading.Thread(
            target=self._consume_loop,
            daemon=True,
            name='KafkaConsumeThread'
        )
        self._consume_thread.start()

    def _health_check_loop(self) -> None:
        """Periodically checks the health of the Kafka consumer and triggers
        re-initialization if it's deemed unhealthy.

        This method runs in a background thread and periodically wakes up
        based on the `health_check_interval_seconds`. It calls the
        `is_consumer_healthy()` method to determine the current state of the
        consumer. If the consumer is not healthy, a re-initialization attempt
        is triggered in a separate thread (`KafkaReinitThread`) to avoid
        blocking the health check loop. An exponential backoff strategy is
        employed in the `_initialize_consumer()` method for subsequent
        re-initialization attempts. Warnings and errors are logged based on
        the number of consecutive health check failures. The loop continues
        until the `_stop_event` is set, signaling a shutdown. Any unexpected
        exceptions within the health check loop are caught, logged, and a
        brief sleep is introduced to prevent a tight error loop.
        """
        logger.debug('Health check loop started.')

        # Wait a bit initially for the first init attempt to likely complete
        while not self._stop_event.wait(self.health_check_interval_seconds):
            try:
                if not self.is_consumer_healthy():
                    logger.warning(
                        'Health check failed. Attempting re-initialization.'
                    )
                    self._reinit_attempts += 1
                    if self._reinit_attempts > REINIT_MAX_ATTEMPTS_BEFORE_WARN:
                        logger.error(
                            "Kafka consumer has failed %d consecutive "
                            "health checks. Re-init attempts continue.",
                            self._reinit_attempts
                        )
                    # Trigger re-initialization in its own thread to avoid
                    # blocking health check
                    reinit_thread = threading.Thread(
                        target=self._initialize_consumer,
                        args=(True,),
                        daemon=True,
                        name='KafkaReinitThread'
                    )
                    reinit_thread.start()
                # else:
                #     logger.debug('Consumer health check passed.')
            except Exception as err:  # pylint: disable=W0703
                logger.exception(
                    "Unexpected error in consumer health check loop: %s",
                    err
                )
                # Avoid tight loop on unexpected error
                time.sleep(self.health_check_interval_seconds)

        logger.info('Health check loop stopped.')

    def _consume_loop(self) -> None:  # noqa: C901
        """The main loop for polling and processing messages.

        This method runs in a background thread and continuously polls the
        Kafka consumer for new messages. It first retrieves the current
        consumer instance using `get_consumer()`. If the consumer is not
        ready (e.g., during initialization or after a health check failure),
        it logs a warning and waits briefly before retrying.

        The method then enters a loop that continues until the `_stop_event`
        is set. Inside the loop, it performs the following steps:

        1.  **Get Consumer:** Retrieves the current Kafka consumer instance
            using `self.get_consumer()`. If no valid consumer is available,
            it logs a warning and continues to the next iteration after a
            short delay.
        2.  **Poll for Messages:** Calls `consumer.poll()` with a specified
            `timeout_ms` to retrieve available messages from the subscribed
            topic partitions. The timeout allows the loop to periodically
            check the `_stop_event`.
        3.  **Process Batches:** If the `poll()` call returns any records,
            it iterates through each `TopicPartition` and the list of
            `ConsumerRecord` objects associated with it.
        4.  **Process Messages:** For each received message:
            * It checks the `_stop_event` again before processing to ensure
                a prompt shutdown.
            * It calls the `self.message_handler()` function to process
                the individual message.
            * If `self.config.enable_auto_commit` is False, it calls
                `self._commit_message_offset()` to manually commit the
                offset of the processed message to Kafka.
        5.  **Error Handling (Message Handler):** If the `self.message_handler()`
            raises an exception, it is caught, logged with relevant message
            details (offset, partition), and the consumption loop continues.
        """
        logger.debug('Consume loop started.')
        # pylint: disable=R1702
        while not self._stop_event.is_set():
            consumer = self.get_consumer()  # Get current consumer instance
            if not consumer:
                logger.warning(
                    'Consume loop: Consumer not ready or None. Waiting...'
                )
                # Wait briefly before checking again, init/health
                # check will handle recovery
                time.sleep(1)
                continue

            try:
                # Poll for records - Lock is not strictly needed around poll if
                # consumer object reference is stable and poll is thread-safe
                # (which it is)
                records: Dict[
                    TopicPartition, List[ConsumerRecord]] = consumer.poll(
                    timeout_ms=1000
                )  # Poll with timeout

                if not records:
                    # logger.debug('No records received in poll.')
                    continue  # Go back to check stop_event and poll again

                logger.debug(
                    "Received %d batches in poll.",
                    len(records)
                )
                for partition, messages in records.items():
                    logger.debug(
                        "Processing %d messages from partition %s",
                        len(messages),
                        partition
                    )
                    for message in messages:
                        # Check stop event before processing each message
                        # for faster shutdown
                        if self._stop_event.is_set():
                            logger.info(
                                'Stop event detected during message '
                                'processing.'
                            )
                            break  # Break inner message loop

                        try:
                            # Process the message using the provided handler
                            self.message_handler(message)
                            # Manual commit if needed
                            if not self.config.enable_auto_commit:
                                self._commit_message_offset(consumer, message)
                        except Exception as err:  # pylint: disable=W0703
                            # Log error from message handler, but continue
                            # consuming potentially
                            logger.exception(
                                "Error processing message by handler: "
                                "Offset %d, Partition %s. Error: %s",
                                message.offset,
                                message.partition,
                                err
                            )

                    if self._stop_event.is_set():
                        break  # Break outer partition loop if stopped

            # Catch specific Kafka errors during poll/commit
            except KafkaError as err:
                logger.error(
                    "KafkaError during consumption loop: %s. "
                    "Consumer might need re-initialization.",
                    err
                )
                # Health check loop will likely detect this and trigger re-init
                # Add a small sleep to prevent tight loop on certain
                # persistent errors
                time.sleep(1)
            except Exception as err:  # pylint: disable=W0703
                logger.exception(
                    "Unexpected error during message consumption: %s",
                    err
                )
                # Consider if this should trigger a shutdown or just wait
                # for health check
                time.sleep(5)  # Sleep longer on unexpected errors

        logger.info("Consume loop stopped.")

    def _commit_message_offset(
            self,
            consumer: KafkaConsumer,
            message: ConsumerRecord
    ) -> None:
        """Manually commits the offset of a consumed message to Kafka.

        This method is called to explicitly commit the offset of a successfully
        processed message. It is only effective when the consumer is configured
        with `enable_auto_commit=False`. Committing offsets manually ensures
        at-least-once processing semantics. The method constructs a `
        TopicPartition` object and an `OffsetAndMetadata` object
        representing the next offset to be consumed for that partition. It
        then calls the `consumer.commit()` method. Successful commits are
        logged at the debug level. If a `KafkaError` occurs during the
        commit operation, an error is logged, and further error handling
        (such as retries or sending to a dead-letter queue) might be necessary
        at a higher level.

        Args:
            consumer (KafkaConsumer): The active KafkaConsumer instance.
            message (ConsumerRecord): The message that has been successfully processed.
        """
        topic_partition = TopicPartition(message.topic, message.partition)
        # Commit next offset to read
        offset = OffsetAndMetadata(message.offset + 1, None)
        offsets_to_commit = {topic_partition: offset}

        max_attempts = self.config.commit_retry_attempts
        retry_delay = self.config.commit_retry_delay_seconds

        # Handle commit failure (e.g., retry)
        for attempt in range(1, max_attempts + 1):
            try:
                consumer.commit(offsets_to_commit)
                logger.debug(
                    "Successfully committed offset %d for partition %s (attempt %d/%d)",
                    offset.offset,
                    topic_partition,
                    attempt,
                    max_attempts
                )
                return  # Commit successful, exit the method

            except (
                    KafkaTimeoutError,
                    KafkaConnectionError,
                    NodeNotReadyError
            ) as err:
                # These errors are often transient and worth retrying
                logger.warning(
                    "Commit failed for offset %d, partition %s "
                    "(attempt %d/%d): %s. Retrying in %ds...",
                    offset.offset,
                    topic_partition,
                    attempt,
                    max_attempts,
                    err,
                    retry_delay
                )
                # Check stop event before sleeping
                if self._stop_event.is_set():
                    logger.warning(
                        'Stop event set during commit retry, '
                        'abandoning commit.'
                    )
                    return
                # Don't sleep on the last attempt
                if attempt < max_attempts:
                    time.sleep(retry_delay)

            except KafkaError as err:
                # Includes CommitFailedError (e.g., rebalance), etc.
                # These might indicate a more persistent issue or a state
                # problem (like rebalance). Retrying might not help, but we
                # log critically and stop retrying here.
                logger.critical(
                    "Unrecoverable KafkaError during commit for offset %d, "
                    "partition %s (attempt %d/%d): %s. Aborting commit.",
                    offset.offset,
                    topic_partition,
                    attempt,
                    max_attempts,
                    err
                )
                return  # Stop trying after non-transient KafkaError

            except Exception as err:  # pylint: disable=W0703
                # Catch any other unexpected errors
                logger.exception(
                    "Unexpected error during commit for offset %d, partition "
                    "%s (attempt %d/%d): %s. Aborting commit.",
                    offset.offset,
                    topic_partition,
                    attempt,
                    max_attempts,
                    err
                )
                return  # Stop trying after unexpected error

        # If loop finishes without returning, all retries failed
        logger.error(
            "Failed to commit offset %d for partition %s after %d attempts.",
            offset.offset,
            topic_partition,
            max_attempts
        )

    @staticmethod
    def _validate_config(config: KafkaConsumerConfig) -> None:
        """Validates the essential configuration parameters of a
        KafkaConsumerConfig object.

        This method checks for the presence of required configurations such as
        'bootstrap_servers', 'topic', and 'group_id'. If any of these are
        missing, it raises a ValueError with a descriptive message.

        Args:
            config (KafkaConsumerConfig): The configuration object to validate.

        Raises:
            ValueError: If any of the required configurations are None.
        """
        if config.bootstrap_servers is None:
            raise ValueError('bootstrap_servers must be provided.')
        if config.topic is None:
            raise ValueError('topic must be provided.')
        if config.group_id is None:
            raise ValueError('group_id must be provided.')

    @staticmethod
    def _get_health_check_interval(session_timeout_ms: Optional[int]) -> int:
        """Calculates the health check interval in seconds based on the max
        poll interval.

        If the provided max_poll_interval_ms is None, a default hardcoded
        value is used. A warning is logged in this case to indicate the use
        of the default value.

        Args:
            session_timeout_ms (int): The maximum interval in milliseconds that
            the consumer can be idle before being considered dead.

        Returns:
            int: The health check interval in seconds.
        """
        # Default session timeout if not provided
        timeout_ms = session_timeout_ms if session_timeout_ms is not None else 10000
        if timeout_ms <= 0:
            timeout_ms = 10000  # Ensure positive
            logger.warning(
                "session_timeout_ms is non-positive. Using default: %s ms",
                timeout_ms
            )

        # Health check should be significantly less than session timeout
        # Typically 1/3rd is recommended for heartbeat, maybe similar
        # for health check?
        interval_sec = max(1, int(timeout_ms / 3 / 1000))
        logger.info(
            "Setting health check interval to %d seconds based on "
            "session timeout %d ms",
            interval_sec,
            timeout_ms
        )
        return interval_sec
