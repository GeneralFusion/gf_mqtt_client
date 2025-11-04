"""
Synchronous MQTT Client Implementation.

This module provides the synchronous (blocking) MQTT client implementation using
the paho-mqtt library. It implements the MQTTClientBase interface to provide
traditional blocking I/O operations with request-response pattern support.

Key Features:
    - Pure synchronous/blocking operations (no asyncio dependencies)
    - Request-response pattern with threading-based correlation
    - Extensible message handler system (synchronous handlers only)
    - Thread-safe handler management using threading.Lock
    - Background message processing via paho's loop_start()
    - Context manager support for automatic connect/disconnect

Architecture:
    The sync client uses threading primitives for concurrency:
    - threading.Event: Connection state management
    - threading.Lock: Thread-safe access to shared data structures
    - queue.Queue: Request-response correlation and blocking waits
    - paho-mqtt loop_start(): Background network thread

    Message handlers execute synchronously in paho's network thread, so they
    must be fast and non-blocking to avoid impacting message throughput.

Example:
    >>> from gf_mqtt_client import SyncMQTTClient, Method
    >>>
    >>> # Using context manager (auto connect/disconnect)
    >>> with SyncMQTTClient("broker.emqx.io", port=1883) as client:
    ...     response = client.request(
    ...         target_device_tag="device-001",
    ...         subsystem="sensors",
    ...         path="/temperature",
    ...         method=Method.GET
    ...     )
    ...     print(response["body"]["value"])
    >>>
    >>> # Manual connection management
    >>> client = SyncMQTTClient("broker.emqx.io", port=1883)
    >>> client.connect()
    >>> response = client.request("device-001", "sensors", "/temperature")
    >>> client.disconnect()

Threading Notes:
    - Message handlers run in paho's network thread
    - Keep handlers fast to avoid blocking message processing
    - When publishing from within a handler, set wait=False to avoid deadlock
    - All public methods are thread-safe

See Also:
    - MQTTClient: Asynchronous async/await-based implementation
    - MQTTClientBase: Abstract base class defining the interface
"""
import threading
import queue
import logging
import orjson
from typing import Any
from pydantic import SecretStr
import paho.mqtt.client as mqtt

from ..core.base import MQTTClientBase, parse_method
from ..core.models import Method, ResponseCode
from ..core.exceptions import ResponseException, GatewayTimeoutResponse
from .message_handler import (
    SyncMessageHandlerProtocol,
    SyncResponseHandlerDefault,
)

logger = logging.getLogger(__name__)


class SyncMQTTClient(MQTTClientBase):
    """
    Synchronous MQTT client with blocking request-response pattern support.

    This client provides traditional blocking I/O operations for MQTT messaging,
    making it suitable for synchronous applications, scripts, and environments
    where asyncio is not available or desirable.

    The client uses paho-mqtt's loop_start() for background message processing
    in a separate thread, while providing blocking API methods for sending requests
    and receiving responses. Request-response correlation is handled using
    threading.Queue objects for efficient blocking waits.

    Attributes:
        _client: The underlying paho.mqtt.client.Client instance
        _connected: Boolean flag indicating connection status
        _connected_event: threading.Event for waiting on connection establishment
        _pending_requests: Dict mapping request_id to queue.Queue for correlation
        _pending_requests_lock: threading.Lock for thread-safe request management
        _message_handlers: List of registered synchronous message handlers
        _handlers_lock: threading.Lock for thread-safe handler management

    Inherited Attributes:
        broker: MQTT broker hostname
        identifier: Unique client identifier
        timeout: Default timeout for requests in seconds
        qos_default: Default quality of service level (0, 1, or 2)
        logger: Contextual logger with client_id

    Thread Safety:
        All public methods are thread-safe and can be called from multiple threads.
        Internal data structures are protected by locks. Message handlers run in
        paho's network thread and must not perform blocking operations.

    Example:
        >>> # Simple request-response
        >>> client = SyncMQTTClient("broker.emqx.io")
        >>> client.connect()
        >>> response = client.request("device-001", "sensors", "/temp", Method.GET)
        >>> client.disconnect()
        >>>
        >>> # Using context manager
        >>> with SyncMQTTClient("broker.emqx.io") as client:
        ...     response = client.request("device-001", "sensors", "/temp")
        ...     print(response["body"])

    Warning:
        Message handlers execute in paho's network thread. Keep them fast and
        non-blocking. When publishing from within a handler, use wait=False to
        avoid deadlock with the network thread.
    """

    def __init__(
        self,
        broker: str,
        port: int | None = None,
        timeout: int = 5,
        identifier: str | None = None,
        subscriptions: list | None = None,
        username: str | None = None,
        password: str | SecretStr | None = None,
        ensure_unique_identifier: bool = False,
        logger: logging.LoggerAdapter | None = None,
        qos_default: int = 0,
    ):
        """
        Initialize the synchronous MQTT client.

        Args:
            broker: MQTT broker hostname
            port: MQTT broker port (default: 1883)
            timeout: Default timeout for operations in seconds
            identifier: Client identifier (auto-generated if None)
            subscriptions: List of topics to subscribe to on connect
            username: MQTT username for authentication
            password: MQTT password for authentication
            ensure_unique_identifier: If True, append UUID to identifier
            logger: Custom logger adapter (creates default if None)
            qos_default: Default QoS level for publish/subscribe
        """
        # Initialize base class
        super().__init__(
            broker=broker,
            port=port or 1883,
            timeout=timeout,
            identifier=identifier,
            subscriptions=subscriptions,
            username=username,
            password=password,
            ensure_unique_identifier=ensure_unique_identifier,
            logger=logger,
            qos_default=qos_default,
        )

        # Sync-specific attributes
        self._client: mqtt.Client | None = None
        self._connected = False
        self._connected_event = threading.Event()
        self._pending_requests: dict[str, queue.Queue] = {}
        self._pending_requests_lock = threading.Lock()
        self._message_handlers: list[SyncMessageHandlerProtocol] = []
        self._handlers_lock = threading.Lock()

    def _on_connect(self, client, userdata, flags, rc):
        """
        Paho callback invoked when connection is established.

        This callback runs in paho's network thread when the broker accepts
        the connection. It performs subscription setup and handler registration.

        Args:
            client: The paho mqtt.Client instance
            userdata: User data set in Client() constructor (unused)
            flags: Response flags sent by the broker
            rc: Connection result code (0 = success, >0 = error)

        Note:
            This is an internal callback and should not be called directly.
            The callback runs in paho's network thread.
        """
        if rc == 0:
            self.logger.info(
                f"Successfully connected to broker {self.broker}:{self._port} as client '{self.identifier}'"
            )
            self._connected = True
            self._connected_event.set()

            # Subscribe to default topics
            request_topic = self._topic_manager.build_request_topic(
                target_device_tag=self.identifier, subsystem="+", request_id="+"
            )
            self.subscriptions.append(request_topic)

            for topic in self.subscriptions:
                self._client.subscribe(topic, qos=self.qos_default)
                self.logger.debug(f"Subscribed to topic pattern: {topic}")

            # Add default response handler
            self.add_message_handler(SyncResponseHandlerDefault())
            self.logger.debug("Registered default response handler")
        else:
            error_messages = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorized"
            }
            error_detail = error_messages.get(rc, f"Unknown error code {rc}")
            self.logger.error(
                f"Connection to broker {self.broker}:{self._port} failed: {error_detail} (code {rc}). "
                f"Solution: Check broker availability, verify credentials, and ensure network connectivity.",
                extra={"broker": self.broker, "port": self._port, "return_code": rc}
            )
            self._connected = False
            self._connected_event.clear()

    def _on_disconnect(self, client, userdata, rc):
        """
        Paho callback invoked when disconnected from broker.

        This callback runs in paho's network thread when the connection to the
        broker is lost, either intentionally or due to network issues.

        Args:
            client: The paho mqtt.Client instance
            userdata: User data set in Client() constructor (unused)
            rc: Disconnect result code (0 = clean disconnect, >0 = unexpected)

        Note:
            This is an internal callback and should not be called directly.
            The callback runs in paho's network thread.
        """
        self._connected = False
        self._connected_event.clear()
        if rc != 0:
            self.logger.warning(
                f"Unexpected disconnection from broker {self.broker}:{self._port} (code {rc}). "
                f"This may indicate network issues, broker restart, or connection timeout. "
                f"Solution: Check network connectivity and broker status. "
                f"Consider implementing reconnection logic if needed.",
                extra={"broker": self.broker, "port": self._port, "return_code": rc}
            )
        else:
            self.logger.info(f"Clean disconnect from broker {self.broker}:{self._port}")

    def _on_message(self, client, userdata, msg):
        """
        Paho callback for processing incoming MQTT messages.

        This callback runs synchronously in paho's network thread for every received
        message. It decodes the JSON payload, extracts message metadata, and dispatches
        to registered handlers. The callback must return quickly to avoid blocking the
        network thread and impacting message throughput.

        Processing Flow:
            1. Decode message payload from bytes to JSON
            2. Extract request_id and determine if response or request
            3. Dispatch to registered handlers via _dispatch_to_handlers()
            4. For responses to pending requests, queue the result

        Args:
            client: The paho mqtt.Client instance
            userdata: User data set in Client() constructor (unused)
            msg: paho.mqtt.client.MQTTMessage object with topic and payload

        Performance:
            This callback runs in the network thread. Keep processing fast to avoid
            blocking message reception. Heavy processing should be offloaded to
            worker threads or queues.

        Note:
            This is an internal callback and should not be called directly.
        """
        try:
            topic = msg.topic
            payload_str = msg.payload.decode()
            payload = orjson.loads(payload_str)

            self.logger.debug(
                f"Received message on topic '{topic}': {self._truncate_str(payload_str, output_length=100)}",
                extra=self._extract_extras(payload)
            )

            header = payload.get("header", {})
            request_id = header.get("request_id")
            is_response = "response_code" in header

            # Dispatch to handlers first (they may raise exceptions)
            # For pending request responses, exceptions will be caught and queued
            self._dispatch_to_handlers(topic, payload, request_id if is_response else None)

        except orjson.JSONDecodeError as e:
            self.logger.warning(
                f"Failed to decode JSON payload from topic '{topic}': {str(e)}. "
                f"Payload preview: {self._truncate_str(payload_str, output_length=200)}. "
                f"Solution: Ensure sender is publishing valid JSON. Invalid messages are discarded.",
                extra={"topic": topic, "error_position": e.pos}
            )
        except Exception as e:
            self.logger.error(
                f"Unexpected error in message callback for topic '{topic}': {type(e).__name__}: {str(e)}. "
                f"This may indicate a bug in a message handler. Message processing continues.",
                exc_info=True,
                extra={"topic": topic, "error_type": type(e).__name__}
            )

    def _dispatch_to_handlers(self, topic: str, payload: dict, pending_request_id: str | None = None):
        """
        Dispatch message to registered synchronous handlers.

        Iterates through registered handlers in order, calling can_handle() and handle()
        for matching handlers. Handles exceptions from handlers and queues results for
        pending requests. Runs synchronously in paho's network thread.

        Handler Processing:
            1. Copy handler list under lock to avoid blocking
            2. For each handler, check if it can handle the message
            3. Call handler.handle() for matching handlers
            4. Stop processing if handler.propagate=False or exception raised
            5. For pending requests, queue the result (payload or exception)

        Args:
            topic: MQTT topic the message was received on
            payload: Decoded message payload (dict)
            pending_request_id: If this is a response to a pending request, the request_id.
                              When set, the result is queued for the waiting request() call.

        Exception Handling:
            - ResponseException: Logged and queued for pending requests
            - Other exceptions: Logged with traceback and queued for pending requests
            - Exceptions stop handler chain processing

        Performance:
            This method runs in paho's network thread. Handlers should be fast to avoid
            blocking message processing. Heavy work should be offloaded to worker threads.

        Note:
            All handlers must be synchronous (no async/await). Use SyncMessageHandlerBase
            and its subclasses for implementing handlers.
        """
        with self._handlers_lock:
            handlers = list(self._message_handlers)  # Copy to avoid lock issues

        processed = False
        exception_raised = None

        for handler in handlers:
            try:
                if not handler.can_handle(self, topic, payload):
                    continue

                # Execute sync handler directly
                handler.handle(self, topic, payload)

                processed = True
                if not handler.propagate:
                    break

            except ResponseException as e:
                self.logger.error(
                    f"'{ResponseCode(e.response_code)}' during request to '{e.target}' "
                    f"on path '{e.path}': {e.detail}"
                )
                exception_raised = e
                break  # Stop processing handlers after exception
            except Exception as e:
                self.logger.error(f"Handler error: {e}", exc_info=True)
                exception_raised = e
                break

        # If this is a response to a pending request, queue the result (payload or exception)
        if pending_request_id:
            with self._pending_requests_lock:
                response_queue = self._pending_requests.get(pending_request_id)
                if response_queue:
                    if exception_raised:
                        response_queue.put(exception_raised)
                    else:
                        response_queue.put(payload)
                    return  # Don't continue processing

        if not processed and not exception_raised:
            # Default handler
            self.logger.debug(f"No handler processed message on topic {topic}")

    def connect(self):
        """
        Connect to the MQTT broker and start background message processing.

        This method establishes a connection to the MQTT broker, starts paho's background
        network loop in a separate thread, and waits for the connection to be established.
        Once connected, it subscribes to configured topics and registers the default
        response handler.

        The method blocks until connection is established or timeout occurs. After
        successful connection, messages are processed asynchronously in paho's network
        thread via registered handlers.

        Returns:
            self: For method chaining (enables `client.connect().request(...)`)

        Raises:
            GatewayTimeoutResponse: If connection fails or times out

        Example:
            >>> client = SyncMQTTClient("broker.emqx.io")
            >>> client.connect()  # Blocks until connected
            >>> # Now ready to send/receive messages
            >>> client.disconnect()

        Threading:
            After connect() returns, paho's network loop runs in a background thread,
            processing incoming messages and invoking callbacks. The thread is stopped
            by disconnect().

        Note:
            This method is idempotent - calling it multiple times has no effect if
            already connected. Always call disconnect() before program exit to properly
            clean up the background thread.
        """
        if self._connected:
            self.logger.debug(
                "Connection attempt skipped: client already connected to broker. "
                "To reconnect, call disconnect() first."
            )
            return self

        self.logger.debug(
            f"Initiating connection to broker {self.broker}:{self._port} "
            f"with client identifier '{self.identifier}'"
        )

        # Create paho client
        self._client = mqtt.Client(client_id=self.identifier, protocol=mqtt.MQTTv311)

        # Set callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # Set credentials if provided
        if self._username:
            password = (
                self._password.get_secret_value()
                if isinstance(self._password, SecretStr)
                else self._password
            )
            self._client.username_pw_set(self._username, password)

        # Connect (blocking)
        try:
            self._client.connect(self.broker, self._port, keepalive=60)
        except Exception as e:
            self.logger.error(
                f"Failed to connect to broker {self.broker}:{self._port}: {e}. "
                f"Possible causes: network unreachable, broker offline, or firewall blocking port {self._port}. "
                f"Solution: Verify broker is running and accessible, check network connectivity, and validate firewall rules.",
                extra={"broker": self.broker, "port": self._port, "error": str(e)}
            )
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path="",
                detail=str(e),
                source=self.identifier,
            )

        # Start network loop in background thread
        self._client.loop_start()
        self.logger.debug("Started paho background network loop thread")

        # Wait for connection to be established
        if not self._connected_event.wait(timeout=self.timeout):
            self.logger.error(
                f"Connection to broker {self.broker}:{self._port} timed out after {self.timeout} seconds. "
                f"The TCP connection may have been established but MQTT handshake did not complete. "
                f"Solution: Check broker logs, verify MQTT protocol version compatibility, "
                f"and ensure broker is accepting connections.",
                extra={"broker": self.broker, "port": self._port, "timeout": self.timeout}
            )
            self.disconnect()
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path="",
                detail=f"Connection timeout after {self.timeout} seconds",
                source=self.identifier,
            )

        return self

    def disconnect(self):
        """
        Disconnect from the MQTT broker and stop the background network thread.

        This method performs graceful shutdown by:
        1. Marking the client as disconnected (blocks new operations)
        2. Failing all pending requests with TimeoutError
        3. Stopping paho's background network loop thread
        4. Disconnecting from the broker

        The method is safe to call multiple times and suppresses errors during cleanup.
        Any pending request() calls will receive TimeoutError.

        Example:
            >>> client.connect()
            >>> # ... use client ...
            >>> client.disconnect()
            # Background thread stopped, connection closed

        Note:
            Always call this method before program termination to ensure proper cleanup
            of the background thread and to prevent pending requests from hanging.
        """
        if not self._connected:
            self.logger.debug("Disconnect skipped: client not connected")
            return

        self.logger.debug(
            f"Initiating graceful disconnect from broker {self.broker}:{self._port}. "
            f"Pending requests: {len(self._pending_requests)}"
        )

        # Mark as disconnected first
        self._connected = False
        self._connected_event.clear()

        # Fail all pending requests
        with self._pending_requests_lock:
            cancelled_count = len(self._pending_requests)
            for request_queue in self._pending_requests.values():
                request_queue.put(TimeoutError("Client disconnected"))
            self._pending_requests.clear()
            if cancelled_count > 0:
                self.logger.warning(
                    f"Cancelled {cancelled_count} pending request(s) during disconnect. "
                    f"These requests will raise TimeoutError to their callers."
                )

        # Stop network loop and disconnect
        if self._client:
            self._client.loop_stop()
            self.logger.debug("Stopped paho background network loop thread")
            self._client.disconnect()
            self._client = None

        self.logger.info(f"Successfully disconnected from broker {self.broker}:{self._port}")

    def publish(self, topic: str, payload: Any, qos: int | None = None, wait: bool = True):
        """
        Publish a message to a topic.

        Args:
            topic: MQTT topic to publish to
            payload: Message payload (will be JSON-encoded)
            qos: Quality of Service level (uses qos_default if None)
            wait: If True, block until message is published. If False, return immediately.
                  Set to False when publishing from within message handlers to avoid deadlock.
        """
        qos = qos if qos is not None else self.qos_default
        if not self._connected:
            raise RuntimeError("Client not connected")

        payload_bytes = orjson.dumps(payload)
        result = self._client.publish(topic, payload_bytes, qos=qos)

        # Only wait if requested and not in callback thread
        if wait:
            result.wait_for_publish()  # Block until published

        self.logger.debug(f"Published to topic {topic}")

    def subscribe(self, topic: str, qos: int | None = None):
        """
        Subscribe to an MQTT topic (blocking).

        Args:
            topic: MQTT topic pattern to subscribe to
            qos: Quality of Service level (uses qos_default if None)
        """
        qos = qos if qos is not None else self.qos_default
        if not self._connected:
            raise RuntimeError("Client not connected")

        self._client.subscribe(topic, qos=qos)
        self.logger.debug(f"Subscribed to topic {topic}")

    def request(
        self,
        target_device_tag: str,
        subsystem: str,
        path: str,
        method: Method = Method.GET,
        value: Any = None,
        timeout: int | None = None,
        qos: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a request and wait for response (blocking).

        Args:
            target_device_tag: Target device identifier
            subsystem: Subsystem name
            path: Request path (URI-like)
            method: HTTP-like method (GET, POST, PUT, DELETE)
            value: Request body/value
            timeout: Request timeout in seconds (uses self.timeout if None)
            qos: Quality of Service level (uses qos_default if None)

        Returns:
            Response payload dictionary

        Raises:
            ResponseException: If response indicates an error
            TimeoutError: If request times out
        """
        method = parse_method(method)

        if not self._connected:
            raise RuntimeError("Client not connected. Call connect() first.")

        request_id = self.generate_request_id()
        effective_timeout = timeout if timeout is not None else self.timeout

        # Build request payload using shared handler
        request_payload = self._payload_handler.create_request_payload(
            method=method,
            path=path,
            request_id=request_id,
            body=value,
            source=self.identifier
        )

        # Build topics using shared topic manager
        request_topic = self._topic_manager.build_request_topic(
            target_device_tag=target_device_tag,
            subsystem=subsystem,
            request_id=request_id,
        )
        response_topic = self._topic_manager.build_response_topic(
            request_topic=request_topic
        )

        # Create response queue
        response_queue = queue.Queue(maxsize=1)
        with self._pending_requests_lock:
            self._pending_requests[request_id] = response_queue

        try:
            # Subscribe to response topic
            self.subscribe(response_topic, qos=qos)

            # Publish request
            self.publish(request_topic, request_payload, qos=qos)

            self.logger.info(
                f"'{Method(method)}' request sent to '{target_device_tag}' on uri-path '{path}'",
                extra=self._extract_extras(request_payload, extra_extras={"topic": request_topic})
            )

            # Block waiting for response
            try:
                result = response_queue.get(timeout=effective_timeout)

                # Check if result is an exception
                if isinstance(result, Exception):
                    raise result

                # Process response
                response_code = ResponseCode(result.get("header", {}).get("response_code"))
                self.logger.info(
                    f"'{Method(method)}' request successful with outcome '{response_code}' "
                    f"to '{target_device_tag}' on uri-path '{path}'",
                    extra=self._extract_extras(result, extra_extras={"topic": response_topic})
                )
                return result

            except queue.Empty:
                self.logger.error(
                    f"'{Method(method)}' request to '{target_device_tag}' on uri-path '{path}' "
                    f"with request_id '{request_id}' timed out after {effective_timeout} seconds",
                    extra=self._extract_extras(request_payload, extra_extras={"topic": request_topic})
                )
                raise GatewayTimeoutResponse(
                    response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                    path=path,
                    detail="Request timed out",
                    source=self.identifier,
                    target=target_device_tag,
                )

        finally:
            # Cleanup
            self._client.unsubscribe(response_topic)
            with self._pending_requests_lock:
                self._pending_requests.pop(request_id, None)

    def add_message_handler(self, handler: SyncMessageHandlerProtocol):
        """
        Add a synchronous message handler (thread-safe).

        Args:
            handler: Handler implementing SyncMessageHandlerProtocol

        Note:
            Only synchronous handlers are supported. Handlers must not use async/await.
        """
        with self._handlers_lock:
            if not isinstance(handler, SyncMessageHandlerProtocol):
                raise ValueError("Handler must implement SyncMessageHandlerProtocol")
            self._message_handlers.append(handler)
            self.logger.debug(f"Added message handler {handler.__class__.__name__}")

    def remove_message_handler(self, handler: SyncMessageHandlerProtocol):
        """
        Remove a synchronous message handler (thread-safe).

        Args:
            handler: Handler to remove
        """
        with self._handlers_lock:
            if handler in self._message_handlers:
                self._message_handlers.remove(handler)
                self.logger.debug(f"Removed message handler {handler.__class__.__name__}")
            else:
                self.logger.warning(f"Handler {handler.__class__.__name__} not found")

    @property
    def is_connected(self) -> bool:
        """Check if client is currently connected to broker."""
        return self._connected

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
