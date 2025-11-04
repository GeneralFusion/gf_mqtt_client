"""
Synchronous MQTT client using paho-mqtt directly.
No asyncio dependencies - pure blocking I/O.
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
    Synchronous MQTT client using paho-mqtt blocking APIs.

    Uses threading primitives (Queue, Lock) for request/response correlation.
    Message handlers are executed in paho's network thread.
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
        """Paho callback when connection is established."""
        if rc == 0:
            self.logger.info(f"Connected to broker {self.broker}:{self._port}")
            self._connected = True
            self._connected_event.set()

            # Subscribe to default topics
            request_topic = self._topic_manager.build_request_topic(
                target_device_tag=self.identifier, subsystem="+", request_id="+"
            )
            self.subscriptions.append(request_topic)

            for topic in self.subscriptions:
                self._client.subscribe(topic, qos=self.qos_default)
                self.logger.debug(f"Subscribed to topic {topic}")

            # Add default response handler
            self.add_message_handler(SyncResponseHandlerDefault())
        else:
            self.logger.error(f"Connection failed with code {rc}")
            self._connected = False
            self._connected_event.clear()

    def _on_disconnect(self, client, userdata, rc):
        """Paho callback when disconnected."""
        self._connected = False
        self._connected_event.clear()
        if rc != 0:
            self.logger.warning(f"Unexpected disconnection (code {rc})")
        else:
            self.logger.info("Disconnected from broker")

    def _on_message(self, client, userdata, msg):
        """
        Paho callback for incoming messages.
        Runs in paho's network thread - keep it fast!
        """
        try:
            topic = msg.topic
            payload_str = msg.payload.decode()
            payload = orjson.loads(payload_str)

            self.logger.debug(
                f"Received message: {self._truncate_str(payload_str, output_length=100)}",
                extra=self._extract_extras(payload)
            )

            header = payload.get("header", {})
            request_id = header.get("request_id")
            is_response = "response_code" in header

            # Dispatch to handlers first (they may raise exceptions)
            # For pending request responses, exceptions will be caught and queued
            self._dispatch_to_handlers(topic, payload, request_id if is_response else None)

        except orjson.JSONDecodeError:
            self.logger.warning(
                f"Failed to decode JSON payload from topic {topic}: {payload_str}"
            )
        except Exception as e:
            self.logger.error(f"Error in message callback: {e}", exc_info=True)

    def _dispatch_to_handlers(self, topic: str, payload: dict, pending_request_id: str | None = None):
        """
        Dispatch message to registered sync handlers.
        Runs synchronously in paho's network thread.

        Args:
            topic: MQTT topic
            payload: Message payload
            pending_request_id: If this is a response to a pending request, the request_id

        Note: All handlers must be synchronous (no async/await).
        Use SyncMessageHandlerBase and its subclasses.
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
        Connect to the MQTT broker (blocking).

        Returns:
            self for chaining
        """
        if self._connected:
            self.logger.debug("Client already connected to broker")
            return self

        self.logger.debug(f"Connecting to broker {self.broker}:{self._port}")

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
            self.logger.error(f"Failed to connect to broker: {e}")
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path="",
                detail=str(e),
                source=self.identifier,
            )

        # Start network loop in background thread
        self._client.loop_start()

        # Wait for connection to be established
        if not self._connected_event.wait(timeout=self.timeout):
            self.disconnect()
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path="",
                detail="Connection timeout",
                source=self.identifier,
            )

        return self

    def disconnect(self):
        """
        Disconnect from the MQTT broker (blocking).
        """
        if not self._connected:
            return

        self.logger.debug(f"Disconnecting from broker {self.broker}:{self._port}")

        # Mark as disconnected first
        self._connected = False
        self._connected_event.clear()

        # Fail all pending requests
        with self._pending_requests_lock:
            for request_queue in self._pending_requests.values():
                request_queue.put(TimeoutError("Client disconnected"))
            self._pending_requests.clear()

        # Stop network loop and disconnect
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None

        self.logger.info(f"Disconnected from broker {self.broker}:{self._port}")

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
