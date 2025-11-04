"""
Asynchronous MQTT Client Implementation.

This module provides the asynchronous MQTT client implementation using the aiomqtt library.
It implements the MQTTClientBase interface to provide async/await-based MQTT operations
with request-response pattern support.

Key Features:
    - Full async/await support using aiomqtt
    - Request-response pattern with automatic correlation
    - Extensible message handler system
    - Automatic reconnection and error handling
    - Windows event loop compatibility checks
    - Thread-safe pending request management

Classes:
    MQTTClient: Main asynchronous MQTT client class

The client uses:
    - asyncio for concurrency
    - orjson for fast JSON serialization
    - aiomqtt for MQTT protocol handling
    - asyncio.Future for request-response correlation

Example:
    >>> import asyncio
    >>> from gf_mqtt_client import MQTTClient, Method
    >>>
    >>> async def main():
    ...     client = MQTTClient(broker="broker.emqx.io", port=1883)
    ...     await client.connect()
    ...
    ...     # Send a request and wait for response
    ...     response = await client.request(
    ...         target_device_tag="device-001",
    ...         subsystem="sensors",
    ...         path="/temperature",
    ...         method=Method.GET
    ...     )
    ...
    ...     await client.disconnect()
    >>>
    >>> asyncio.run(main())

See Also:
    - MQTTClientBase: Abstract base class defining the interface
    - SyncMQTTClient: Synchronous blocking implementation
"""
import asyncio
import contextlib
import orjson
from typing import Any, Self
import logging
from pydantic import SecretStr
from aiomqtt import Client, MqttError
from ..core.models import ResponseCode, Method, MessageType
from .message_handler import (
    MessageHandlerProtocol,
    MessageHandlerBase,
    ResponseHandlerDefault,
)
from ..core.exceptions import ResponseException, GatewayTimeoutResponse
from .compatibility import ensure_compatible_event_loop_policy
from ..core.base import MQTTClientBase, parse_method, MessageLogger

logger = logging.getLogger(__name__)


class MQTTClient(MQTTClientBase):
    """
    Asynchronous MQTT client with request-response pattern support.

    This client provides full async/await support for MQTT operations, including:
    - Publishing and subscribing to topics
    - Request-response correlation with automatic timeout handling
    - Extensible message handler system for custom processing
    - Automatic connection management and error recovery

    The client maintains pending requests using asyncio.Future objects and automatically
    correlates responses based on request_id fields in the message headers.

    Attributes:
        _client: The underlying aiomqtt.Client instance
        _client_task: Background task running the message loop
        _pending_requests: Dict mapping request_id to asyncio.Future for correlation
        _lock: Asyncio lock for thread-safe pending request management
        _message_handlers: List of registered message handlers
        _connected: Asyncio event indicating connection status

    Inherited Attributes:
        broker: MQTT broker hostname
        identifier: Unique client identifier
        timeout: Default timeout for requests in seconds
        qos_default: Default quality of service level (0, 1, or 2)
        logger: Contextual logger with client_id

    Example:
        >>> async def handle_request(client, topic, payload):
        ...     # Custom request handler
        ...     return {"status": "processed"}
        >>>
        >>> client = MQTTClient("broker.emqx.io")
        >>> await client.add_message_handler(
        ...     MessageHandlerBase(
        ...         can_handle=lambda c, t, p: "method" in p.get("header", {}),
        ...         process=handle_request
        ...     )
        ... )
        >>> await client.connect()
        >>> response = await client.request("device-001", "sensors", "/temp")
        >>> await client.disconnect()
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
        # Initialize base class with shared configuration
        super().__init__(
            broker=broker,
            port=port,
            timeout=timeout,
            identifier=identifier,
            subscriptions=subscriptions,
            username=username,
            password=password,
            ensure_unique_identifier=ensure_unique_identifier,
            logger=logger,
            qos_default=qos_default,
        )

        # Async-specific attributes
        self._client: Client | None = None
        self._client_task: asyncio.Task | None = None
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._message_handlers: list[MessageHandlerProtocol] = []
        self._connected = asyncio.Event()

        # Ensure compatible event loop policy for Windows
        ensure_compatible_event_loop_policy()

    async def add_message_handler(self, handler: MessageHandlerProtocol):
        """
        Register a message handler for processing incoming MQTT messages.

        Message handlers are called in the order they were registered. Each handler
        can choose to process the message and optionally allow it to propagate to
        subsequent handlers by setting the propagate flag.

        Args:
            handler: Handler implementing MessageHandlerProtocol with can_handle()
                    and handle() methods

        Raises:
            ValueError: If handler doesn't implement MessageHandlerProtocol

        Example:
            >>> async def process_request(client, topic, payload):
            ...     return {"status": "processed"}
            >>>
            >>> handler = MessageHandlerBase(
            ...     can_handle=lambda c, t, p: "method" in p.get("header", {}),
            ...     process=process_request,
            ...     propagate=False
            ... )
            >>> await client.add_message_handler(handler)

        Note:
            Handlers are executed in registration order. Set propagate=False on a
            handler to prevent subsequent handlers from processing the same message.
        """
        async with self._lock:
            if not isinstance(handler, MessageHandlerProtocol):
                self.logger.error(
                    f"Invalid handler type: {type(handler).__name__}. "
                    "Handler must implement MessageHandlerProtocol with can_handle() and handle() methods."
                )
                raise ValueError("Handler must implement MessageHandlerProtocol")
            self._message_handlers.append(handler)
            self.logger.debug(
                f"Registered message handler: {handler.__class__.__name__} "
                f"(total handlers: {len(self._message_handlers)})"
            )

    async def remove_message_handler(self, handler: MessageHandlerProtocol):
        """
        Unregister a previously registered message handler.

        Removes the handler from the handler chain. If the handler is not found,
        a warning is logged but no exception is raised.

        Args:
            handler: The handler instance to remove (must be the same object
                    that was registered)

        Example:
            >>> handler = MessageHandlerBase(...)
            >>> await client.add_message_handler(handler)
            >>> # Later, when no longer needed:
            >>> await client.remove_message_handler(handler)

        Note:
            This method uses object identity comparison, so you must pass the
            exact same handler object that was registered. Handler removal is
            thread-safe and can be called while messages are being processed.
        """
        async with self._lock:
            if handler in self._message_handlers:
                self._message_handlers.remove(handler)
                self.logger.debug(
                    f"Unregistered message handler: {handler.__class__.__name__} "
                    f"(remaining handlers: {len(self._message_handlers)})"
                )
            else:
                self.logger.warning(
                    f"Attempted to remove handler {handler.__class__.__name__}, but it was not found. "
                    f"This may indicate the handler was already removed or never registered. "
                    f"Current registered handlers: {[h.__class__.__name__ for h in self._message_handlers]}"
                )

    async def connect(self):
        """
        Establish connection to the MQTT broker and start the message loop.

        This method performs the following operations:
        1. Connects to the MQTT broker using configured credentials
        2. Starts the background message processing loop
        3. Subscribes to all configured topics
        4. Registers the default response handler

        The method is idempotent - calling it multiple times has no effect if
        already connected. After successful connection, the client will begin
        processing incoming messages through registered handlers.

        Raises:
            GatewayTimeoutResponse: If connection to broker fails due to network
                                   or authentication issues

        Example:
            >>> client = MQTTClient("broker.emqx.io", port=1883)
            >>> await client.connect()
            # Client is now ready to publish and receive messages

        Note:
            This method automatically subscribes to request topics matching the
            client's identifier, enabling the client to receive requests from
            other devices. Always call disconnect() to properly clean up resources.
        """
        if self._connected.is_set():
            self.logger.debug(
                "Connection attempt skipped: client already connected to broker. "
                "To reconnect, call disconnect() first."
            )
            return

        self.logger.debug(
            f"Initiating connection to broker {self.broker}:{self._port or '(default)'} "
            f"with client identifier '{self.identifier}'"
        )

        # Convert SecretStr to str lazily
        password = (
            self._password.get_secret_value()
            if isinstance(self._password, SecretStr)
            else self._password
        )

        kwargs: dict[str, Any] = {"hostname": self.broker}
        if self._port is not None:
            kwargs["port"] = self._port
        if self._username is not None:
            kwargs["username"] = self._username
        if password is not None:
            kwargs["password"] = password
        if self.identifier is not None:
            kwargs["identifier"] = self.identifier

        self._client = Client(**kwargs)
        try:
            await self._client.__aenter__()
            self.logger.info(
                f"Successfully connected to broker {self.broker}:{kwargs.get('port', '(default)')} "
                f"as client '{self.identifier}'"
            )
            self._client_task = asyncio.create_task(self._message_loop())
            self._connected.set()
        except MqttError as e:
            self.logger.error(
                f"Failed to connect to broker {self.broker}:{kwargs.get('port', '(default)')}: {e}. "
                f"Possible causes: network unreachable, broker offline, authentication failure, "
                f"or firewall blocking port {kwargs.get('port', 1883)}. "
                f"Solution: Verify broker is running, check network connectivity, and validate credentials.",
                extra={"broker": self.broker, "port": kwargs.get('port', 1883), "error": str(e)}
            )
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path="",
                detail=str(e),
                source=self.identifier,
            )

        request_topic = self._topic_manager.build_request_topic(
            target_device_tag=self.identifier, subsystem="+", request_id="+"
        )
        self.subscriptions.append(request_topic)

        for topic in self.subscriptions:
            await self.subscribe(topic, qos=self.qos_default)

        await self.add_message_handler(ResponseHandlerDefault())

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    async def disconnect(self):
        """
        Disconnect from the MQTT broker and clean up all resources.

        This method performs graceful shutdown in the following order:
        1. Blocks new operations by clearing the connected flag
        2. Stops the background message processing loop
        3. Closes the MQTT client connection
        4. Cancels all pending requests with CancelledError

        The method is safe to call multiple times and will suppress errors during
        cleanup. Any pending request() calls will receive asyncio.CancelledError.

        Example:
            >>> await client.connect()
            >>> # ... use client ...
            >>> await client.disconnect()
            # Client resources are now cleaned up

        Note:
            Always call this method before program termination to ensure proper
            resource cleanup and to prevent pending requests from hanging indefinitely.
            The method ensures all futures are completed so no coroutines are left waiting.
        """
        self.logger.debug(
            f"Initiating graceful disconnect from broker {self.broker}:{self._port}. "
            f"Pending requests: {len(self._pending_requests)}"
        )

        # 1) Block new ops immediately
        self._connected.clear()

        # 2) Stop message loop
        if self._client_task:
            self._client_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._client_task
            self._client_task = None
            self.logger.debug("Message processing loop stopped")

        # 3) Close client
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.__aexit__(None, None, None)
            self._client = None
            self.logger.debug("MQTT client connection closed")

        # 4) Fail any pending requests so callers aren't left hanging
        async with self._lock:
            cancelled_count = 0
            for fut in self._pending_requests.values():
                if not fut.done():
                    fut.set_exception(asyncio.CancelledError("MQTT client disconnected"))
                    cancelled_count += 1
            self._pending_requests.clear()
            if cancelled_count > 0:
                self.logger.warning(
                    f"Cancelled {cancelled_count} pending request(s) during disconnect. "
                    f"These requests will raise asyncio.CancelledError to their callers."
                )

        self.logger.info(f"Successfully disconnected from broker {self.broker}:{self._port}")

    async def _message_loop(self):
        """
        Background task that continuously processes incoming MQTT messages.

        This is the core message processing loop that:
        1. Receives messages from subscribed topics
        2. Decodes and validates JSON payloads
        3. Routes messages through registered handlers
        4. Correlates responses with pending requests
        5. Handles exceptions and errors

        The loop runs continuously until the client disconnects or the task is cancelled.
        Messages are processed sequentially in the order they arrive.

        Handler Processing Flow:
            - Handlers are called in registration order
            - Each handler's can_handle() is checked first
            - Matching handlers process the message via handle()
            - If handler.propagate=False, processing stops after that handler
            - If no handlers match, the default handler is used

        Exception Handling:
            - ResponseException: Logged and forwarded to pending request future
            - JSONDecodeError: Logged as warning, message discarded
            - Other exceptions: Logged with full traceback and re-raised

        Note:
            This is an internal method and should not be called directly.
            It is automatically started by connect() and stopped by disconnect().
        """
        async for message in self._client.messages:
            topic = str(message.topic)  # Convert Topic object to string
            payload_str = message.payload.decode()
            try:
                payload = await asyncio.to_thread(orjson.loads, payload_str)
                header = payload.get("header", {})
                request_id = header.get("request_id", "N/A")
                self.logger.debug(
                    f"Received message on topic '{topic}': {self._truncate_str(payload_str, output_length=100)}",
                    extra=self._extract_extras(payload)
                )
                processed = False
                for handler in self._message_handlers:
                    if not handler.can_handle(self, topic, payload):
                        continue
                    try:
                        response = await handler.handle(self, topic, payload)
                        if response is not None:
                            if "request_id" in payload.get("header", {}):
                                async with self._lock:
                                    future = self._pending_requests.pop(request_id, None)
                                if future and not future.done():
                                    future.set_result(payload)
                                    self.logger.debug(
                                        f"Resolved pending request {request_id} via handler {handler.__class__.__name__}"
                                    )
                            processed = True
                        if not handler.propagate:
                            self.logger.debug(
                                f"Handler {handler.__class__.__name__} stopped propagation for message {request_id}"
                            )
                            break
                    except ResponseException as e:
                        self.logger.error(
                            f"Handler {handler.__class__.__name__} raised {ResponseCode(e.response_code).name} "
                            f"({e.response_code}) for request '{e.request_id}' to '{e.target}' on path '{e.path}': {e.detail}",
                            extra={"request_id": e.request_id, "path": e.path, "response_code": e.response_code}
                        )
                        async with self._lock:
                            future = self._pending_requests.pop(
                                payload.get("header", {}).get("request_id"), None
                            )
                        if future and not future.done():
                            future.set_exception(e)
                if not processed:
                    self.logger.debug(
                        f"No handler matched message {request_id}, using default handler"
                    )
                    default_handler = MessageHandlerBase(
                        can_handle=lambda c, m, p: True,
                        process=self._default_handler,
                        propagate=True,
                    )
                    await default_handler.handle(self, topic, payload)

            except orjson.JSONDecodeError as e:
                self.logger.warning(
                    f"Failed to decode JSON payload from topic '{topic}': {str(e)}. "
                    f"Payload preview: {self._truncate_str(payload_str, output_length=200)}. "
                    f"Solution: Ensure sender is publishing valid JSON. Invalid messages are discarded.",
                    extra={"topic": topic, "error_position": e.pos}
                )

            except Exception as e:
                self.logger.error(
                    f"Unexpected error processing message from topic '{topic}': {type(e).__name__}: {str(e)}. "
                    f"This may indicate a bug in a message handler or internal client logic. "
                    f"The message loop will terminate.",
                    exc_info=True,
                    extra={"topic": topic, "error_type": type(e).__name__}
                )
                raise e

    async def _default_handler(
        self, client: Self, topic: str, payload: Any
    ) -> dict[str, Any]:
        """
        Fallback handler for messages not processed by any registered handler.

        This handler is called when no registered handlers match a message or when
        they all allow propagation. It simply logs the message and returns it unchanged.
        Override this behavior by registering handlers with propagate=False.

        Args:
            client: The MQTT client instance (self)
            topic: MQTT topic the message was received on
            payload: Decoded message payload (dict)

        Returns:
            The unmodified payload dictionary

        Note:
            This is an internal method used as a catch-all for unhandled messages.
            It prevents messages from being silently dropped.
        """
        self.logger.debug(
            f"Default handler processing message on topic '{topic}'. "
            f"No specific handler matched this message. Consider registering a custom handler "
            f"if this message type should be processed differently."
        )
        return payload

    async def publish(self, topic: str, payload: Any, qos: int | None = None):
        """
        Publish a message to an MQTT topic.

        Serializes the payload to JSON using orjson and publishes it to the specified
        topic with the configured quality of service level.

        Args:
            topic: MQTT topic to publish to (e.g., "gf_int_v1/sensors/request/device-001/abc-123")
            payload: Message payload (will be serialized to JSON - dict, list, or primitive types)
            qos: Quality of service level (0, 1, or 2). Defaults to client's qos_default if not specified.

        Raises:
            RuntimeError: If client is not connected to broker
            TypeError: If payload cannot be serialized to JSON

        Example:
            >>> await client.publish(
            ...     topic="gf_int_v1/sensors/data/device-001",
            ...     payload={"temperature": 23.5, "humidity": 45.2},
            ...     qos=1
            ... )

        Note:
            QoS levels: 0=at most once, 1=at least once, 2=exactly once.
            Higher QoS levels provide stronger delivery guarantees but increase overhead.
        """
        qos = qos if qos is not None else self.qos_default
        if not self._connected.is_set():
            self.logger.error(
                f"Cannot publish to topic '{topic}': client not connected. "
                f"Solution: Call await client.connect() before publishing."
            )
            raise RuntimeError("Client not connected")
        await self._client.publish(topic, await asyncio.to_thread(orjson.dumps, payload), qos=qos)
        self.logger.debug(
            f"Published message to topic '{topic}' with QoS {qos}",
            extra={"topic": topic, "qos": qos, "payload_size": len(str(payload))}
        )

    async def subscribe(self, topic: str, qos: int | None = None):
        """
        Subscribe to an MQTT topic to receive messages.

        Subscribes to the specified topic pattern. Messages published to matching
        topics will be received and processed by the message loop. Supports MQTT
        wildcard patterns (+ for single level, # for multiple levels).

        Args:
            topic: MQTT topic or pattern to subscribe to
                  Examples: "sensor/temp", "sensor/+", "sensor/#"
            qos: Quality of service level (0, 1, or 2). Defaults to client's qos_default if not specified.

        Raises:
            RuntimeError: If client is not connected to broker

        Example:
            >>> # Subscribe to specific topic
            >>> await client.subscribe("gf_int_v1/sensors/data/device-001")
            >>>
            >>> # Subscribe to all devices in sensors subsystem
            >>> await client.subscribe("gf_int_v1/sensors/+/+/+")
            >>>
            >>> # Subscribe to all messages for a device
            >>> await client.subscribe("gf_int_v1/+/+/device-001/#")

        Note:
            Wildcards: + matches single level, # matches multiple levels (must be last).
            The message loop must be running (via connect()) to process received messages.
        """
        qos = qos if qos is not None else self.qos_default
        if not self._connected.is_set():
            self.logger.error(
                f"Cannot subscribe to topic '{topic}': client not connected. "
                f"Solution: Call await client.connect() before subscribing."
            )
            raise RuntimeError("Client not connected")
        await self._client.subscribe(topic, qos=qos)
        self.logger.debug(
            f"Subscribed to topic pattern '{topic}' with QoS {qos}",
            extra={"topic": topic, "qos": qos}
        )

    async def request(
        self,
        target_device_tag: str,
        subsystem: str,
        path: str,
        method: Method = Method.GET,
        value: Any = None,
        timeout: int|None = None,
        qos: int|None = None,
    ) -> dict[str, Any]:
        """
        Send a request to a target device and wait for the response.

        This is the core method for request-response messaging. It:
        1. Creates a request payload with a unique request_id
        2. Subscribes to the response topic for this specific request
        3. Publishes the request to the target device
        4. Waits for the response with timeout handling
        5. Automatically unsubscribes and cleans up resources

        The method blocks until the response is received or timeout occurs.

        Args:
            target_device_tag: Identifier of the target device (e.g., "device-001", "2D_XX_0_9999")
            subsystem: Subsystem name (e.g., "sensors", "actuators", "axuv")
            path: Resource path to access (e.g., "/temperature", "/config/network")
            method: HTTP-like method (GET, POST, PUT, DELETE). Defaults to GET.
                   Can be Method enum, string ("GET"), or int (1).
            value: Optional request body data (dict, list, or primitive types)
            timeout: Timeout in seconds. Defaults to client's configured timeout if not specified.
            qos: Quality of service level (0, 1, or 2). Defaults to client's qos_default if not specified.

        Returns:
            Response payload dictionary with header and body fields

        Raises:
            RuntimeError: If client is not connected
            GatewayTimeoutResponse: If no response received within timeout period
            ResponseException: For error responses (4xx, 5xx codes) - specific subclass
                              based on response_code (NotFoundResponse, InternalServerErrorResponse, etc.)

        Example:
            >>> # GET request
            >>> response = await client.request(
            ...     target_device_tag="device-001",
            ...     subsystem="sensors",
            ...     path="/temperature"
            ... )
            >>> temp = response["body"]["value"]
            >>>
            >>> # POST request with data
            >>> response = await client.request(
            ...     target_device_tag="device-001",
            ...     subsystem="actuators",
            ...     path="/valve/state",
            ...     method=Method.POST,
            ...     value={"state": "open"},
            ...     timeout=10
            ... )

        Note:
            Each request uses a unique request_id (UUID) for correlation. The method
            automatically handles response subscription/unsubscription and cleanup,
            even if timeout or exception occurs.
        """
        method = parse_method(method)

        if not self._connected.is_set():
            self.logger.error(
                f"Cannot send {method.name} request to '{target_device_tag}' on path '{path}': client not connected. "
                f"Solution: Call await client.connect() before making requests."
            )
            raise RuntimeError("Client not connected")

        request_id = self.generate_request_id()

        request_payload = self._payload_handler.create_request_payload(
            method=method, path=path, request_id=request_id, body=value, source=self.identifier
        )

        request_topic = self._topic_manager.build_request_topic(
            target_device_tag=target_device_tag,
            subsystem=subsystem,
            request_id=request_id,
        )
        response_topic = self._topic_manager.build_response_topic(
            request_topic=request_topic
        )

        future = asyncio.get_event_loop().create_future()
        async with self._lock:
            self._pending_requests[request_id] = future

        await self.subscribe(response_topic, qos=qos)
        await self.publish(topic=request_topic, payload=request_payload, qos=qos)

        self.logger.info(
            f"'{Method(method)}' request sent to '{target_device_tag}' on uri-path '{path}'",
            extra=self._extract_extras(request_payload, extra_extras={"topic": request_topic})
        )

        try:
            loop = asyncio.get_running_loop()
            request_init_time = loop.time()
            result = await asyncio.wait_for(future, timeout=timeout or self.timeout)
            request_time = loop.time() - request_init_time
            response_code = ResponseCode(result.get("header", {}).get("response_code", None))
            self.logger.info(
                f"'{Method(method)}' request successful with outcome '{response_code}' to '{target_device_tag}' on uri-path '{path}' taking {request_time * 1000:.0f}ms",
                extra=self._extract_extras(result, extra_extras={"topic": response_topic})
            )
            return result
        except asyncio.TimeoutError:
            timeout_elapsed = loop.time() - request_init_time
            self.logger.error(
                f"'{Method(method)}' request to '{target_device_tag}' on uri-path '{path}' with request_id '{request_id}' "
                f"timed out after {timeout_elapsed:.3f} seconds (timeout={timeout or self.timeout}s). "
                f"Possible causes: target device offline, network issues, device not subscribed to request topic, "
                f"or device taking too long to respond. "
                f"Solution: Verify device is online and subscribed to '{request_topic}', check network connectivity, "
                f"or increase timeout parameter.",
                extra=self._extract_extras(request_payload, extra_extras={
                    "topic": request_topic,
                    "timeout_seconds": timeout or self.timeout,
                    "elapsed_seconds": timeout_elapsed
                })
            )
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path=path,
                detail=f"Request timed out after {timeout_elapsed:.3f} seconds",
                source=self.identifier,
                target=target_device_tag,
            )
        finally:
            await self._client.unsubscribe(response_topic)
            async with self._lock:
                self._pending_requests.pop(request_id, None)


# Example usage of message handlers
if __name__ == "__main__":

    async def main():
        async def request_handler(client, topic: str, payload: Any) -> dict[str, Any]:
            logging.info(f"Handling request: {payload}")
            payload_handler = PayloadHandler()
            return payload_handler.create_response_payload(
                response_code=ResponseCode.CONTENT,
                path=payload["header"]["path"],
                request_id=payload["header"]["request_id"],
                body={"data": [1, 2, 3]},
            )

        async def logging_handler(client, topic: str, payload: Any) -> None:
            logging.debug(f"Logging message: {payload}")

        async def response_handler(client, topic: str, payload: Any) -> None:
            if "response_code" in payload.get("header", {}):
                logging.info(f"Received response: {payload}")

        client = MQTTClient("localhost")
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: "method" in p.get("header", {}),
                process=request_handler,
                propagate=False,
            )
        )
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: True,
                process=logging_handler,
                propagate=True,
            )
        )
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: "response_code" in p.get("header", {}),
                process=response_handler,
                propagate=False,
            )
        )

        await client.connect()
        await client.request("device1", "subsystem1", "path1")
        await asyncio.sleep(5)
        await client.disconnect()

    asyncio.run(main())
