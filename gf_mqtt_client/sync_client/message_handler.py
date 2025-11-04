"""
Synchronous Message Handler System for MQTT Client.

This module provides the synchronous handler architecture for processing incoming MQTT
messages in the sync client. It implements the Chain of Responsibility pattern using
pure blocking I/O without async/await, making it suitable for traditional synchronous
applications and environments where asyncio is not available.

Key Components:
    - SyncMessageHandlerProtocol: Protocol defining the synchronous handler interface
    - SyncMessageHandlerBase: Base implementation for custom sync handlers
    - SyncResponseHandlerBase: Specialized handler for response messages (response_code)
    - SyncRequestHandlerBase: Specialized handler for request messages (method)
    - Default handlers: Pre-configured handlers for common scenarios

Handler Architecture:
    Handlers are organized in a chain and executed in registration order. Each handler:
    1. Checks if it can handle the message via can_handle()
    2. Processes the message via handle() if applicable (synchronously, no await)
    3. Controls propagation via the propagate flag

    Handlers execute in paho's network thread, so they must be fast and non-blocking
    to avoid impacting message throughput.

Threading Considerations:
    **CRITICAL**: All handlers run in paho-mqtt's background network thread. This means:
    - Handlers must be fast (ideally < 10ms) to avoid blocking message processing
    - Avoid blocking I/O operations (file access, network calls, database queries)
    - Do NOT call client.publish() with wait=True from handlers (causes deadlock)
    - For heavy processing, queue work and process in a separate worker thread
    - Thread safety: Be careful with shared mutable state

Exception Handling:
    - SyncResponseHandlerBase: Automatically raises typed exceptions for error response codes
    - SyncRequestHandlerBase: Does not raise exceptions (propagates errors to caller)
    - Custom handlers: Can set raise_exceptions flag to control behavior

Example:
    >>> # Simple sync handler (runs in network thread)
    >>> def process_temp_request(client, topic, payload):
    ...     # Fast, non-blocking processing only!
    ...     return {"temperature": 23.5, "unit": "celsius"}
    >>>
    >>> handler = SyncMessageHandlerBase(
    ...     can_handle=lambda c, t, p: p.get("header", {}).get("path") == "/temperature",
    ...     process=process_temp_request,
    ...     propagate=False
    ... )
    >>> client.add_message_handler(handler)
    >>>
    >>> # Handler with heavy processing (use worker thread)
    >>> import queue
    >>> work_queue = queue.Queue()
    >>>
    >>> def queue_for_processing(client, topic, payload):
    ...     work_queue.put((client, topic, payload))  # Fast queuing
    ...     return None
    >>>
    >>> handler = SyncMessageHandlerBase(
    ...     can_handle=lambda c, t, p: True,
    ...     process=queue_for_processing,
    ...     propagate=True
    ... )

See Also:
    - async_client.message_handler: Async/await version for non-blocking operations
    - core.message_handler: Shared utilities for response validation
"""
from typing import Any, Callable, Dict, Optional, runtime_checkable, Protocol
import logging

from ..core.message_handler import handle_response_with_exception

# Type placeholder for MQTTClient to avoid circular imports
MQTTClient = Any

logger = logging.getLogger(__name__)


# === Sync Handler Protocol ===

@runtime_checkable
class SyncMessageHandlerProtocol(Protocol):
    """
    Protocol defining the interface for synchronous message handlers.

    This protocol uses Python's structural subtyping (PEP 544) to define the
    required interface for synchronous message handlers. Unlike the async version,
    all methods are purely synchronous with no async/await.

    Required Methods:
        can_handle: Determines if handler should process a message
        handle: Processes the message synchronously (NO async/await)
        propagate: Property controlling whether message continues to next handler

    The protocol is marked as @runtime_checkable, allowing isinstance() checks
    at runtime for validation when handlers are registered.

    Threading Warning:
        Handlers implementing this protocol will be called in paho-mqtt's network
        thread. They MUST be fast and non-blocking to avoid degrading message
        processing performance.

    Example Implementation:
        >>> class CustomHandler:
        ...     def can_handle(self, client, topic, payload):
        ...         return payload.get("type") == "sensor_data"
        ...
        ...     def handle(self, client, topic, payload):
        ...         # Process sensor data (must be fast!)
        ...         return {"status": "processed"}
        ...
        ...     @property
        ...     def propagate(self):
        ...         return False
        >>>
        >>> handler = CustomHandler()
        >>> isinstance(handler, SyncMessageHandlerProtocol)  # True
        >>> client.add_message_handler(handler)

    Note:
        This is a synchronous protocol - handle() must be a regular method, NOT async.
        For async handlers, see async_client.message_handler.
    """
    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        """Determine if this handler should process the given message."""
        ...

    def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process the message synchronously (no async/await)."""
        ...

    @property
    def propagate(self) -> bool:
        """Control whether message should continue to next handler."""
        ...


# === Base Handler (Sync) ===

class SyncMessageHandlerBase:
    """
    Base implementation of the synchronous message handler pattern.

    This class provides a flexible, function-based approach to creating message handlers
    without requiring inheritance. It accepts callable functions for can_handle and process
    logic, making it easy to create handlers inline or from existing functions.

    **Threading Warning**: Handlers run in paho's network thread and must be fast!

    Attributes:
        _can_handle: Function determining if handler processes a message
        _process: Synchronous function performing the message processing
        _propagate: Whether to allow message to continue to next handler
        _raise_exceptions: Whether to validate responses and raise exceptions for errors

    Args:
        can_handle: Callable taking (client, topic, payload) returning bool
        process: Synchronous callable taking (client, topic, payload) returning optional dict
        propagate: If True, message continues to next handler. If False, stops here. (default: True)
        raise_exceptions: If True, validates response codes and raises typed exceptions (default: False)

    Example:
        >>> # Simple handler with lambda
        >>> handler = SyncMessageHandlerBase(
        ...     can_handle=lambda c, t, p: "temperature" in p.get("body", {}),
        ...     process=lambda c, t, p: {"status": "logged"},
        ...     propagate=True
        ... )
        >>>
        >>> # Handler with function
        >>> def process_sensor_data(client, topic, payload):
        ...     data = payload.get("body", {})
        ...     # Use wait=False to avoid deadlock when publishing from handler!
        ...     client.publish("processed/data", {"result": data}, wait=False)
        ...     return {"status": "processed"}
        >>>
        >>> handler = SyncMessageHandlerBase(
        ...     can_handle=lambda c, t, p: p.get("header", {}).get("path") == "/sensor",
        ...     process=process_sensor_data,
        ...     propagate=False
        ... )

    Performance:
        Handlers execute in paho's network thread. Keep processing < 10ms to avoid
        blocking message reception. For heavy work, queue tasks for worker threads.

    Note:
        For response handling with automatic exception raising, consider using
        SyncResponseHandlerBase instead, which sets raise_exceptions=True by default.
    """
    def __init__(
        self,
        can_handle: Callable[[MQTTClient, str, Dict[str, Any]], bool],
        process: Callable[[MQTTClient, str, Dict[str, Any]], Optional[Dict[str, Any]]],
        propagate: bool = True,
        raise_exceptions: bool = False,
    ):
        self._can_handle = can_handle
        self._process = process
        self._propagate = propagate
        self._raise_exceptions = raise_exceptions

    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        """Check if this handler should process the message."""
        return self._can_handle(client, topic, payload)

    def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process the message synchronously.

        If raise_exceptions is True, validates response codes and raises typed exceptions
        for error responses (4xx, 5xx) before processing.

        Raises:
            ResponseException: If raise_exceptions=True and payload contains error response code
        """
        if self._raise_exceptions:
            handle_response_with_exception(client, topic, payload)
        return self._process(client, topic, payload)

    @property
    def propagate(self) -> bool:
        """Get the propagation flag for this handler."""
        return self._propagate

    def _truncate_payload(self, payload: Any, output_length: int = 50) -> str:
        """Truncate payload to specified length for logging purposes."""
        if not isinstance(payload, str):
            try:
                payload = str(payload)
            except Exception:
                return "<unreadable payload>"

        if len(payload) > output_length:
            return payload[:output_length] + "..."
        return payload


# === Response Handler (Sync) ===

class SyncResponseHandlerBase(SyncMessageHandlerBase):
    """
    Specialized synchronous handler for response messages (messages with response_code).

    Auto-detects response messages by checking for 'response_code' field in the header.
    Raises typed exceptions by default for error response codes (4xx, 5xx). Runs in
    paho's network thread - must be fast!

    Default Behavior:
        - can_handle: Returns True if payload.header.response_code exists
        - propagate: False (responses typically don't need further processing)
        - raise_exceptions: True (converts error codes to typed exceptions)

    Note:
        If raise_exceptions=True, error response codes will raise typed exceptions
        before the process function is called. Only success codes (2xx) reach process.
    """
    def __init__(
        self,
        process: Callable[[MQTTClient, str, Dict[str, Any]], Dict[str, Any]],
        propagate: bool = False,
        raise_exceptions: bool = True
    ):
        def can_handle_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
            return "response_code" in payload.get("header", {})

        super().__init__(
            can_handle=can_handle_response,
            process=process,
            propagate=propagate,
            raise_exceptions=raise_exceptions
        )


# === Request Handler (Sync) ===

class SyncRequestHandlerBase(SyncMessageHandlerBase):
    """
    Specialized synchronous handler for request messages (messages with method field).

    Auto-detects request messages by checking for 'method' field in the header.
    Does NOT raise exceptions (returns errors to caller). Suitable for implementing
    server-side request handlers. Runs in paho's network thread - must be fast!

    Default Behavior:
        - can_handle: Returns True if payload.header.method exists
        - propagate: True (allows other handlers to log/monitor requests)
        - raise_exceptions: False (request handlers generate responses, not exceptions)

    Performance Warning:
        For complex request processing, queue the work and return quickly. Process
        in a worker thread to avoid blocking paho's network thread.
    """
    def __init__(
        self,
        process: Callable[[MQTTClient, str, Dict[str, Any]], Dict[str, Any]],
        propagate: bool = True
    ):
        def can_handle_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
            return "method" in payload.get("header", {})

        super().__init__(
            can_handle=can_handle_request,
            process=process,
            propagate=propagate,
            raise_exceptions=False
        )


# === Default Handlers (Sync) ===

class SyncResponseHandlerDefault(SyncResponseHandlerBase):
    """
    Default synchronous handler for response messages with automatic exception raising.

    This handler is automatically registered by the sync MQTT client during connection.
    It logs responses at DEBUG level and raises typed exceptions for error codes.
    Runs in paho's network thread.
    """
    def __init__(self):
        def process_default_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logger.debug(
                f"Response received on topic '{topic}': {self._truncate_payload(payload)}",
                extra={
                    "topic": topic,
                    "client_id": client.identifier,
                    "response_code": payload.get("header", {}).get("response_code"),
                    "request_id": payload.get("header", {}).get("request_id")
                }
            )
            return payload

        super().__init__(
            process=process_default_response, propagate=True, raise_exceptions=True
        )


class SyncRequestHandlerDefault(SyncRequestHandlerBase):
    """
    Default synchronous handler for request messages with basic logging.

    This handler can be used as a fallback for unhandled requests or for logging
    all incoming requests. It does not generate responses. Runs in paho's network thread.
    """
    def __init__(self):
        def process_default_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logger.debug(
                f"Request received on topic '{topic}': {self._truncate_payload(payload)}",
                extra={
                    "topic": topic,
                    "client_id": client.identifier,
                    "method": payload.get("header", {}).get("method"),
                    "path": payload.get("header", {}).get("path"),
                    "request_id": payload.get("header", {}).get("request_id")
                }
            )
            return payload

        super().__init__(process=process_default_request, propagate=True)


__all__ = [
    "SyncMessageHandlerProtocol",
    "SyncMessageHandlerBase",
    "SyncResponseHandlerBase",
    "SyncRequestHandlerBase",
    "SyncResponseHandlerDefault",
    "SyncRequestHandlerDefault",
]
