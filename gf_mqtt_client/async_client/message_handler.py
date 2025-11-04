"""
Asynchronous Message Handler System for MQTT Client.

This module provides the async handler architecture for processing incoming MQTT messages.
It implements the Chain of Responsibility pattern, allowing multiple handlers to process
messages in sequence with configurable propagation control.

Key Components:
    - MessageHandlerProtocol: Protocol defining the handler interface
    - MessageHandlerBase: Base implementation for custom async handlers
    - ResponseHandlerBase: Specialized handler for response messages (response_code)
    - RequestHandlerBase: Specialized handler for request messages (method)
    - Default handlers: Pre-configured handlers for common scenarios

Handler Architecture:
    Handlers are organized in a chain and executed in registration order. Each handler:
    1. Checks if it can handle the message via can_handle()
    2. Processes the message via handle() if applicable
    3. Controls propagation via the propagate flag

    If propagate=False, processing stops after that handler. Otherwise, the message
    continues to the next handler in the chain.

Exception Handling:
    - ResponseHandlerBase: Automatically raises typed exceptions for error response codes
    - RequestHandlerBase: Does not raise exceptions (propagates errors to caller)
    - Custom handlers: Can set raise_exceptions flag to control behavior

Example:
    >>> # Custom async request handler
    >>> async def process_temp_request(client, topic, payload):
    ...     return {"temperature": 23.5, "unit": "celsius"}
    >>>
    >>> handler = MessageHandlerBase(
    ...     can_handle=lambda c, t, p: p.get("header", {}).get("path") == "/temperature",
    ...     process=process_temp_request,
    ...     propagate=False  # Stop after this handler
    ... )
    >>> await client.add_message_handler(handler)

See Also:
    - sync_client.message_handler: Synchronous version for blocking operations
    - core.message_handler: Shared utilities for response validation
"""
from typing import Any, Awaitable, Callable, Dict, Optional, runtime_checkable, Protocol
import logging

from ..core.message_handler import handle_response_with_exception

# Type placeholder for MQTTClient to avoid circular imports
MQTTClient = Any

logger = logging.getLogger(__name__)


# === Async Handler Protocol ===

@runtime_checkable
class MessageHandlerProtocol(Protocol):
    """
    Protocol defining the interface for asynchronous message handlers.

    This protocol uses Python's structural subtyping (PEP 544) to define the
    required interface for message handlers. Any class implementing these methods
    will be accepted as a valid handler, enabling duck typing while maintaining
    type safety.

    Required Methods:
        can_handle: Determines if handler should process a message
        handle: Processes the message asynchronously
        propagate: Property controlling whether message continues to next handler

    The protocol is marked as @runtime_checkable, allowing isinstance() checks
    at runtime for validation when handlers are registered.

    Example Implementation:
        >>> class CustomHandler:
        ...     def can_handle(self, client, topic, payload):
        ...         return payload.get("type") == "sensor_data"
        ...
        ...     async def handle(self, client, topic, payload):
        ...         # Process sensor data
        ...         return {"status": "processed"}
        ...
        ...     @property
        ...     def propagate(self):
        ...         return False  # Stop after this handler
        >>>
        >>> handler = CustomHandler()
        >>> isinstance(handler, MessageHandlerProtocol)  # True
        >>> await client.add_message_handler(handler)

    Note:
        This is an async protocol - handle() must be an async method returning
        an awaitable. For synchronous handlers, see sync_client.message_handler.
    """
    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        """
        Determine if this handler should process the given message.

        Args:
            client: The MQTT client instance
            topic: MQTT topic the message was received on
            payload: Decoded message payload (dict)

        Returns:
            True if handler should process this message, False otherwise
        """
        ...

    async def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process the message asynchronously.

        Args:
            client: The MQTT client instance
            topic: MQTT topic the message was received on
            payload: Decoded message payload (dict)

        Returns:
            Processed result (dict) or None if no result to return
        """
        ...

    @property
    def propagate(self) -> bool:
        """
        Control whether message should continue to next handler.

        Returns:
            True to allow propagation, False to stop after this handler
        """
        ...


# === Base Handler (Async) ===

class MessageHandlerBase:
    """
    Base implementation of the async message handler pattern.

    This class provides a flexible, function-based approach to creating message handlers
    without requiring inheritance. It accepts callable functions for can_handle and process
    logic, making it easy to create handlers inline or from existing functions.

    Attributes:
        _can_handle: Function determining if handler processes a message
        _process: Async function performing the message processing
        _propagate: Whether to allow message to continue to next handler
        _raise_exceptions: Whether to validate responses and raise exceptions for errors

    Args:
        can_handle: Callable taking (client, topic, payload) returning bool
        process: Async callable taking (client, topic, payload) returning optional dict
        propagate: If True, message continues to next handler. If False, stops here. (default: True)
        raise_exceptions: If True, validates response codes and raises typed exceptions (default: False)

    Example:
        >>> # Simple handler with lambda
        >>> handler = MessageHandlerBase(
        ...     can_handle=lambda c, t, p: "temperature" in p.get("body", {}),
        ...     process=lambda c, t, p: {"status": "logged"},
        ...     propagate=True
        ... )
        >>>
        >>> # Handler with async function
        >>> async def process_sensor_data(client, topic, payload):
        ...     data = payload.get("body", {})
        ...     await client.publish("processed/data", {"result": data})
        ...     return {"status": "processed"}
        >>>
        >>> handler = MessageHandlerBase(
        ...     can_handle=lambda c, t, p: p.get("header", {}).get("path") == "/sensor",
        ...     process=process_sensor_data,
        ...     propagate=False
        ... )

    Note:
        For response handling with automatic exception raising, consider using
        ResponseHandlerBase instead, which sets raise_exceptions=True by default.
    """
    def __init__(
        self,
        can_handle: Callable[[MQTTClient, str, Dict[str, Any]], bool],
        process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]],
        propagate: bool = True,
        raise_exceptions: bool = False,
    ):
        """
        Initialize the message handler with processing functions.

        Args:
            can_handle: Function to determine if handler should process message
            process: Async function to process the message
            propagate: Whether to continue to next handler (default: True)
            raise_exceptions: Whether to validate and raise exceptions (default: False)
        """
        self._can_handle = can_handle
        self._process = process
        self._propagate = propagate
        self._raise_exceptions = raise_exceptions

    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        """
        Check if this handler should process the message.

        Delegates to the can_handle function provided during initialization.

        Args:
            client: The MQTT client instance
            topic: MQTT topic the message was received on
            payload: Decoded message payload (dict)

        Returns:
            True if handler should process this message, False otherwise
        """
        return self._can_handle(client, topic, payload)

    async def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process the message asynchronously.

        If raise_exceptions is True, validates response codes and raises typed exceptions
        for error responses (4xx, 5xx) before processing. Then delegates to the process
        function provided during initialization.

        Args:
            client: The MQTT client instance
            topic: MQTT topic the message was received on
            payload: Decoded message payload (dict)

        Returns:
            Result from the process function, or None

        Raises:
            ResponseException: If raise_exceptions=True and payload contains error response code
        """
        if self._raise_exceptions:
            handle_response_with_exception(client, topic, payload)
        return await self._process(client, topic, payload)

    @property
    def propagate(self) -> bool:
        """
        Get the propagation flag for this handler.

        Returns:
            True if message should continue to next handler, False to stop here
        """
        return self._propagate

    def _truncate_payload(self, payload: Any, output_length: int = 50) -> str:
        """
        Truncate payload to specified length for logging purposes.

        Converts payload to string if necessary and truncates to avoid
        excessive log output.

        Args:
            payload: The payload to truncate (any type)
            output_length: Maximum length before truncation (default: 50)

        Returns:
            Truncated string representation with "..." suffix if truncated

        Example:
            >>> handler._truncate_payload({"long": "payload" * 100}, 20)
            "{'long': 'payloadpa..."
        """
        if not isinstance(payload, str):
            try:
                payload = str(payload)
            except Exception:
                return "<unreadable payload>"

        if len(payload) > output_length:
            return payload[:output_length] + "..."
        return payload


# === Response Handler (Async) ===

class ResponseHandlerBase(MessageHandlerBase):
    """
    Specialized async handler for response messages (messages with response_code).

    This handler automatically detects response messages by checking for the
    'response_code' field in the message header. It's designed for handling
    responses to previous requests in the request-response pattern.

    Key Features:
        - Auto-detects response messages via response_code field
        - Raises typed exceptions by default for error response codes (4xx, 5xx)
        - Does not propagate by default (stops after handling response)
        - Suitable for implementing custom response processing logic

    Default Behavior:
        - can_handle: Returns True if payload.header.response_code exists
        - propagate: False (responses typically don't need further processing)
        - raise_exceptions: True (converts error codes to typed exceptions)

    Args:
        process: Async function to process the response message
        propagate: Whether to continue to next handler (default: False)
        raise_exceptions: Whether to raise exceptions for error codes (default: True)

    Example:
        >>> async def log_successful_response(client, topic, payload):
        ...     code = payload["header"]["response_code"]
        ...     logger.info(f"Request succeeded with code {code}")
        ...     return payload
        >>>
        >>> handler = ResponseHandlerBase(
        ...     process=log_successful_response,
        ...     propagate=False,
        ...     raise_exceptions=True
        ... )
        >>> await client.add_message_handler(handler)

    Note:
        If raise_exceptions=True, error response codes will raise typed exceptions
        (NotFoundResponse for 404, InternalServerErrorResponse for 500, etc.) before
        the process function is called. Only success codes (2xx) reach the process function.
    """
    def __init__(
        self,
        process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Dict[str, Any]]],
        propagate: bool = False,
        raise_exceptions: bool = True
    ):
        """
        Initialize the response handler.

        Args:
            process: Async function to process response messages
            propagate: Whether to continue to next handler (default: False)
            raise_exceptions: Whether to raise exceptions for error codes (default: True)
        """
        def can_handle_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
            """Check if payload is a response message (has response_code)."""
            return "response_code" in payload.get("header", {})

        super().__init__(
            can_handle=can_handle_response,
            process=process,
            propagate=propagate,
            raise_exceptions=raise_exceptions
        )


# === Request Handler (Async) ===

class RequestHandlerBase(MessageHandlerBase):
    """
    Specialized async handler for request messages (messages with method field).

    This handler automatically detects request messages by checking for the
    'method' field in the message header. It's designed for implementing
    server-side request handlers that respond to incoming requests.

    Key Features:
        - Auto-detects request messages via method field (GET, POST, PUT, DELETE)
        - Does NOT raise exceptions (returns errors to caller)
        - Propagates by default (allows logging/monitoring handlers downstream)
        - Suitable for implementing request processing and response generation

    Default Behavior:
        - can_handle: Returns True if payload.header.method exists
        - propagate: True (allows other handlers to log/monitor requests)
        - raise_exceptions: False (request handlers generate responses, not exceptions)

    Args:
        process: Async function to process the request and generate response
        propagate: Whether to continue to next handler (default: True)

    Example:
        >>> async def handle_sensor_request(client, topic, payload):
        ...     method = payload["header"]["method"]
        ...     path = payload["header"]["path"]
        ...
        ...     if path == "/temperature":
        ...         # Return response payload
        ...         return {
        ...             "header": {
        ...                 "response_code": 205,  # CONTENT
        ...                 "path": path,
        ...                 "request_id": payload["header"]["request_id"]
        ...             },
        ...             "body": {"value": 23.5, "unit": "celsius"}
        ...         }
        >>>
        >>> handler = RequestHandlerBase(
        ...     process=handle_sensor_request,
        ...     propagate=True
        ... )
        >>> await client.add_message_handler(handler)

    Note:
        Request handlers should generate proper response payloads with response_code,
        not raise exceptions. Exceptions in the process function will be caught by
        the message loop and logged, but won't be sent back to the requester.
    """
    def __init__(
        self,
        process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Dict[str, Any]]],
        propagate: bool = True
    ):
        """
        Initialize the request handler.

        Args:
            process: Async function to process requests and generate responses
            propagate: Whether to continue to next handler (default: True)
        """
        def can_handle_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
            """Check if payload is a request message (has method)."""
            return "method" in payload.get("header", {})

        super().__init__(
            can_handle=can_handle_request,
            process=process,
            propagate=propagate,
            raise_exceptions=False
        )


# === Default Handlers (Async) ===

class ResponseHandlerDefault(ResponseHandlerBase):
    """
    Default async handler for response messages with automatic exception raising.

    This handler is automatically registered by the async MQTT client during connection.
    It provides basic response handling with exception raising for error codes.

    Behavior:
        - Handles all response messages (messages with response_code)
        - Logs response receipt at DEBUG level
        - Raises typed exceptions for error response codes (4xx, 5xx)
        - Propagates to allow other handlers to process responses
        - Returns payload unchanged for success codes (2xx)

    Configuration:
        - propagate: True (allows logging/monitoring handlers)
        - raise_exceptions: True (converts error codes to exceptions)

    Usage:
        This handler is automatically added by client.connect(). No manual
        registration is required. If you want to disable automatic exception
        raising, unregister this handler and add a custom one.

    Example:
        >>> # Handler is auto-registered, but you can add it manually:
        >>> await client.add_message_handler(ResponseHandlerDefault())
        >>>
        >>> # To disable exception raising, remove default and add custom:
        >>> await client.remove_message_handler(default_handler)
        >>> await client.add_message_handler(
        ...     ResponseHandlerBase(
        ...         process=lambda c, t, p: p,
        ...         raise_exceptions=False
        ...     )
        ... )

    Note:
        This handler raises exceptions BEFORE returning the payload. If you want
        to handle error responses without exceptions, replace this handler with
        a custom one that has raise_exceptions=False.
    """
    def __init__(self):
        """Initialize the default response handler with exception raising enabled."""
        async def process_default_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            """Log and return response payload."""
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


class RequestHandlerDefault(RequestHandlerBase):
    """
    Default async handler for request messages with basic logging.

    This handler can be used as a fallback for unhandled requests or for
    logging all incoming requests. It does not generate responses - you must
    register additional handlers to actually process requests.

    Behavior:
        - Handles all request messages (messages with method field)
        - Logs request receipt at DEBUG level
        - Does NOT generate responses (returns payload unchanged)
        - Propagates to allow other handlers to process requests
        - Does NOT raise exceptions

    Usage:
        Typically registered alongside request-processing handlers to log
        all requests, or as a catch-all for debugging.

    Example:
        >>> # Add logging for all requests
        >>> await client.add_message_handler(RequestHandlerDefault())
        >>>
        >>> # Add actual request processor
        >>> async def process_requests(client, topic, payload):
        ...     # Generate and publish response
        ...     response = generate_response(payload)
        ...     response_topic = topic.replace("/request/", "/response/")
        ...     await client.publish(response_topic, response)
        ...     return response
        >>>
        >>> await client.add_message_handler(
        ...     RequestHandlerBase(process=process_requests, propagate=True)
        ... )

    Note:
        This handler does NOT generate responses. It's primarily for logging.
        You must register additional handlers to actually process and respond
        to requests.
    """
    def __init__(self):
        """Initialize the default request handler with logging only."""
        async def process_default_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            """Log and return request payload."""
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
    "MessageHandlerProtocol",
    "MessageHandlerBase",
    "ResponseHandlerBase",
    "RequestHandlerBase",
    "ResponseHandlerDefault",
    "RequestHandlerDefault",
]
