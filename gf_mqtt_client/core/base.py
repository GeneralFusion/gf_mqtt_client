"""
Abstract Base Class for MQTT Clients.

This module provides the foundational abstract base class (MQTTClientBase) that defines
the common interface and shared functionality for both asynchronous and synchronous MQTT
client implementations.

Key Components:
    - MQTTClientBase: Abstract base class defining the client interface
    - MessageLogger: Custom logger adapter with contextual logging support
    - ClientFormatter: Log formatter with extra field support
    - Utility functions: parse_method(), generate_unique_id()

The base class handles:
    - Client configuration and initialization
    - Credential management
    - Shared payload and topic handling utilities
    - Logging infrastructure with contextual information
    - Abstract method definitions for subclass implementation

This design follows the Template Method pattern, where the base class defines the
structure and common operations while subclasses provide specific implementations
for async (aiomqtt) and sync (paho-mqtt) operations.
"""
import uuid
import logging
from abc import ABC, abstractmethod
from typing import Any, Self
from pydantic import SecretStr

from .models import Method
from .payload_handler import PayloadHandler
from .topic_manager import TopicManager


logger = logging.getLogger(__name__)


class ClientFormatter(logging.Formatter):
    """
    Custom log formatter that appends contextual metadata to log messages.

    This formatter extends the standard logging.Formatter to automatically append
    any extra fields passed via the 'extra' parameter as key=value pairs to the
    log message. This is useful for adding structured context like client_id,
    request_id, or correlation_id to log entries.

    Example:
        >>> formatter = ClientFormatter()
        >>> handler.setFormatter(formatter)
        >>> logger.info("Message sent", extra={"client_id": "device-123", "topic": "sensor/data"})
        # Output: "Message sent client_id=device-123 topic=sensor/data"
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record by appending extra fields to the message.

        Args:
            record: LogRecord instance containing the log event information

        Returns:
            Formatted log message string with extra fields appended
        """
        if hasattr(record, 'extra') and isinstance(record.extra, dict):
            extra_info = ' '.join(f"{k}={v}" for k, v in record.extra.items())
            return f"{record.msg} {extra_info}"
        return super().format(record)


class MessageLogger(logging.LoggerAdapter):
    """
    Enhanced logger adapter that provides contextual logging with flexible extra field management.

    This adapter extends logging.LoggerAdapter to provide sophisticated context management
    for log messages. It supports merging, filtering, and automatic injection of contextual
    information like client_id, request_id, and other metadata into all log records.

    Attributes:
        logger: The underlying Logger instance
        extra: Base context dictionary attached to all log records
        merge_extra: If True, merge call-time extras with base extras; if False, replace
        exclude_extras: List of field names to exclude from the extra context

    Example:
        >>> logger = MessageLogger(
        ...     logging.getLogger(__name__),
        ...     extra={"client_id": "device-001"},
        ...     merge_extra=True
        ... )
        >>> logger.info("Connected to broker", extra={"broker": "mqtt.example.com"})
        # Logs with both client_id and broker in the context
    """

    def __init__(
        self,
        logger: logging.Logger,
        extra: dict[str, Any] | None = None,
        merge_extra: bool = False,
        exclude_extras: list[str] | None = None
    ):
        """
        Initialize the MessageLogger adapter.

        Args:
            logger: Base Logger instance to wrap
            extra: Base contextual information to attach to all log records
            merge_extra: If True, merge per-call extras with base extras;
                        if False, per-call extras replace base extras
            exclude_extras: List of field names to filter out from extra context
        """
        super().__init__(logger, extra or {})
        self.logger = logger
        self.extra = extra
        self.merge_extra = merge_extra
        self.exclude_extras = exclude_extras or []

    def process(self, msg, kwargs):
        """
        Process the logging call to inject and manage extra context fields.

        This method is called automatically by the LoggerAdapter before each log
        record is emitted. It handles merging or replacing extra fields and
        applies any exclusion filters.

        Args:
            msg: The log message string
            kwargs: Keyword arguments passed to the logging call

        Returns:
            Tuple of (message, modified_kwargs) ready for the underlying logger
        """
        # Merge or replace extra fields based on configuration
        if self.merge_extra and "extra" in kwargs:
            kwargs["extra"] = {**self.extra, **kwargs["extra"]}
        else:
            kwargs["extra"] = self.extra

        # Apply exclusion filters to remove unwanted fields
        if self.exclude_extras:
            for key in self.exclude_extras:
                kwargs["extra"].pop(key, None)

        return msg, kwargs


def parse_method(method: Any) -> Method:
    """
    Parse and validate a Method from various input formats.

    This utility function converts flexible input types (string, integer, or Method enum)
    into a validated Method enum value. It supports case-insensitive string parsing and
    numeric method codes (1=GET, 2=POST, 3=PUT, 4=DELETE).

    Args:
        method: The method to parse. Can be:
            - Method enum (returned as-is)
            - str: "GET", "POST", "PUT", or "DELETE" (case-insensitive)
            - int: 1 (GET), 2 (POST), 3 (PUT), or 4 (DELETE)

    Returns:
        Validated Method enum value

    Raises:
        ValueError: If the method is invalid or cannot be parsed

    Example:
        >>> parse_method("get")  # Returns Method.GET
        >>> parse_method(2)      # Returns Method.POST
        >>> parse_method(Method.PUT)  # Returns Method.PUT
    """
    # If already a Method enum, return as-is
    if isinstance(method, Method):
        return method

    # Parse string method names (case-insensitive)
    if isinstance(method, str):
        try:
            method = Method[method.upper()]
            logger.debug(f"Parsed string method '{method}' to {method}")
        except KeyError:
            valid_methods = ", ".join([m.name for m in Method])
            logger.error(
                f"Invalid method string: '{method}'. Must be one of: {valid_methods}"
            )
            raise ValueError(
                f"Invalid method: '{method}'. Valid methods are: {valid_methods}"
            )

    # Parse integer method codes
    elif isinstance(method, int):
        try:
            method = Method(method)
            logger.debug(f"Parsed integer method code {method} to {method}")
        except ValueError:
            valid_codes = ", ".join([f"{m.value}={m.name}" for m in Method])
            logger.error(
                f"Invalid method code: {method}. Valid codes are: {valid_codes}"
            )
            raise ValueError(
                f"Invalid method value: {method}. Valid codes are: {valid_codes}"
            )

    # Reject unsupported types
    else:
        logger.error(f"Method must be str, int, or Method enum, got {type(method).__name__}")
        raise ValueError(
            f"Method must be an int or str, got {type(method).__name__}"
        )

    return method


def generate_unique_id(prefix: str | None = "mqtt_client") -> str:
    """
    Generate a globally unique identifier with an optional prefix.

    Creates a UUID4-based unique identifier, optionally prefixed with a descriptive
    string. This is commonly used for MQTT client identifiers to ensure uniqueness
    across multiple client instances.

    Args:
        prefix: Optional prefix string. If None, returns raw UUID.
                Default is "mqtt_client".

    Returns:
        Unique identifier string in format "{prefix}-{uuid}" or just "{uuid}"

    Example:
        >>> generate_unique_id("device")
        "device-a7f3c8d9-1234-5678-9abc-def012345678"
        >>> generate_unique_id(None)
        "a7f3c8d9-1234-5678-9abc-def012345678"
    """
    if prefix is None:
        return str(uuid.uuid4())
    return f"{prefix}-{uuid.uuid4()}"


class MQTTClientBase(ABC):
    """
    Abstract base class for MQTT clients.

    Provides shared functionality for both async and sync implementations:
    - Configuration management
    - Logging utilities
    - Payload and topic handling
    - Message handler management interface
    - Helper methods for string truncation and extras extraction

    Subclasses must implement connection, messaging, and handler operations.
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
        Initialize the MQTT client base.

        Args:
            broker: MQTT broker hostname
            port: MQTT broker port (default depends on implementation)
            timeout: Default timeout for operations in seconds
            identifier: Client identifier (auto-generated if None)
            subscriptions: List of topics to subscribe to on connect
            username: MQTT username for authentication
            password: MQTT password for authentication
            ensure_unique_identifier: If True, append UUID to identifier
            logger: Custom logger adapter (creates default if None)
            qos_default: Default QoS level for publish/subscribe
        """
        self.broker = broker
        self._port = port
        self.timeout = timeout
        self._username = username
        self._password = password

        # Generate or use provided identifier
        if ensure_unique_identifier:
            identifier = generate_unique_id(identifier)
        else:
            identifier = identifier or generate_unique_id()
        self.identifier = identifier

        # Shared components
        self._topic_manager = TopicManager()
        self._payload_handler = PayloadHandler()

        # Configuration
        self.subscriptions = subscriptions or []
        self.qos_default = qos_default

        # Set up logging
        self.logger = logger or MessageLogger(
            logging.getLogger(__name__),
            extra={"client_id": self.identifier},
            merge_extra=True
        )

        self.logger.debug(
            f"Initialized MQTT client to broker {self.broker}:{self._port} "
            f"with identifier '{self.identifier}'"
        )

    def set_credentials(self, username: str, password: str | SecretStr):
        """Set MQTT authentication credentials."""
        self._username = username
        self._password = password
        self.logger.debug(
            "Credentials set for username",
            extra={"username": username, "password": "***"},
        )

    def generate_request_id(self) -> str:
        """Generate a unique request ID."""
        return str(uuid.uuid4())

    def _truncate_str(self, input_string: str, output_length: int = 50) -> str:
        """
        Truncate a string to specified length, appending '...' if truncated.

        Args:
            input_string: String to truncate
            output_length: Maximum length

        Returns:
            Truncated string
        """
        if not isinstance(input_string, str):
            try:
                input_string = str(input_string)
            except Exception:
                return input_string
        if len(input_string) > output_length:
            return input_string[:output_length] + "..."
        return input_string

    def _extract_extras(
        self,
        payload: dict,
        max_len: int = 50,
        extra_extras: dict | None = None
    ) -> dict:
        """
        Extract extra information from the payload for logging.

        Args:
            payload: Message payload dictionary
            max_len: Maximum length for string values
            extra_extras: Additional extras to merge

        Returns:
            Dictionary of extracted fields for logging context
        """
        from .models import MessageType

        def _determine_message_type(payload: dict) -> str | None:
            if "header" in payload and "method" in payload["header"]:
                return MessageType.REQUEST.value
            if "header" in payload and "response_code" in payload["header"]:
                return MessageType.RESPONSE.value
            return MessageType.UNKNOWN.value

        assert max_len > 0, "max_len must be greater than 0"

        header = payload.get("header", {})
        source = header.get("source", "unknown")
        target = header.get("target", "unknown")
        request_id = header.get("request_id", "N/A")
        correlation_id = header.get("correlation_id", None)
        path = header.get("path", None)
        response_code = header.get("response_code", None)
        message_type = _determine_message_type(payload)

        extra_new = {}
        extra_new["source"] = source
        extra_new["target"] = target
        extra_new["request_id"] = request_id
        extra_new["correlation_id"] = correlation_id
        extra_new["path"] = path
        extra_new["message_type"] = message_type
        extra_new["response_code"] = response_code

        # Post-processing of extra fields - truncate strings
        for k, v in extra_new.items():
            if isinstance(v, str) and len(v) > max_len:
                extra_new[k] = self._truncate_str(v, output_length=max_len)

        # Remove keys with None values
        extra_new = {k: v for k, v in extra_new.items() if v is not None}

        return {**extra_new, **(extra_extras or {})}

    # Abstract methods that subclasses must implement

    @abstractmethod
    def connect(self):
        """
        Connect to the MQTT broker.

        Implementation should:
        - Establish connection to broker
        - Subscribe to default topics
        - Set up message handlers
        - Mark client as connected
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Disconnect from the MQTT broker.

        Implementation should:
        - Stop message processing
        - Close connection
        - Clean up resources
        - Clear pending requests
        """
        pass

    @abstractmethod
    def publish(self, topic: str, payload: Any, qos: int | None = None):
        """
        Publish a message to a topic.

        Args:
            topic: MQTT topic to publish to
            payload: Message payload (will be JSON-encoded)
            qos: Quality of Service level (uses qos_default if None)
        """
        pass

    @abstractmethod
    def subscribe(self, topic: str, qos: int | None = None):
        """
        Subscribe to an MQTT topic.

        Args:
            topic: MQTT topic pattern to subscribe to
            qos: Quality of Service level (uses qos_default if None)
        """
        pass

    @abstractmethod
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
        Send a request and wait for response.

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
        pass

    @abstractmethod
    def add_message_handler(self, handler):
        """
        Add a message handler.

        Args:
            handler: Handler implementing MessageHandlerProtocol
        """
        pass

    @abstractmethod
    def remove_message_handler(self, handler):
        """
        Remove a message handler.

        Args:
            handler: Handler to remove
        """
        pass

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if client is currently connected to broker."""
        pass
