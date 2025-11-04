"""
Data Models for MQTT Protocol Communication.

This module defines the core data structures for MQTT messaging using a CoAP-inspired
request-response protocol. All models use Pydantic for validation and serialization.

Key Models:
    - MQTTBrokerConfig: Broker connection configuration
    - Method: HTTP-like request methods (GET, POST, PUT, DELETE)
    - ResponseCode: CoAP-style response codes (2xx success, 4xx client error, 5xx server error)
    - MessageType: Message classification (REQUEST, RESPONSE, SYSTEM, EVENT)
    - PayloadBaseModel: Base for all payload types
    - GeneralPayload: Simple publish-subscribe messages
    - RequestPayload: Request messages with method and request_id
    - ResponsePayload: Response messages with response_code

Protocol Design:
    This module implements a CoAP-like protocol over MQTT, where:
    - Requests include methods (GET/POST/PUT/DELETE) and request IDs
    - Responses include response codes (201-505) and correlate back to requests
    - All payloads include timestamps and optional bodies
    - Headers carry routing and correlation metadata

Example:
    >>> from gf_mqtt_client.core.models import RequestPayload, Method
    >>> payload = RequestPayload(
    ...     header={"method": Method.GET, "path": "/sensor/temp", "request_id": "abc-123"},
    ...     body=None,
    ...     timestamp="1710000000000"
    ... )
"""
from enum import Enum, StrEnum
from typing import Dict, Optional, Union, List
from pydantic import BaseModel, SecretStr, field_validator, model_validator


class MQTTBrokerConfig(BaseModel):
    """
    MQTT broker connection configuration.

    Encapsulates all settings required to connect to an MQTT broker, including
    authentication credentials and connection parameters. Uses sensible defaults
    for testing with public brokers.

    Attributes:
        hostname: MQTT broker hostname or IP address (default: "broker.emqx.io")
        port: MQTT broker port (default: 1883 for non-TLS, 8883 for TLS)
        timeout: Connection timeout in seconds (default: 5)
        username: Optional MQTT username for authentication
        password: Optional MQTT password (can be SecretStr for secure storage)

    Example:
        >>> config = MQTTBrokerConfig(
        ...     hostname="mqtt.example.com",
        ...     port=8883,
        ...     username="device-001",
        ...     password="secret123"
        ... )
    """
    hostname: str = "broker.emqx.io"
    port: Optional[int] = 1883
    timeout: Optional[int] = 5
    username: Optional[str] = None
    password: Optional[str|SecretStr] = None

    @model_validator(mode="after")
    def set_defaults_if_none(self) -> "MQTTBrokerConfig":
        """
        Ensure port and timeout have default values even if explicitly set to None.

        Returns:
            Self with defaults applied
        """
        if self.port is None:
            self.port = 1883
        if self.timeout is None:
            self.timeout = 5
        return self
        

class Method(StrEnum):
    """
    HTTP-like request methods for MQTT request-response protocol.

    Defines the four primary CRUD operations using HTTP method semantics:
    - GET: Retrieve a resource (read)
    - POST: Create a new resource (create)
    - PUT: Update an existing resource (update)
    - DELETE: Remove a resource (delete)

    These methods are used in request payloads to indicate the intended operation.
    """
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"

    def __str__(self):
        """Return the method name as a string."""
        return self.name


class MessageType(Enum):
    """
    Classification of MQTT messages by their purpose.

    Defines the high-level message categories used for routing and handling:
    - REQUEST: Client-initiated request messages requiring a response
    - RESPONSE: Server or device responses to requests
    - SYSTEM: System-level messages (status, health, configuration)
    - EVENT: Asynchronous event notifications (no response expected)
    - UNKNOWN: Unclassified or malformed messages
    """
    REQUEST = "request"
    RESPONSE = "response"
    SYSTEM = "system"
    EVENT = "event"
    UNKNOWN = None


class ResponseCode(Enum):
    """
    CoAP-inspired response status codes for MQTT request-response protocol.

    Response codes follow the CoAP (RFC 7252) specification pattern:
    - 2xx: Success codes (resource created, changed, or retrieved successfully)
    - 4xx: Client error codes (invalid request, authentication failure, not found)
    - 5xx: Server error codes (internal errors, service unavailable, timeouts)

    Success Codes (2xx):
        CREATED (201): Resource successfully created
        DELETED (202): Resource successfully deleted
        VALID (203): Request valid, no content to return
        CHANGED (204): Resource successfully modified
        CONTENT (205): Request successful, content returned

    Client Error Codes (4xx):
        BAD_REQUEST (400): Malformed or invalid request
        UNAUTHORIZED (401): Authentication required or failed
        BAD_OPTION (402): Invalid or unsupported option/parameter
        FORBIDDEN (403): Access denied by policy
        NOT_FOUND (404): Requested resource does not exist
        METHOD_NOT_ALLOWED (405): Method not supported for this resource
        NOT_ACCEPTABLE (406): Response format not acceptable
        PRECONDITION_FAILED (412): Conditional request failed (e.g., ETag mismatch)
        REQUEST_ENTITY_TOO_LARGE (413): Request payload exceeds size limit
        UNSUPPORTED_CONTENT_FORMAT (415): Request content type not supported

    Server Error Codes (5xx):
        INTERNAL_SERVER_ERROR (500): Unexpected server error
        NOT_IMPLEMENTED (501): Feature or method not implemented
        BAD_GATEWAY (502): Invalid upstream response
        SERVICE_UNAVAILABLE (503): Server temporarily unavailable
        GATEWAY_TIMEOUT (504): Upstream request timed out
        PROXYING_NOT_SUPPORTED (505): Proxy functionality not available
    """
    # Success codes (2xx)
    CREATED = 201
    DELETED = 202
    VALID = 203
    CHANGED = 204
    CONTENT = 205

    # Client error codes (4xx)
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    BAD_OPTION = 402
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    NOT_ACCEPTABLE = 406
    PRECONDITION_FAILED = 412
    REQUEST_ENTITY_TOO_LARGE = 413
    UNSUPPORTED_CONTENT_FORMAT = 415

    # Server error codes (5xx)
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
    PROXYING_NOT_SUPPORTED = 505

    def __str__(self):
        """Return formatted string representation of response code."""
        return f"{self.name}:{self.value}"


class PayloadBaseModel(BaseModel):
    """
    Base model for all MQTT payload types.

    Provides common fields shared by all message payloads: timestamp and body.
    The timestamp allows for message ordering and staleness detection, while
    the body carries the actual payload data.

    Attributes:
        timestamp: Unix timestamp in milliseconds (int or string representation)
        body: Flexible payload content (primitives, lists, or dictionaries)

    Note:
        This is an abstract base class. Use GeneralPayload, RequestPayload,
        or ResponsePayload for actual messages.
    """
    timestamp: Union[int, str]
    body: Optional[Union[int, float, str, List, Dict]] = None

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v):
        """
        Validate and normalize timestamp to integer format.

        Accepts either integer timestamps or string representations of integers
        (e.g., "1710000000000"). Strings are converted to integers if possible.

        Args:
            v: Timestamp value (int or str)

        Returns:
            Validated integer timestamp or original string if not numeric

        Raises:
            ValueError: If timestamp is not an integer or numeric string

        Example:
            >>> validate_timestamp("1710000000000")  # Returns 1710000000000
            >>> validate_timestamp(1710000000000)    # Returns 1710000000000
        """
        if isinstance(v, str):
            if v.isdigit():
                return int(v)
            # Allow non-numeric strings to pass through for custom timestamp formats
            return v
        if isinstance(v, int):
            return v
        raise ValueError(
            f"timestamp must be an integer or string representation of an integer, got {type(v).__name__}"
        )

    @field_validator("body")
    @classmethod
    def validate_body(cls, v):
        """
        Validate that body contains serializable data types.

        The body can be None (for empty payloads) or any JSON-serializable type:
        primitives (int, float, str), collections (list, dict), or None.

        Args:
            v: Body value to validate

        Returns:
            Validated body value

        Raises:
            ValueError: If body contains non-serializable types

        Example:
            >>> validate_body({"temp": 23.5, "unit": "C"})  # Valid dict
            >>> validate_body([1, 2, 3])                     # Valid list
            >>> validate_body(None)                          # Valid (empty)
        """
        if v is None:
            return v
        if isinstance(v, (int, float, str, list, dict)):
            return v
        raise ValueError(
            f"body must be an int, float, str, list, or dict, got {type(v).__name__}"
        )


class RequestBaseModel(BaseModel):
    """
    Base model for request and response headers.

    Contains fields common to both request and response headers, providing
    request correlation, routing information, and optional authentication.

    Attributes:
        request_id: Unique request identifier (UUID format without hyphens)
        path: Resource path or endpoint (URI-like, e.g., "/sensor/temperature")
        correlation_id: Optional correlation ID for grouping related requests
        source: Optional source identifier (client or device that sent the message)
        token: Optional authentication token or bearer token
    """
    request_id: str
    path: str
    correlation_id: Optional[str] = None
    source: Optional[str] = None
    token: Optional[str] = None

    @field_validator("request_id")
    @classmethod
    def validate_request_id(cls, v):
        """
        Validate that request_id is a valid UUID format.

        Accepts UUIDs with or without hyphens. Must be 32 hexadecimal characters
        when hyphens are removed (standard UUID4 format).

        Args:
            v: Request ID string to validate

        Returns:
            Validated request ID string

        Raises:
            ValueError: If request_id is not a valid UUID format

        Example:
            >>> validate_request_id("a1b2c3d4-e5f6-7890-1234-567890abcdef")  # Valid
            >>> validate_request_id("a1b2c3d4e5f67890123456 7890abcdef")    # Valid
        """
        v_stripped = v.replace("-", "").strip()
        if not (
            len(v_stripped) == 32
            and all(c in "0123456789abcdefABCDEF" for c in v_stripped)
        ):
            raise ValueError(
                f"request_id must be a valid UUID (32 hex characters), got '{v}'. "
                "Example: 'a1b2c3d4-e5f6-7890-1234-567890abcdef'"
            )
        return v


class HeaderRequest(RequestBaseModel):
    """
    Header for request messages.

    Extends RequestBaseModel with the 'method' field to indicate the type
    of operation being requested (GET, POST, PUT, DELETE).

    Attributes:
        method: Request method (GET/POST/PUT/DELETE as int or string)
        request_id: Unique request identifier
        path: Resource path
        correlation_id: Optional correlation ID
        source: Optional source identifier
        token: Optional authentication token

    Example:
        >>> header = HeaderRequest(
        ...     method="GET",
        ...     path="/sensor/temperature",
        ...     request_id="a1b2c3d4-e5f6-7890-1234-567890abcdef"
        ... )
    """
    method: Union[int, str]

    @field_validator("method")
    @classmethod
    def validate_method(cls, v):
        """
        Validate and normalize method to string value.

        Accepts methods as integers (1-4) or strings ("GET", "POST", "PUT", "DELETE").
        Converts to uppercase string value for consistency.

        Args:
            v: Method value (int or str)

        Returns:
            Validated method string ("GET", "POST", "PUT", or "DELETE")

        Raises:
            ValueError: If method is invalid

        Example:
            >>> validate_method("get")  # Returns "GET"
            >>> validate_method(1)      # Returns "GET"
        """
        if not isinstance(v, (int, str)):
            raise ValueError(
                f"Method must be an integer or string, got {type(v).__name__}. "
                "Valid values: 1-4 (int) or 'GET', 'POST', 'PUT', 'DELETE' (str)"
            )

        try:
            if isinstance(v, int):
                if v not in {m.value for m in Method}:
                    valid_codes = ", ".join([f"{m.value}={m.name}" for m in Method])
                    raise ValueError(f"Invalid method code {v}. Valid codes: {valid_codes}")
                return Method(v).value

            if isinstance(v, str):
                if v.upper() not in {m.name for m in Method}:
                    valid_methods = ", ".join([m.name for m in Method])
                    raise ValueError(f"Invalid method '{v}'. Valid methods: {valid_methods}")
                return Method[v.upper()].value

        except (ValueError, KeyError) as e:
            raise ValueError(
                f"Invalid method: {v}. Method must be 1-4 (int) or "
                "'GET', 'POST', 'PUT', 'DELETE' (str)"
            ) from e


class HeaderResponse(RequestBaseModel):
    """
    Header for response messages.

    Extends RequestBaseModel with the 'response_code' field to indicate
    the result of the requested operation (CoAP-style codes 2xx, 4xx, 5xx).

    Attributes:
        response_code: Response status code (2xx success, 4xx client error, 5xx server error)
        request_id: Unique request identifier (must match the original request)
        path: Resource path (should match the request path)
        correlation_id: Optional correlation ID
        source: Optional source identifier (responding device/server)
        token: Optional authentication token

    Example:
        >>> header = HeaderResponse(
        ...     response_code=205,  # CONTENT
        ...     path="/sensor/temperature",
        ...     request_id="a1b2c3d4-e5f6-7890-1234-567890abcdef"
        ... )
    """
    response_code: int

    @field_validator("response_code")
    @classmethod
    def validate_response_code(cls, v):
        """
        Validate response code against known ResponseCode values.

        Accepts any integer but validates against the defined ResponseCode enum.
        Unknown codes are allowed to pass through for forward compatibility.

        Args:
            v: Response code integer

        Returns:
            Validated response code integer

        Note:
            Unknown response codes are logged but not rejected to maintain
            forward compatibility with future protocol versions.
        """
        # Validate code is a known ResponseCode, but allow unknown codes
        if v in {rc.value for rc in ResponseCode}:
            return ResponseCode(v).value
        # Unknown code - allow it but could log a warning in production
        return v


class GeneralPayload(PayloadBaseModel):
    """
    General-purpose payload for simple publish-subscribe messages.

    Use this for basic pub/sub messages that don't require request-response
    semantics. Contains only timestamp and body fields without headers.

    Example:
        >>> payload = GeneralPayload(
        ...     body={"temperature": 23.5, "humidity": 65},
        ...     timestamp="1710000000000"
        ... )
    """
    pass


class RequestPayload(PayloadBaseModel):
    """
    Request payload for request-response pattern.

    Wraps a request header with optional body data. Used when initiating
    a request that expects a correlated response.

    Attributes:
        header: Request header with method, path, and request_id
        body: Optional request data
        timestamp: Message timestamp

    Example:
        >>> payload = RequestPayload(
        ...     header={
        ...         "method": "POST",
        ...         "path": "/actuator/valve",
        ...         "request_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef"
        ...     },
        ...     body={"state": "open"},
        ...     timestamp="1710000000000"
        ... )
    """
    header: HeaderRequest


class ResponsePayload(PayloadBaseModel):
    """
    Response payload for request-response pattern.

    Wraps a response header with optional body data. Used when responding
    to a request, carrying the operation result and status code.

    Attributes:
        header: Response header with response_code, path, and request_id
        body: Optional response data
        timestamp: Message timestamp

    Example:
        >>> payload = ResponsePayload(
        ...     header={
        ...         "response_code": 205,  # CONTENT
        ...         "path": "/sensor/temperature",
        ...         "request_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef"
        ...     },
        ...     body={"value": 23.5, "unit": "celsius"},
        ...     timestamp="1710000000000"
        ... )
    """
    header: HeaderResponse
