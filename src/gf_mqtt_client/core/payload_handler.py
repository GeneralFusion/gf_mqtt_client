"""
Payload Handler for MQTT Message Creation and Validation.

This module provides the PayloadHandler class for creating, validating, and parsing
MQTT message payloads. It handles three payload types: GeneralPayload, RequestPayload,
and ResponsePayload, ensuring all payloads conform to the protocol schema.

Key Features:
    - Create validated payloads with automatic timestamp generation
    - Parse and validate JSON payloads from MQTT messages
    - Type-specific payload creation (general, request, response)
    - Comprehensive validation using Pydantic models
    - Detailed error reporting for validation failures

The PayloadHandler ensures that all outgoing messages are properly formatted
and that incoming messages are validated before processing, providing a layer
of data integrity for the MQTT protocol.

Example:
    >>> handler = PayloadHandler()
    >>> request = handler.create_request_payload(
    ...     method=Method.GET,
    ...     path="/sensor/temp",
    ...     request_id="abc-123",
    ...     body=None
    ... )
    >>> # Returns validated request payload dict
"""
from enum import Enum
from typing import Dict, Any, Optional
from pydantic import ValidationError
from .models import GeneralPayload, RequestPayload, ResponsePayload, HeaderRequest, HeaderResponse, Method, ResponseCode
import logging

logger = logging.getLogger(__name__)


class PayloadHandler:
    """
    Handler for creating and validating MQTT message payloads.

    Provides factory methods for creating validated payloads and utilities
    for parsing and validating incoming MQTT messages. All payloads are
    validated against Pydantic models before being returned.

    Attributes:
        methods: Reference to Method enum for convenience
        response_codes: Reference to ResponseCode enum for convenience
    """

    def __init__(self):
        """Initialize the PayloadHandler with enum references."""
        self.methods = Method
        self.response_codes = ResponseCode

    def create_general_payload(self, body: Dict[str, Any], timestamp: str) -> Dict[str, Any]:
        """
        Create a general-purpose payload for publish-subscribe messaging.

        Use this for simple messages that don't require request-response semantics.
        The payload contains only a timestamp and body without header information.

        Args:
            body: Message content (dict, list, or primitive types)
            timestamp: Unix timestamp in milliseconds (string or int)

        Returns:
            Validated general payload dictionary

        Raises:
            ValidationError: If body or timestamp are invalid

        Example:
            >>> handler.create_general_payload(
            ...     body={"status": "online", "temp": 23.5},
            ...     timestamp="1710000000000"
            ... )
        """
        payload = GeneralPayload(body=body, timestamp=timestamp)
        logger.debug(f"Created general payload with timestamp {timestamp}")
        return payload.model_dump()

    def create_request_payload(
        self,
        method: Method,
        path: str,
        request_id: str,
        body: Optional[Dict[str, Any]] = None,
        token: Optional[str] = None,
        correlation_id: Optional[str] = None,
        source: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a request payload for request-response messaging.

        Generates a validated request payload with automatic timestamp generation.
        The payload includes a header with method, path, request_id, and optional
        authentication/correlation fields.

        Args:
            method: Request method (GET, POST, PUT, DELETE)
            path: Resource path (e.g., "/sensor/temperature")
            request_id: Unique request identifier (UUID format)
            body: Optional request data
            token: Optional authentication token
            correlation_id: Optional correlation ID for request grouping
            source: Optional source identifier (client/device ID)

        Returns:
            Validated request payload dictionary with auto-generated timestamp

        Raises:
            ValidationError: If any field fails validation

        Example:
            >>> handler.create_request_payload(
            ...     method=Method.POST,
            ...     path="/actuator/valve",
            ...     request_id="abc-123-def-456",
            ...     body={"state": "open"}
            ... )
        """
        header = HeaderRequest(
            method=method.value,
            path=path,
            request_id=request_id,
            token=token,
            correlation_id=correlation_id,
            source=source
        )
        payload = RequestPayload(header=header, body=body, timestamp=str(self._get_current_timestamp()))

        logger.debug(
            f"Created request payload: method={method.name}, path={path}, request_id={request_id}",
            extra={"method": method.name, "path": path, "request_id": request_id}
        )

        return payload.model_dump()

    def create_response_payload(
        self,
        response_code: ResponseCode,
        path: str,
        request_id: str,
        body: Dict[str, Any],
        correlation_id: Optional[str] = None,
        source: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a response payload for request-response messaging.

        Generates a validated response payload with automatic timestamp generation.
        The payload includes a header with response_code, path, request_id, and
        optional correlation fields.

        Args:
            response_code: Response status code (e.g., ResponseCode.CONTENT, ResponseCode.NOT_FOUND)
            path: Resource path (should match the request path)
            request_id: Request identifier (must match the original request)
            body: Response data
            correlation_id: Optional correlation ID (should match request if present)
            source: Optional source identifier (server/device ID)

        Returns:
            Validated response payload dictionary with auto-generated timestamp

        Raises:
            ValidationError: If any field fails validation

        Example:
            >>> handler.create_response_payload(
            ...     response_code=ResponseCode.CONTENT,
            ...     path="/sensor/temperature",
            ...     request_id="abc-123-def-456",
            ...     body={"value": 23.5, "unit": "celsius"}
            ... )
        """
        header = HeaderResponse(
            response_code=response_code.value,
            path=path,
            request_id=request_id,
            correlation_id=correlation_id,
            source=source
        )
        payload = ResponsePayload(header=header, body=body, timestamp=str(self._get_current_timestamp()))

        logger.debug(
            f"Created response payload: code={response_code.value}, path={path}, request_id={request_id}",
            extra={"response_code": response_code.value, "path": path, "request_id": request_id}
        )

        return payload.model_dump()

    def validate_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a payload dictionary against the appropriate Pydantic model.

        Automatically detects the payload type based on header contents and
        validates against the corresponding model (RequestPayload, ResponsePayload,
        or GeneralPayload). Returns a validated and normalized payload dictionary.

        Args:
            payload: Raw payload dictionary to validate

        Returns:
            Validated payload dictionary with normalized fields

        Raises:
            ValidationError: If payload fails validation (includes detailed error info)
            Exception: For unexpected errors during validation

        Example:
            >>> handler.validate_payload({
            ...     "header": {"method": "GET", "path": "/temp", "request_id": "abc-123"},
            ...     "timestamp": "1710000000000"
            ... })
            # Returns validated RequestPayload dict
        """
        try:
            # Detect payload type by header contents
            header = payload.get("header", {})

            if "method" in header:
                # Request payload
                validated = RequestPayload(**payload).model_dump()
                logger.debug(f"Validated request payload: {header.get('request_id', 'N/A')}")
                return validated

            elif "response_code" in header:
                # Response payload
                validated = ResponsePayload(**payload).model_dump()
                logger.debug(
                    f"Validated response payload: code={header.get('response_code')}, "
                    f"request_id={header.get('request_id', 'N/A')}"
                )
                return validated

            else:
                # General payload (no header or unrecognized header)
                validated = GeneralPayload(**payload).model_dump()
                logger.debug("Validated general payload")
                return validated

        except ValidationError as e:
            # Log detailed validation errors for debugging
            logger.error(
                f"Payload validation failed: {e.error_count()} error(s). Details: {e.json()}",
                extra={"payload_preview": str(payload)[:100]}
            )
            raise

        except Exception as e:
            # Log unexpected errors
            logger.error(
                f"Unexpected error during payload validation: {type(e).__name__}: {str(e)}",
                extra={"payload_preview": str(payload)[:100]}
            )
            raise

    def parse_payload(self, payload: str) -> Dict[str, Any]:
        """
        Parse and validate a JSON payload string.

        Convenience method that combines JSON parsing with payload validation.
        Useful for processing raw MQTT message payloads received as strings.

        Args:
            payload: JSON string containing the payload

        Returns:
            Validated payload dictionary

        Raises:
            ValueError: If JSON is malformed
            ValidationError: If payload fails validation

        Example:
            >>> json_str = '{"body": {"temp": 23.5}, "timestamp": "1710000000000"}'
            >>> handler.parse_payload(json_str)
            # Returns validated dict
        """
        import json
        try:
            payload_dict = json.loads(payload)
            logger.debug(f"Parsed JSON payload: {len(payload)} bytes")
            return self.validate_payload(payload_dict)

        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to parse JSON payload: {str(e)}. Payload preview: {payload[:100]}",
                extra={"error_position": e.pos, "error_line": e.lineno}
            )
            raise ValueError(f"Invalid JSON payload: {str(e)}. Check payload format and encoding.")

    def _get_current_timestamp(self) -> int:
        """
        Get the current timestamp in milliseconds.

        Returns:
            Current Unix timestamp in milliseconds (suitable for payload timestamps)

        Example:
            >>> handler._get_current_timestamp()
            1710000000000
        """
        from time import time
        return int(time() * 1000)