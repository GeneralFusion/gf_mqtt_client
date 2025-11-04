"""
Shared Message Handler Utilities.

This module provides common utilities for processing MQTT messages in both async
and sync client implementations. It handles response validation, exception mapping,
and error handling for the request-response protocol.

Key Components:
    - RESPONSE_CODE_EXCEPTION_MAP: Maps ResponseCode enums to exception classes
    - handle_response_with_exception(): Validates responses and raises appropriate exceptions

The module enables automatic exception raising when error response codes (4xx, 5xx)
are detected in message payloads, providing a Pythonic error handling mechanism
for the MQTT request-response protocol.

Usage:
    This module is used internally by response handlers to automatically convert
    error response codes into typed exceptions. Application code typically doesn't
    need to call these functions directly.

Example:
    >>> # Used internally by response handlers
    >>> handle_response_with_exception(client, topic, response_payload)
    # Raises NotFoundResponse if response_code == 404
    # Raises InternalServerErrorResponse if response_code == 500
    # Returns normally for 2xx success codes
"""
from typing import Any, Dict
import logging

from .models import ResponseCode
from .exceptions import *
from .topic_manager import TopicManager

# Type placeholder for MQTTClient to avoid circular imports
MQTTClient = Any

logger = logging.getLogger(__name__)


# Map response codes to their corresponding exception classes
RESPONSE_CODE_EXCEPTION_MAP = {
    ResponseCode.BAD_REQUEST: BadRequestResponse,
    ResponseCode.UNAUTHORIZED: UnauthorizedResponse,
    ResponseCode.BAD_OPTION: BadOptionResponse,
    ResponseCode.FORBIDDEN: ForbiddenResponse,
    ResponseCode.NOT_FOUND: NotFoundResponse,
    ResponseCode.METHOD_NOT_ALLOWED: MethodNotAllowedResponse,
    ResponseCode.NOT_ACCEPTABLE: NotAcceptableResponse,
    ResponseCode.PRECONDITION_FAILED: PreconditionFailedResponse,
    ResponseCode.REQUEST_ENTITY_TOO_LARGE: RequestEntityTooLargeResponse,
    ResponseCode.UNSUPPORTED_CONTENT_FORMAT: UnsupportedContentFormatResponse,
    ResponseCode.INTERNAL_SERVER_ERROR: InternalServerErrorResponse,
    ResponseCode.NOT_IMPLEMENTED: NotImplementedResponse,
    ResponseCode.BAD_GATEWAY: BadGatewayResponse,
    ResponseCode.SERVICE_UNAVAILABLE: ServiceUnavailableResponse,
    ResponseCode.GATEWAY_TIMEOUT: GatewayTimeoutResponse,
    ResponseCode.PROXYING_NOT_SUPPORTED: ProxyingNotSupportedResponse,
}


def handle_response_with_exception(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate response payload and raise typed exception for error response codes.

    This function inspects response payloads and automatically raises appropriate
    exceptions when error codes (4xx, 5xx) are detected. Success codes (2xx) pass
    through without raising exceptions. Unknown response codes raise a generic
    ResponseException with the unrecognized code value.

    The function extracts contextual information from both the payload header and
    the MQTT topic to provide detailed error information including request ID,
    path, target device, and error details.

    Args:
        client: MQTT client instance (used to extract source identifier)
        topic: MQTT topic string (parsed to extract target device tag)
        payload: Response message payload dictionary with header and optional body

    Returns:
        The payload dictionary if response code is a success code (2xx) or None

    Raises:
        ResponseException: For unknown or invalid response codes
        BadRequestResponse: For response_code 400
        UnauthorizedResponse: For response_code 401
        NotFoundResponse: For response_code 404
        InternalServerErrorResponse: For response_code 500
        ... (and other specific ResponseException subclasses)

    Example:
        >>> # Success response (2xx) - no exception raised
        >>> handle_response_with_exception(client, topic, {"header": {"response_code": 205}})

        >>> # Error response (4xx/5xx) - exception raised
        >>> handle_response_with_exception(client, topic, {"header": {"response_code": 404}})
        # Raises NotFoundResponse with full context
    """
    topic_manager = TopicManager()

    # Extract header fields for exception context
    header = payload.get("header", {})
    method = header.get("method")
    request_id = header.get("request_id")
    path = header.get("path")
    response_code_value = header.get("response_code")

    # Extract error detail from multiple possible locations
    detail = payload.get("body") or header.get("location") or header.get("error_message")

    # Parse target device tag from MQTT topic
    target_tag = topic_manager.get_target_device_tag_from_topic(topic)

    # Only process if response contains a response_code
    if response_code_value is not None:
        try:
            response_code = ResponseCode(response_code_value)
        except ValueError:
            # Unknown response code - log and raise generic exception
            logger.error(
                f"Received unknown response code: {response_code_value}. "
                f"Expected one of: {[rc.value for rc in ResponseCode]}",
                extra={"request_id": request_id, "path": path, "target": target_tag}
            )
            raise ResponseException(
                f"Unknown response code: {response_code_value}",
                response_code=response_code_value,
                path=path,
                request_id=request_id
            )

        # Check if this response code maps to an exception
        exception_class = RESPONSE_CODE_EXCEPTION_MAP.get(response_code)
        if exception_class:
            # Log the error before raising
            logger.error(
                f"Request failed with {response_code.name} ({response_code.value}): {detail}",
                extra={
                    "response_code": response_code.value,
                    "request_id": request_id,
                    "path": path,
                    "target": target_tag,
                    "method": method
                }
            )

            # Raise typed exception with full context
            raise exception_class(
                response_code=response_code.value,
                path=path,
                detail=str(detail),
                source=client.identifier,
                target=target_tag,
                request_id=request_id,
                method=method
            )

        # Success code (2xx) - log and return normally
        logger.debug(
            f"Request succeeded with {response_code.name} ({response_code.value})",
            extra={"request_id": request_id, "path": path, "response_code": response_code.value}
        )


__all__ = [
    "RESPONSE_CODE_EXCEPTION_MAP",
    "handle_response_with_exception",
]
