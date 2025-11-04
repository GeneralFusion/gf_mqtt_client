"""
Shared message handler utilities used by both async and sync clients.
"""
from typing import Any, Dict
import logging

from .models import ResponseCode
from .exceptions import *
from .topic_manager import TopicManager

# Type placeholder for MQTTClient to avoid circular imports
MQTTClient = Any

logger = logging.getLogger(__name__)


# Map response codes to exceptions
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
    Check response payload and raise appropriate exception if error code found.
    Used by both sync and async handlers.

    Args:
        client: MQTT client instance
        topic: MQTT topic
        payload: Message payload

    Raises:
        ResponseException: If response contains an error code
    """
    topic_manager = TopicManager()

    header = payload.get("header", {})
    method = header.get("method")
    request_id = header.get("request_id")
    path = header.get("path")
    response_code_value = header.get("response_code")
    detail = payload.get("body") or header.get("location") or header.get("error_message")
    target_tag = topic_manager.get_target_device_tag_from_topic(topic)

    if response_code_value is not None:
        try:
            response_code = ResponseCode(response_code_value)
        except ValueError:
            raise ResponseException(f"Unknown response code: {response_code_value}")

        exception_class = RESPONSE_CODE_EXCEPTION_MAP.get(response_code)
        if exception_class:
            raise exception_class(
                response_code=response_code.value,
                path=path,
                detail=str(detail),
                source=client.identifier,
                target=target_tag,
                request_id=request_id,
                method=method
            )


__all__ = [
    "RESPONSE_CODE_EXCEPTION_MAP",
    "handle_response_with_exception",
]
