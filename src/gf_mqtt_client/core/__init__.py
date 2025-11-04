"""
Core components shared by both async and sync MQTT clients.
"""
from .base import MQTTClientBase, parse_method, MessageLogger, ClientFormatter
from .models import (
    MQTTBrokerConfig,
    Method,
    ResponseCode,
    MessageType,
    RequestPayload,
    ResponsePayload,
    GeneralPayload,
    HeaderRequest,
    HeaderResponse,
)
from .exceptions import (
    ResponseException,
    BadRequestResponse,
    UnauthorizedResponse,
    BadOptionResponse,
    ForbiddenResponse,
    NotFoundResponse,
    MethodNotAllowedResponse,
    NotAcceptableResponse,
    PreconditionFailedResponse,
    RequestEntityTooLargeResponse,
    UnsupportedContentFormatResponse,
    InternalServerErrorResponse,
    NotImplementedResponse,
    BadGatewayResponse,
    ServiceUnavailableResponse,
    GatewayTimeoutResponse,
    ProxyingNotSupportedResponse,
)
# Message handler utilities (shared between async and sync)
from .message_handler import (
    RESPONSE_CODE_EXCEPTION_MAP,
    handle_response_with_exception,
)
from .payload_handler import PayloadHandler
from .topic_manager import TopicManager
from . import protocol_utils

__all__ = [
    # Base classes
    "MQTTClientBase",
    "parse_method",
    "MessageLogger",
    "ClientFormatter",
    # Models
    "MQTTBrokerConfig",
    "Method",
    "ResponseCode",
    "MessageType",
    "RequestPayload",
    "ResponsePayload",
    "GeneralPayload",
    "HeaderRequest",
    "HeaderResponse",
    # Exceptions
    "ResponseException",
    "BadRequestResponse",
    "UnauthorizedResponse",
    "BadOptionResponse",
    "ForbiddenResponse",
    "NotFoundResponse",
    "MethodNotAllowedResponse",
    "NotAcceptableResponse",
    "PreconditionFailedResponse",
    "RequestEntityTooLargeResponse",
    "UnsupportedContentFormatResponse",
    "InternalServerErrorResponse",
    "NotImplementedResponse",
    "BadGatewayResponse",
    "ServiceUnavailableResponse",
    "GatewayTimeoutResponse",
    "ProxyingNotSupportedResponse",
    # Message handler utilities
    "RESPONSE_CODE_EXCEPTION_MAP",
    "handle_response_with_exception",
    # Utilities
    "PayloadHandler",
    "TopicManager",
    "protocol_utils",
]
