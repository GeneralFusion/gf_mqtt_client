"""
Async MQTT client implementation using aiomqtt.
"""
from .client import MQTTClient
from .compatibility import (
    ensure_compatible_event_loop_policy,
    set_compatible_event_loop_policy,
    configure_asyncio_compatibility,
    reset_event_loop_policy,
)
from .message_handler import (
    MessageHandlerProtocol,
    MessageHandlerBase,
    ResponseHandlerBase,
    RequestHandlerBase,
    ResponseHandlerDefault,
    RequestHandlerDefault,
)

__all__ = [
    # Client
    "MQTTClient",
    # Compatibility
    "ensure_compatible_event_loop_policy",
    "set_compatible_event_loop_policy",
    "configure_asyncio_compatibility",
    "reset_event_loop_policy",
    # Message handlers
    "MessageHandlerProtocol",
    "MessageHandlerBase",
    "ResponseHandlerBase",
    "RequestHandlerBase",
    "ResponseHandlerDefault",
    "RequestHandlerDefault",
]
