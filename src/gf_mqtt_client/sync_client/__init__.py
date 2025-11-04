"""
Synchronous MQTT client implementation using paho-mqtt.
No async/await - pure blocking I/O.
"""
from .client import SyncMQTTClient
from .message_handler import (
    SyncMessageHandlerProtocol,
    SyncMessageHandlerBase,
    SyncResponseHandlerBase,
    SyncRequestHandlerBase,
    SyncResponseHandlerDefault,
    SyncRequestHandlerDefault,
)

__all__ = [
    # Client
    "SyncMQTTClient",
    # Message handlers
    "SyncMessageHandlerProtocol",
    "SyncMessageHandlerBase",
    "SyncResponseHandlerBase",
    "SyncRequestHandlerBase",
    "SyncResponseHandlerDefault",
    "SyncRequestHandlerDefault",
]
