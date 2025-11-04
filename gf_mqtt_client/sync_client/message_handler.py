"""
Sync message handlers for the sync MQTT client.
No async/await - pure blocking I/O.
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
    Protocol for synchronous message handlers.
    Used by SyncMQTTClient - no async/await.
    """
    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        ...

    def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ...

    @property
    def propagate(self) -> bool:
        ...


# === Base Handler (Sync) ===

class SyncMessageHandlerBase:
    """
    Base synchronous message handler.
    Used by sync MQTT client - no async/await.
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
        return self._can_handle(client, topic, payload)

    def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self._raise_exceptions:
            handle_response_with_exception(client, topic, payload)
        return self._process(client, topic, payload)

    @property
    def propagate(self) -> bool:
        return self._propagate

    def _truncate_payload(self, payload: Any, output_length: int = 50) -> str:
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
    """Sync response handler for sync MQTT client."""
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
    """Sync request handler for sync MQTT client."""
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
    """Default sync response handler."""
    def __init__(self):
        def process_default_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logger.debug(f"Response received: {self._truncate_payload(payload)}", extra={"topic": topic, "client_id": client.identifier})
            return payload

        super().__init__(
            process=process_default_response, propagate=True, raise_exceptions=True
        )


class SyncRequestHandlerDefault(SyncRequestHandlerBase):
    """Default sync request handler."""
    def __init__(self):
        def process_default_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logger.debug(f"Request received: {self._truncate_payload(payload)}", extra={"topic": topic, "client_id": client.identifier})
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
