"""
Async message handlers for the async MQTT client.
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
    Protocol for async message handlers.
    Used by async MQTT client.
    """
    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        ...

    async def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ...

    @property
    def propagate(self) -> bool:
        ...


# === Base Handler (Async) ===

class MessageHandlerBase:
    """
    Base async message handler.
    Used by async MQTT client.
    """
    def __init__(
        self,
        can_handle: Callable[[MQTTClient, str, Dict[str, Any]], bool],
        process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]],
        propagate: bool = True,
        raise_exceptions: bool = False,
    ):
        self._can_handle = can_handle
        self._process = process
        self._propagate = propagate
        self._raise_exceptions = raise_exceptions

    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        return self._can_handle(client, topic, payload)

    async def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self._raise_exceptions:
            handle_response_with_exception(client, topic, payload)
        return await self._process(client, topic, payload)

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


# === Response Handler (Async) ===

class ResponseHandlerBase(MessageHandlerBase):
    """Async response handler for async MQTT client."""
    def __init__(
        self,
        process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Dict[str, Any]]],
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


# === Request Handler (Async) ===

class RequestHandlerBase(MessageHandlerBase):
    """Async request handler for async MQTT client."""
    def __init__(
        self,
        process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Dict[str, Any]]],
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


# === Default Handlers (Async) ===

class ResponseHandlerDefault(ResponseHandlerBase):
    """Default async response handler."""
    def __init__(self):
        async def process_default_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logger.debug(f"Response received: {self._truncate_payload(payload)}", extra={"topic": topic, "client_id": client.identifier})
            return payload

        super().__init__(
            process=process_default_response, propagate=True, raise_exceptions=True
        )


class RequestHandlerDefault(RequestHandlerBase):
    """Default async request handler."""
    def __init__(self):
        async def process_default_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logger.debug(f"Request received: {self._truncate_payload(payload)}", extra={"topic": topic, "client_id": client.identifier})
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
