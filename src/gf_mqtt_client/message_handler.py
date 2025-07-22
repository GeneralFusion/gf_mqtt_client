


from typing import Any, Awaitable, Callable, Dict, Optional, runtime_checkable, Protocol
import logging

# from src.mqtt_client import MQTTClient
MQTTClient = Any  # Placeholder for the actual MQTTClient type, replace with the correct import
@runtime_checkable
class MessageHandlerProtocol(Protocol):
    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        ...

    async def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ...

    @property
    def propagate(self) -> bool:
        ...


        
class MessageHandlerBase:
    def __init__(
        self,
        can_handle: Callable[[MQTTClient, str, Dict[str, Any]], bool],
        process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]],
        propagate: bool = True
    ):
        self._can_handle = can_handle
        self._process = process
        self._propagate = propagate

    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        return self._can_handle(client, topic, payload)

    async def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return await self._process(client, topic, payload)

    @property
    def propagate(self) -> bool:
        return self._propagate
    
    def _truncate_payload(self, payload: str, output_length: int = 50) -> str:
        """
        Truncates a payload to a specified length, appending '...' if truncated.

        Args:
            payload: The input string to truncate.
            output_length: Maximum length of the output string (default: 50).

        Returns:
            Truncated payload as a string, or original payload if length <= output_length.
        """
        if not isinstance(payload, str):
            try:
                payload = str(payload)
            except Exception:
                return payload

        if len(payload) > output_length:
            return payload[:output_length] + "..."
        return payload

class ResponseHandlerBase(MessageHandlerBase):
    def __init__(self, process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Dict[str, Any]]], propagate: bool = False):
        def can_handle_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
            return "response_code" in payload.get("header", {})

        super().__init__(can_handle=can_handle_response, process=process, propagate=propagate)

class RequestHandlerBase(MessageHandlerBase):
    def __init__(self, process: Callable[[MQTTClient, str, Dict[str, Any]], Awaitable[Dict[str, Any]]], propagate: bool = True):
        def can_handle_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
            return "method" in payload.get("header", {})

        super().__init__(can_handle=can_handle_request, process=process, propagate=propagate)


class ResponseHandlerDefault(ResponseHandlerBase):
    def __init__(self):
        async def process_default_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Awaitable[Dict[str, Any]]:
            logging.debug(f"Response received: {self._truncate_payload(payload)}", extra={"topic": topic})
            return payload
        super().__init__(process=process_default_response, propagate=True)

class RequestHandlerDefault(RequestHandlerBase):
    def __init__(self):
        async def process_default_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Awaitable[Dict[str, Any]]:
            logging.debug(f"Request received: {self._truncate_payload(payload)}", extra={"topic": topic})
            return payload
        super().__init__(process=process_default_request, propagate=True)