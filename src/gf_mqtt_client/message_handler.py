


from typing import Any, Awaitable, Callable, Dict, Optional, runtime_checkable, Protocol
import logging
from .models import ResponseCode
from .exceptions import BadRequestResponse, UnauthorizedResponse, NotFoundResponse, InternalServerErrorResponse, MethodNotAllowedResponse, ResponseException, GatewayTimeoutResponse
# from src.mqtt_client import MQTTClient
MQTTClient = Any  # Placeholder for the actual MQTTClient type, replace with the correct import

# === Exceptions ===


# Map response codes to exceptions
RESPONSE_CODE_EXCEPTION_MAP = {
    ResponseCode.BAD_REQUEST: BadRequestResponse,
    ResponseCode.UNAUTHORIZED: UnauthorizedResponse,
    ResponseCode.NOT_FOUND: NotFoundResponse,
    ResponseCode.METHOD_NOT_ALLOWED: MethodNotAllowedResponse,
    ResponseCode.INTERNAL_SERVER_ERROR: InternalServerErrorResponse,
}


def handle_response_with_exception(payload: Dict[str, Any]) -> Dict[str, Any]:
    header = payload.get("header", {})
    response_code_value = header.get("response_code")

    if response_code_value is not None:
        try:
            response_code = ResponseCode(response_code_value)
        except ValueError:
            raise ResponseException(f"Unknown response code: {response_code_value}")

        exception_class = RESPONSE_CODE_EXCEPTION_MAP.get(response_code)
        if exception_class:
            raise exception_class(f"Response code {response_code.value}: {response_code.name}")


# === Handler Protocol ===

@runtime_checkable
class MessageHandlerProtocol(Protocol):
    def can_handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> bool:
        ...

    async def handle(self, client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ...

    @property
    def propagate(self) -> bool:
        ...


# === Base Handler ===

class MessageHandlerBase:
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
            handle_response_with_exception(payload)
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


# === Response Handler Base ===

class ResponseHandlerBase(MessageHandlerBase):
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


# === Request Handler Base ===

class RequestHandlerBase(MessageHandlerBase):
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


# === Default Handlers ===

class ResponseHandlerDefault(ResponseHandlerBase):
    def __init__(self):
        async def process_default_response(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logging.debug(f"Response received: {self._truncate_payload(payload)}", extra={"topic": topic})
            return payload

        super().__init__(process=process_default_response, propagate=True, raise_exceptions=True)


class RequestHandlerDefault(RequestHandlerBase):
    def __init__(self):
        async def process_default_request(client: MQTTClient, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            logging.debug(f"Request received: {self._truncate_payload(payload)}", extra={"topic": topic})
            return payload

        super().__init__(process=process_default_request, propagate=True)