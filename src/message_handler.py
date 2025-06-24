


import json
from typing import Any, Awaitable, Callable, Dict, Optional
import logging
from aiomqtt import Message
# from src.mqtt_client import "MQTTClient"

class MessageHandlerBase:
    def __init__(self, can_handle: Callable[["MQTTClient", Message], bool], process: Callable[["MQTTClient", Message], Awaitable[Dict[str, Any]]], propagate: bool = True):
        self.can_handle = can_handle
        self.process = process
        self.propagate = propagate  # If True, continue to next handler after processing

    async def handle(self, client: "MQTTClient", topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.can_handle(client, topic, payload):
            return await self.process(client, topic, payload)
        return None
    

class ResponseHandlerBase(MessageHandlerBase):
    def __init__(self, process: Callable[["MQTTClient", Message], Awaitable[Dict[str, Any]]], propagate: bool = False):
        def can_handle_response(client: "MQTTClient", topic: str, payload: Dict[str, Any]) -> bool:
            return "response_code" in payload.get("header", {})

        super().__init__(can_handle=can_handle_response, process=process, propagate=propagate)

class RequestHandlerBase(MessageHandlerBase):
    def __init__(self, process: Callable[["MQTTClient", Message], Awaitable[Dict[str, Any]]], propagate: bool = True):
        def can_handle_request(client: "MQTTClient", topic: str, payload: Dict[str, Any]) -> bool:
            return "method" in payload.get("header", {})

        super().__init__(can_handle=can_handle_request, process=process, propagate=propagate)


class ResponseHandlerDefault(ResponseHandlerBase):
    def __init__(self):
        async def process_default_response(client: "MQTTClient", topic: str, payload: Dict[str, Any]) -> Awaitable[Dict[str, Any]]:
            logging.debug(f"Response received: {payload}", extra={"topic": topic})
            return payload
        super().__init__(process=process_default_response, propagate=True)

class RequestHandlerDefault(RequestHandlerBase):
    def __init__(self):
        async def process_default_request(client: "MQTTClient", topic: str, payload: Dict[str, Any]) -> Awaitable[Dict[str, Any]]:
            logging.debug(f"Request received: {payload}", extra={"topic": topic})
            return payload
        super().__init__(process=process_default_request, propagate=True)