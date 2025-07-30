import asyncio
from enum import Enum
import json
import sys
import uuid
from typing import Optional, Dict, Any, Callable, Awaitable, List
import logging
from aiomqtt import Client, MqttError, Topic
from .models import ResponseCode, Method
from .payload_handler import PayloadHandler
from .topic_manager import TopicManager
from .message_handler import (
    MessageHandlerBase,
    MessageHandlerProtocol,
    ResponseHandlerDefault,
)
from .exceptions import ResponseException, GatewayTimeoutResponse
import warnings
from .mqtt_logger_mixin import MQTTLoggerMixin, MessageDirection

# class ContextLogger(logging.LoggerAdapter):

#     def __init__(self, logger: logging.Logger, extra: Optional[Dict[str, Any]] = None):
#         super().__init__(logger, extra or {})
#         self._excluded_log_extras = ["namespace"]
#         self.extra = {k: v for k, v in self.extra.items() if k not in self._excluded_log_extras}

#     def process(self, msg, kwargs):
#         extra = kwargs.get("extra", {})
#         if not isinstance(extra, dict):
#             extra = {}
#         # Merge extra context with the adapter's extra
#         if self.extra:
#             extra = {**self.extra, **extra}
#         # Remove excluded keys from extra context
#         for key in self._excluded_log_extras:
#             extra.pop(key, None)

#         direction = extra.get("direction", MessageDirection.UNKNOWN.value)
#         source = extra.get("source")
#         target = extra.get("target")
#         request_id = extra.get("request_id")
#         subsystem = extra.get("subsystem")
#         client_id = extra.get("client_id")

#         parts = []
#         if client_id:
#             parts.append(f"[{client_id}")
#         else:
#             parts.append("[")

#         if direction == "request":
#             parts.append("/ REQ →]")
#         elif direction == "response":
#             parts.append("/ RESP ←]")
#         else:
#             parts.append("]")

#         if source:
#             route = f"{source} →"
#             route += f" {target}"
#             parts.append(route)

#         if subsystem:
#             parts.append(f"subsystem={subsystem}")
#         if request_id:
#             parts.append(f"req_id={request_id}")

#         tag = " | ".join(parts) + " - " if parts else ""
#         return f"{tag}{msg}", kwargs


def ensure_compatible_event_loop_policy():
    if sys.platform.startswith("win"):
        current_policy = asyncio.get_event_loop_policy()
        if not isinstance(current_policy, asyncio.WindowsSelectorEventLoopPolicy):
            warnings.warn(
                "Your current event loop policy may not support all features "
                "on Windows. Consider setting:\n"
                "  asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())",
                RuntimeWarning,
            )


def set_compatible_event_loop_policy():
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        logging.getLogger(__name__).info(
            "Set event loop policy to WindowsSelectorEventLoopPolicy for compatibility."
        )
    else:
        logging.getLogger(__name__).info(
            "No special event loop policy set for non-Windows platform."
        )


def reset_event_loop_policy():
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    logging.getLogger(__name__).info(
        "Reset event loop policy to DefaultEventLoopPolicy."
    )


def parse_method(method: Any) -> Method:
    if isinstance(method, Method):
        return method
    if isinstance(method, str):
        try:
            method = Method[method.upper()]
        except KeyError:
            raise ValueError(f"Invalid method: {method}")
    elif isinstance(method, int):
        try:
            method = Method(method)
        except ValueError:
            raise ValueError(f"Invalid method value: {method}")
    else:
        raise ValueError(f"Method must be an int or str, got {type(method)}")
    return method


def generate_unique_id(prefix: str = "mqtt_client") -> str:
    return f"{prefix}-{uuid.uuid4()}"


class MQTTClient(MQTTLoggerMixin):
    def __init__(
        self,
        broker: str,
        port: int = 1883,
        timeout: int = 5,
        identifier: Optional[str] = None,
        subscriptions: Optional[list] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ensure_unique_identifier: bool = False,
        logger: Optional[logging.LoggerAdapter] = None,
    ):
        super().__init__(logger_name=__name__, base_logger=logger)
        self.broker = broker
        self.port = port
        self.timeout = timeout
        self._username: Optional[str] = username
        self._password: Optional[str] = password
        if ensure_unique_identifier:
            identifier = generate_unique_id(identifier)
        else:
            identifier = identifier or generate_unique_id()
        self.identifier = identifier
        self._client: Optional[Client] = None
        self._client_task: Optional[asyncio.Task] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._message_handlers: List[MessageHandlerBase] = []
        self._connected = asyncio.Event()
        self._topic_manager = TopicManager()
        self.subscriptions = subscriptions or []

        logging.info("Initialized MQTT client")
        ensure_compatible_event_loop_policy()
        self._excluded_log_extras = ["namespace"]

    def set_credentials(self, username: str, password: str):
        self._username = username
        self._password = password
        logging.info("Credentials set for username")

    async def add_message_handler(self, handler: MessageHandlerProtocol):
        async with self._lock:
            if not isinstance(handler, MessageHandlerProtocol):
                raise ValueError("Handler must implement MessageHandlerProtocol")
            self._message_handlers.append(handler)
            logging.info(f"Added message handler {handler.__class__.__name__}")

    async def remove_message_handler(self, handler: MessageHandlerBase):
        async with self._lock:
            if handler in self._message_handlers:
                self._message_handlers.remove(handler)
                logging.info(
                    f"Removed message handler {handler.__class__.__name__}"
                )
            else:
                logging.warning(f"Handler {handler.__class__.__name__} not found")

    def generate_request_id(self) -> str:
        logging.debug("Generating request ID")
        return str(uuid.uuid4())

    async def connect(self):
        if self._connected.is_set():
            logging.warning("Already connected")
            return

        logging.info(f"Connecting to broker {self.broker}:{self.port}")

        self._client = Client(
            hostname=self.broker,
            port=self.port,
            username=self._username,
            password=self._password,
            identifier=self.identifier,
        )
        try:
            await self._client.__aenter__()
            logging.info("Connected to broker")
            self._client_task = asyncio.create_task(self._message_loop())
            self._connected.set()
        except MqttError as e:
            logging.error(f"Failed to connect: {e}")
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path="",
                detail=str(e),
                source=self.identifier,
            )

        request_topic = self._topic_manager.build_request_topic(
            target_device_tag=self.identifier, subsystem="+", request_id="+"
        )
        self.subscriptions.append(request_topic)

        for topic in self.subscriptions:
            await self._client.subscribe(topic)
            logging.info(f"Subscribed to topic {topic}")

        await self.add_message_handler(ResponseHandlerDefault())

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    async def disconnect(self):
        logging.info("Disconnecting")
        if self._client_task:
            self._client_task.cancel()
            try:
                await self._client_task
            except asyncio.CancelledError:
                logging.debug("Cancelled message loop")

        if self._client:
            await self._client.__aexit__(None, None, None)
            logging.info("Disconnected from broker")

    # async def _process_log(self, message: str, level: int = logging.INFO, topic: str | Topic = None, extra: Optional[Dict[str, Any]] = None):
    #     """Process log messages with optional topic and extra context."""
    #     if extra is None:
    #         extra = {}

    #     if topic:
    #         topic_parts = self._topic_manager.get_parts(topic)
    #         for key, value in topic_parts.items():
    #             if key == "type":
    #                 extra["direction"] = value
    #             elif key not in self._excluded_log_extras:
    #                 extra[key] = value

    #     direction = MessageDirection(extra.get("direction", "N/A"))
    #     if direction == MessageDirection.OUTGOING:
    #         extra["source"] = extra.get("source", self.identifier)
    #         extra["target"] = extra.get("target", "?")
    #     elif direction == MessageDirection.INCOMING:
    #         extra["source"] = extra.get("source", "?")
    #         extra["target"] = self.identifier

    #     logging.log(level, message, extra=extra)

    async def _message_loop(self):
        async for message in self._client.messages:
            topic = message.topic
            payload_str = message.payload.decode()
            try:
                payload = json.loads(payload_str)
                header = payload.get("header", {})
                source = header.get("source", "unknown")
                request_id = header.get("request_id", "N/A")
                self.log_message(
                    f"Received message: {self._truncate_str(payload_str)}",
                    direction=MessageDirection.RESPONSE,
                    level=logging.INFO,
                )
                processed = False
                for handler in self._message_handlers:
                    if not handler.can_handle(self, topic, payload):
                        continue
                    response = await handler.handle(self, topic, payload)
                    if response is not None:
                        if "request_id" in payload.get("header", {}):
                            async with self._lock:
                                future = self._pending_requests.pop(request_id, None)
                            if future and not future.done():
                                future.set_result(payload)
                                self.log_message(
                                    f"Resolved request {request_id} with handler {handler.__class__.__name__}",
                                    direction=MessageDirection.RESPONSE,
                                    level=logging.DEBUG,
                                )
                        processed = True
                    if not handler.propagate:
                        break
                if not processed:
                    default_handler = MessageHandlerBase(
                        can_handle=lambda c, m, p: True,
                        process=self._default_handler,
                        priority=100,
                        propagate=True,
                    )
                    await default_handler.handle(self, topic, payload)

            except json.JSONDecodeError:
                logging.warning(
                    f"Failed to decode JSON payload from topic {topic}: {payload_str}"
                )
            except ResponseException as e:
                logging.exception(
                    f"ResponseException while processing message: {e}"
                )
                async with self._lock:
                    future = self._pending_requests.pop(
                        payload.get("header", {}).get("request_id"), None
                    )
                if future and not future.done():
                    future.set_exception(e)
            except Exception as e:
                logging.error(
                    f"Error processing message: {e}",
                    exc_info=True
                )
                raise e

    async def _default_handler(
        self, topic: str, payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        logging.info(f"Default handler for topic {topic}")
        return payload

    async def _default_handler(
        self, topic: str, payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        logging.info(f"Default handler for topic {topic}")
        return payload

    async def publish(self, topic: str, payload: Dict[str, Any], qos: int = 0):
        if not self._connected.is_set():
            raise RuntimeError("Client not connected")
        await self._client.publish(topic, json.dumps(payload), qos=qos)
        logging.debug(f"Published to topic {topic}")

    async def subscribe(self, topic: str):
        if not self._connected.is_set():
            raise RuntimeError("Client not connected")
        await self._client.subscribe(topic)
        logging.info(f"Subscribed to topic {topic}")

    def _truncate_str(self, input_string: str, output_length: int = 50) -> str:
        if not isinstance(input_string, str):
            try:
                input_string = str(input_string)
            except Exception:
                return input_string
        if len(input_string) > output_length:
            return input_string[:output_length] + "..."
        return input_string

    async def request(
        self,
        target_device_tag,
        subsystem,
        path: str,
        method: Method = Method.GET,
        value: Any = None,
        timeout: int = None,
    ) -> Optional[Dict[str, Any]]:
        method = parse_method(method)

        if not self._connected.is_set():
            raise RuntimeError("Client not connected")

        payload_handler = PayloadHandler()
        request_id = self.generate_request_id()

        request_payload = payload_handler.create_request_payload(
            method=method, path=path, request_id=request_id, body=value, source=self.identifier
        )

        request_topic = self._topic_manager.build_request_topic(
            target_device_tag=target_device_tag,
            subsystem=subsystem,
            request_id=request_id,
        )
        response_topic = self._topic_manager.build_response_topic(
            request_topic=request_topic
        )

        future = asyncio.get_event_loop().create_future()
        async with self._lock:
            self._pending_requests[request_id] = future

        await self.subscribe(response_topic)
        await self.publish(request_topic, request_payload)
        self.log_request(
            f"Published request {request_id} to {request_topic}",
            level=logging.DEBUG,
        )

        try:
            result = await asyncio.wait_for(future, timeout=timeout or self.timeout)
            self.log_response(
                f"Received response for request {request_id} from {response_topic}",
                level=logging.DEBUG,
            )
            return result
        except asyncio.TimeoutError:
            self.log_response(
                f"Request timed out",
                level=logging.WARNING,
            )
            raise GatewayTimeoutResponse(
                response_code=ResponseCode.GATEWAY_TIMEOUT.value,
                path=path,
                detail="Request timed out",
                source=self.identifier,
                target=target_device_tag,
            )
        finally:
            await self._client.unsubscribe(response_topic)
            logging.debug(
                f"Unsubscribed from response topic {response_topic}",
            )
            async with self._lock:
                self._pending_requests.pop(request_id, None)


# Example usage of message handlers
if __name__ == "__main__":

    async def main():
        async def request_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            logging.info(f"Handling request: {payload}")
            payload_handler = PayloadHandler()
            return payload_handler.create_response_payload(
                response_code=ResponseCode.CONTENT,
                path=payload["header"]["path"],
                request_id=payload["header"]["request_id"],
                body={"data": [1, 2, 3]},
            )

        async def logging_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            logging.debug(f"Logging message: {payload}")
            return None

        async def response_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            if "response_code" in payload.get("header", {}):
                logging.info(f"Received response: {payload}")
            return None

        client = MQTTClient("localhost")
        client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda p: "method" in p.get("header", {}),
                process=request_handler,
                priority=1,
                propagate=False,
            )
        )
        client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda p: True,
                process=logging_handler,
                priority=2,
                propagate=True,
            )
        )
        client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda p: "response_code" in p.get("header", {}),
                process=response_handler,
                priority=3,
                propagate=False,
            )
        )

        await client.connect()
        await client.request("device1", "subsystem1", "path1")
        await asyncio.sleep(5)
        await client.disconnect()

    asyncio.run(main())
