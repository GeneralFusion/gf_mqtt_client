import asyncio
import contextlib
import orjson
import uuid
from typing import Optional, Dict, Any, List, Self
import logging
from pydantic import SecretStr
from aiomqtt import Client, MqttError
from .models import ResponseCode, Method, MessageType
from .payload_handler import PayloadHandler
from .topic_manager import TopicManager
from .message_handler import (
    MessageHandlerBase,
    MessageHandlerProtocol,
    ResponseHandlerDefault,
)
from .exceptions import ResponseException, GatewayTimeoutResponse
from .asyncio_compatibility import ensure_compatible_event_loop_policy

logger = logging.getLogger(__name__)

class ClientFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if hasattr(record, 'extra') and isinstance(record.extra, dict):
            extra_info = ' '.join(f"{k}={v}" for k, v in record.extra.items())
            return f"{record.msg} {extra_info}"
        return super().format(record)

class MessageLogger(logging.LoggerAdapter):
    """
    A custom logger that can be used to log messages with additional context.
    """

    def __init__(self, logger: logging.Logger, extra: Optional[Dict[str, Any]] = None, merge_extra: bool = False, exclude_extras: Optional[List[str]] = None):
        super().__init__(logger, extra or {})
        self.logger = logger
        self.extra = extra
        self.merge_extra = merge_extra
        self.exclude_extras = exclude_extras or []

    def process(self, msg, kwargs):
        if self.merge_extra and "extra" in kwargs:
            kwargs["extra"] = {**self.extra, **kwargs["extra"]}
        else:
            kwargs["extra"] = self.extra
        if self.exclude_extras:
            for key in self.exclude_extras:
                kwargs["extra"].pop(key, None)
        return msg, kwargs



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


class MQTTClient():
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
        qos_default: Optional[int] = 0,
    ):
        self.broker = broker
        self.port = port
        self.timeout = timeout
        self._username: Optional[str] = username
        self._password: Optional[str|SecretStr] = password
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
        self.qos_default = qos_default

        self.logger = logger or MessageLogger(
            logging.getLogger(__name__),
            extra={"client_id": self.identifier},
            merge_extra=True
        )

        self.logger.debug(f"Initialized MQTT client to broker {self.broker}:{self.port} with identifier '{self.identifier}'")
        ensure_compatible_event_loop_policy()

    def set_credentials(self, username: str, password: str | SecretStr):
        self._username = username
        self._password = password  # Store as str or SecretStr
        # Mask password for logging
        password_display = (
            password.get_secret_value()
            if isinstance(password, SecretStr)
            else (password or "")
        )
        self.logger.info(f"Credentials set for username - {username}:{password_display}")

    async def add_message_handler(self, handler: MessageHandlerProtocol):
        async with self._lock:
            if not isinstance(handler, MessageHandlerProtocol):
                raise ValueError("Handler must implement MessageHandlerProtocol")
            self._message_handlers.append(handler)
            self.logger.debug(f"Added message handler {handler.__class__.__name__}")

    async def remove_message_handler(self, handler: MessageHandlerBase):
        async with self._lock:
            if handler in self._message_handlers:
                self._message_handlers.remove(handler)
                self.logger.debug(
                    f"Removed message handler {handler.__class__.__name__}"
                )
            else:
                self.logger.warning(f"Handler {handler.__class__.__name__} not found")

    def generate_request_id(self) -> str:
        return str(uuid.uuid4())

    async def connect(self):
        if self._connected.is_set():
            self.logger.debug("Client already connected to broker")
            return

        self.logger.debug(f"Connecting to broker {self.broker}:{self.port}")

        # Convert password to str just before passing to Client
        password = (
            self._password.get_secret_value()
            if isinstance(self._password, SecretStr)
            else self._password
        )

        self._client = Client(
            hostname=self.broker,
            port=self.port,
            username=self._username,
            password=password,
            identifier=self.identifier,
        )
        try:
            await self._client.__aenter__()
            self.logger.info(f"Connected to broker {self.broker}:{self.port}")
            self._client_task = asyncio.create_task(self._message_loop())
            self._connected.set()
        except MqttError as e:
            self.logger.error(f"Failed to connect to broker {self.broker}:{self.port}: {e}")
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
            await self.subscribe(topic, qos=self.qos_default)

        await self.add_message_handler(ResponseHandlerDefault())

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()


    async def disconnect(self):
        self.logger.debug(f"Disconnecting from broker {self.broker}:{self.port}")

        # 1) Block new ops immediately
        self._connected.clear()

        # 2) Stop message loop
        if self._client_task:
            self._client_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._client_task
            self._client_task = None

        # 3) Close client
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.__aexit__(None, None, None)
            self._client = None

        # 4) Fail any pending requests so callers aren’t left hanging
        async with self._lock:
            for fut in self._pending_requests.values():
                if not fut.done():
                    fut.set_exception(asyncio.CancelledError("MQTT client disconnected"))
            self._pending_requests.clear()

        self.logger.info(f"Disconnected from broker {self.broker}:{self.port}")

    def _extract_extras(self, payload: dict, max_len: int = 50, extra_extras: Optional[dict] = None) -> dict:
        """Extract extra information from the payload for logging."""

        def _determine_message_type(payload: dict) -> str:
            if "header" in payload and "method" in payload["header"]:
                return MessageType.REQUEST.value
            if "header" in payload and "response_code" in payload["header"]:
                return MessageType.RESPONSE.value

            return MessageType.UNKNOWN.value

        assert max_len > 0, "max_len must be greater than 0"

        header = payload.get("header", {})
        source = header.get("source", "unknown")
        target = header.get("target", "unknown")
        request_id = header.get("request_id", "N/A")
        correlation_id = header.get("correlation_id", None)
        path = header.get("path", None)
        response_code = header.get("response_code", None)
        message_type = _determine_message_type(payload)

        extra_new = {}
        extra_new["source"] = source
        extra_new["target"] = target
        extra_new["request_id"] = request_id
        extra_new["correlation_id"] = correlation_id
        extra_new["path"] = path
        extra_new["message_type"] = message_type
        extra_new["response_code"] = response_code

        # Post-processing of extra fields
        for k, v in extra_new.items():

            # Truncate string values to max_len and append "..." if truncated
            self._truncate_str(v, output_length=max_len) if isinstance(v, str) and len(v) > max_len else v

        # Remove keys with None values
        extra_new = {k: v for k, v in extra_new.items() if v is not None}

        return {**extra_new, **(extra_extras or {})}    

    async def _message_loop(self):
        async for message in self._client.messages:
            topic = message.topic
            payload_str = message.payload.decode()
            try:
                payload = await asyncio.to_thread(orjson.loads, payload_str)
                header = payload.get("header", {})
                request_id = header.get("request_id", "N/A")
                self.logger.debug(
                    f"Received message: {self._truncate_str(payload_str, output_length=100)}",
                    extra=self._extract_extras(payload)
                )
                processed = False
                for handler in self._message_handlers:
                    if not handler.can_handle(self, topic, payload):
                        continue
                    try:
                        response = await handler.handle(self, topic, payload)
                        if response is not None:
                            if "request_id" in payload.get("header", {}):
                                async with self._lock:
                                    future = self._pending_requests.pop(request_id, None)
                                if future and not future.done():
                                    future.set_result(payload)
                            processed = True
                        if not handler.propagate:
                            break
                    except ResponseException as e:
                        self.logger.error(
                            f"'{ResponseCode(e.response_code)}' during request to '{e.target}' on path '{e.path}': {e.detail}",
                        )
                        async with self._lock:
                            future = self._pending_requests.pop(
                                payload.get("header", {}).get("request_id"), None
                            )
                        if future and not future.done():
                            future.set_exception(e)
                if not processed:
                    default_handler = MessageHandlerBase(
                        can_handle=lambda c, m, p: True,
                        process=self._default_handler,
                        propagate=True,
                    )
                    await default_handler.handle(self, topic, payload)

            except orjson.JSONDecodeError:
                self.logger.warning(
                    f"Failed to decode JSON payload from topic {topic}: {payload_str}",
                    extra=self._extract_extras(payload)
                )

            except Exception as e:
                self.logger.error(
                    f"Error processing message: {e}",
                    exc_info=True
                )
                raise e

    async def _default_handler(
        self, client: Self, topic: str, payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        self.logger.debug(f"Default handler for topic {topic}")
        return payload


    async def publish(self, topic: str, payload: Dict[str, Any], qos: Optional[int|None] = None):
        qos = qos if qos is not None else self.qos_default
        if not self._connected.is_set():
            raise RuntimeError("Client not connected")
        await self._client.publish(topic, await asyncio.to_thread(orjson.dumps, payload), qos=qos)
        self.logger.debug(f"Published to topic {topic}")

    async def subscribe(self, topic: str, qos: Optional[int|None] = None):
        qos = qos if qos is not None else self.qos_default
        if not self._connected.is_set():
            raise RuntimeError("Client not connected")
        await self._client.subscribe(topic, qos=qos)
        self.logger.debug(f"Subscribed to topic {topic}")

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
        target_device_tag: str,
        subsystem: str,
        path: str,
        method: Method = Method.GET,
        value: Any = None,
        timeout: int = None,
        qos: Optional[int|None] = None,
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

        await self.subscribe(response_topic, qos=qos)
        await self.publish(topic=request_topic, payload=request_payload, qos=qos)

        self.logger.info(
            f"'{Method(method)}' request sent to '{target_device_tag}' on uri-path '{path}'",
            extra=self._extract_extras(request_payload, extra_extras={"topic": request_topic})
        )

        try:
            loop = asyncio.get_running_loop()
            request_init_time = loop.time()
            result = await asyncio.wait_for(future, timeout=timeout or self.timeout)
            request_time = loop.time() - request_init_time
            response_code = ResponseCode(result.get("header", {}).get("response_code", None))
            self.logger.info(
                f"'{Method(method)}' request successful with outcome '{response_code}' to '{target_device_tag}' on uri-path '{path}' taking {request_time * 1000:.0f}ms",
                extra=self._extract_extras(result, extra_extras={"topic": response_topic})
            )
            return result
        except asyncio.TimeoutError:
            timeout_elapsed = loop.time() - request_init_time
            self.logger.error(
                f"'{Method(method)}' request to '{target_device_tag}' on uri-path '{path}' with request_id '{request_id}' timed out after {timeout_elapsed:.3f} seconds",
                extra=self._extract_extras(request_payload, extra_extras={"topic": request_topic})
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
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda p: "method" in p.get("header", {}),
                process=request_handler,
                propagate=False,
            )
        )
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda p: True,
                process=logging_handler,
                propagate=True,
            )
        )
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda p: "response_code" in p.get("header", {}),
                process=response_handler,
                propagate=False,
            )
        )

        await client.connect()
        await client.request("device1", "subsystem1", "path1")
        await asyncio.sleep(5)
        await client.disconnect()

    asyncio.run(main())
