import asyncio
import contextlib
import orjson
from typing import Any, Self
import logging
from pydantic import SecretStr
from aiomqtt import Client, MqttError
from ..core.models import ResponseCode, Method, MessageType
from .message_handler import (
    MessageHandlerProtocol,
    MessageHandlerBase,
    ResponseHandlerDefault,
)
from ..core.exceptions import ResponseException, GatewayTimeoutResponse
from .compatibility import ensure_compatible_event_loop_policy
from ..core.base import MQTTClientBase, parse_method, MessageLogger

logger = logging.getLogger(__name__)


class MQTTClient(MQTTClientBase):
    def __init__(
        self,
        broker: str,
        port: int | None = None,
        timeout: int = 5,
        identifier: str | None = None,
        subscriptions: list | None = None,
        username: str | None = None,
        password: str | SecretStr | None = None,
        ensure_unique_identifier: bool = False,
        logger: logging.LoggerAdapter | None = None,
        qos_default: int = 0,
    ):
        # Initialize base class with shared configuration
        super().__init__(
            broker=broker,
            port=port,
            timeout=timeout,
            identifier=identifier,
            subscriptions=subscriptions,
            username=username,
            password=password,
            ensure_unique_identifier=ensure_unique_identifier,
            logger=logger,
            qos_default=qos_default,
        )

        # Async-specific attributes
        self._client: Client | None = None
        self._client_task: asyncio.Task | None = None
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._message_handlers: list[MessageHandlerProtocol] = []
        self._connected = asyncio.Event()

        # Ensure compatible event loop policy for Windows
        ensure_compatible_event_loop_policy()

    async def add_message_handler(self, handler: MessageHandlerProtocol):
        async with self._lock:
            if not isinstance(handler, MessageHandlerProtocol):
                raise ValueError("Handler must implement MessageHandlerProtocol")
            self._message_handlers.append(handler)
            self.logger.debug(f"Added message handler {handler.__class__.__name__}")

    async def remove_message_handler(self, handler: MessageHandlerProtocol):
        async with self._lock:
            if handler in self._message_handlers:
                self._message_handlers.remove(handler)
                self.logger.debug(
                    f"Removed message handler {handler.__class__.__name__}"
                )
            else:
                self.logger.warning(f"Handler {handler.__class__.__name__} not found")

    async def connect(self):
        if self._connected.is_set():
            self.logger.debug("Client already connected to broker")
            return

        self.logger.debug(
            f"Connecting to broker {self.broker}:{self._port or '(default)'}"
        )

        # Convert SecretStr to str lazily
        password = (
            self._password.get_secret_value()
            if isinstance(self._password, SecretStr)
            else self._password
        )

        kwargs: dict[str, Any] = {"hostname": self.broker}
        if self._port is not None:
            kwargs["port"] = self._port
        if self._username is not None:
            kwargs["username"] = self._username
        if password is not None:
            kwargs["password"] = password
        if self.identifier is not None:
            kwargs["identifier"] = self.identifier

        self._client = Client(**kwargs)
        try:
            await self._client.__aenter__()
            self.logger.info(
                f"Connected to broker {self.broker}:{kwargs.get('port', '(default)')}"
            )
            self._client_task = asyncio.create_task(self._message_loop())
            self._connected.set()
        except MqttError as e:
            self.logger.error(
                f"Failed to connect to broker {self.broker}:{kwargs.get('port', '(default)')}: {e}"
            )
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
        self.logger.debug(f"Disconnecting from broker {self.broker}:{self._port}")

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

        self.logger.info(f"Disconnected from broker {self.broker}:{self._port}")

    async def _message_loop(self):
        async for message in self._client.messages:
            topic = str(message.topic)  # Convert Topic object to string
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
        self, client: Self, topic: str, payload: Any
    ) -> dict[str, Any]:
        self.logger.debug(f"Default handler for topic {topic}")
        return payload

    async def publish(self, topic: str, payload: Any, qos: int | None = None):
        qos = qos if qos is not None else self.qos_default
        if not self._connected.is_set():
            raise RuntimeError("Client not connected")
        await self._client.publish(topic, await asyncio.to_thread(orjson.dumps, payload), qos=qos)
        self.logger.debug(f"Published to topic {topic}")

    async def subscribe(self, topic: str, qos: int | None = None):
        qos = qos if qos is not None else self.qos_default
        if not self._connected.is_set():
            raise RuntimeError("Client not connected")
        await self._client.subscribe(topic, qos=qos)
        self.logger.debug(f"Subscribed to topic {topic}")

    async def request(
        self,
        target_device_tag: str,
        subsystem: str,
        path: str,
        method: Method = Method.GET,
        value: Any = None,
        timeout: int|None = None,
        qos: int|None = None,
    ) -> dict[str, Any]:
        method = parse_method(method)

        if not self._connected.is_set():
            raise RuntimeError("Client not connected")

        request_id = self.generate_request_id()

        request_payload = self._payload_handler.create_request_payload(
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
        async def request_handler(client, topic: str, payload: Any) -> dict[str, Any]:
            logging.info(f"Handling request: {payload}")
            payload_handler = PayloadHandler()
            return payload_handler.create_response_payload(
                response_code=ResponseCode.CONTENT,
                path=payload["header"]["path"],
                request_id=payload["header"]["request_id"],
                body={"data": [1, 2, 3]},
            )

        async def logging_handler(client, topic: str, payload: Any) -> None:
            logging.debug(f"Logging message: {payload}")

        async def response_handler(client, topic: str, payload: Any) -> None:
            if "response_code" in payload.get("header", {}):
                logging.info(f"Received response: {payload}")

        client = MQTTClient("localhost")
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: "method" in p.get("header", {}),
                process=request_handler,
                propagate=False,
            )
        )
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: True,
                process=logging_handler,
                propagate=True,
            )
        )
        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: "response_code" in p.get("header", {}),
                process=response_handler,
                propagate=False,
            )
        )

        await client.connect()
        await client.request("device1", "subsystem1", "path1")
        await asyncio.sleep(5)
        await client.disconnect()

    asyncio.run(main())
