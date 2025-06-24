import asyncio
import json
import uuid
from typing import Optional, Dict, Any, Callable, Awaitable, List

from aiomqtt import Client
from models import ResponseCode
from src.payload_handler import Method, PayloadHandler
from src.topic_manager import TopicManager
from src.message_handler import MessageHandlerBase, ResponseHandlerDefault


class MQTTClient:
    def __init__(self, broker: str, port: int = 1883, timeout: int = 5, identifier: Optional[str] = None, subscriptions: Optional[list] = None):
        self.broker = broker
        self.port = port
        self.timeout = timeout
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self.identifier = identifier if identifier else f"mqtt_client_{uuid.uuid4()}"

        self._client: Optional[Client] = None
        self._client_task: Optional[asyncio.Task] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._message_handlers: List[MessageHandlerBase] = []  # Raw list for handlers
        self._connected = asyncio.Event()
        self._topic_manager = TopicManager()
        self.subscriptions = subscriptions or []


    def set_credentials(self, username: str, password: str):
        self._username = username
        self._password = password

    async def add_message_handler(self, handler: MessageHandlerBase):
        async with self._lock:
            self._message_handlers.append(handler)

    async def remove_message_handler(self, handler: MessageHandlerBase):
        async with self._lock:
            if handler in self._message_handlers:
                self._message_handlers.remove(handler)

    async def connect(self):
        self._client = Client(
            hostname=self.broker,
            port=self.port,
            username=self._username,
            password=self._password,
            identifier=self.identifier
        )
        await self._client.__aenter__()  # enter the async context manually

        self._client_task = asyncio.create_task(self._message_loop())
        self._connected.set()

        # Subscribe to the main request topic to receive requests
        request_topic = self._topic_manager.build_request_topic(
            target_device_tag=self.identifier,
            subsystem="+",
            request_id="+"
        )
        self.subscriptions.append(request_topic)

        # Subscribe to additional topics if any
        if self.subscriptions:
            for topic in self.subscriptions:
                await self._client.subscribe(topic)

        # Add default response handler for logging of responses
        await self.add_message_handler(ResponseHandlerDefault())
        

    async def disconnect(self):
        if self._client_task:
            self._client_task.cancel()
            try:
                await self._client_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.__aexit__(None, None, None)

    async def _message_loop(self):
        async for message in self._client.messages:
            payload_str = message.payload.decode()
            try:

                payload = json.loads(payload_str)
                topic = message.topic
                # Iterate over handlers in the order they were added
                processed = False
                for handler in self._message_handlers:
                    response = await handler.handle(self, topic, payload)
                    if response is not None:
                        header = payload.get("header", {})
                        if "request_id" in header and "response_code" in header:
                            request_id = header["request_id"]
                            async with self._lock:
                                future = self._pending_requests.pop(request_id, None)
                            if future and not future.done():
                                future.set_result(payload)
                        processed = True
                    if not handler.propagate:
                        break  # Stop if propagate is False

                if not processed and any(h.can_handle(self, message) for h in self._message_handlers):
                    # Ensure default handler is processed if no other handler did
                    default_handler = MessageHandlerBase(
                        can_handle=lambda c, m: True,
                        process=self._default_handler,
                        priority=100,
                        propagate=True
                    )
                    await default_handler.handle(self, topic, payload)

            except json.JSONDecodeError:
                print("Invalid JSON received")
            except Exception as e:
                print(f"Error in message loop: {e}")

    async def _default_handler(self, topic: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        print(f"Default handler processing unhandled message on topic {topic}: {payload}")
        return None  # No response by default

    async def publish(self, topic: str, payload: Dict[str, Any], qos: int = 0):
        if not self._client:
            raise RuntimeError("Client is not connected")

        payload_str = json.dumps(payload)
        await self._client.publish(topic, payload_str, qos=qos)

    async def subscribe(self, topic: str):
        if not self._client:
            raise RuntimeError("Client is not connected")

        await self._client.subscribe(topic)

    def generate_request_id(self) -> str:
        return str(uuid.uuid4())

    async def request(self, target_device_tag, subsystem, path: str) -> Optional[Dict[str, Any]]:
        if not self._connected.is_set():
            raise RuntimeError("Client not connected")

        payload_handler = PayloadHandler()
        request_id = self.generate_request_id()

        request_payload = payload_handler.create_request_payload(
            method=Method.GET,
            path=path,
            request_id=request_id
        )

        request_topic = self._topic_manager.build_request_topic(
            target_device_tag=target_device_tag,
            subsystem=subsystem,
            request_id=request_id
        )
        response_topic = self._topic_manager.build_response_topic(
            request_topic=request_topic
        )

        future = asyncio.get_event_loop().create_future()
        async with self._lock:
            self._pending_requests[request_id] = future

        # Subscribe to the response topic for this request
        await self.subscribe(response_topic)
        await self.publish(request_topic, request_payload)

        try:
            return await asyncio.wait_for(future, timeout=self.timeout)
        except asyncio.TimeoutError:
            print(f"Request timed out after {self.timeout} seconds")
            async with self._lock:
                self._pending_requests.pop(request_id, None)
            return None

# Example usage of message handlers
if __name__ == "__main__":
    async def main():
        async def request_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            print(f"Handling request: {payload}")
            payload_handler = PayloadHandler()
            return payload_handler.create_response_payload(
                response_code=ResponseCode.CONTENT,
                path=payload["header"]["path"],
                request_id=payload["header"]["request_id"],
                body={"data": [1, 2, 3]}
            )

        async def logging_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            print(f"Logging message: {payload}")
            return None  # No response, just logging

        async def response_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            if "response_code" in payload.get("header", {}):
                print(f"Received response: {payload}")
            return None

        client = MQTTClient("localhost")
        client.add_message_handler(MessageHandlerBase(
            can_handle=lambda p: "method" in p.get("header", {}),
            process=request_handler,
            priority=1,
            propagate=False  # Stop after handling request
        ))
        client.add_message_handler(MessageHandlerBase(
            can_handle=lambda p: True,  # Log all messages
            process=logging_handler,
            priority=2,
            propagate=True  # Allow further processing
        ))
        client.add_message_handler(MessageHandlerBase(
            can_handle=lambda p: "response_code" in p.get("header", {}),
            process=response_handler,
            priority=3,
            propagate=False  # Stop after handling response
        ))

        await client.connect()
        await client.request("device1", "subsystem1", "path1")
        await asyncio.sleep(5)
        await client.disconnect()

    asyncio.run(main())