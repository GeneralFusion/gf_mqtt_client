import asyncio
import json
import uuid
from typing import Optional, Dict, Any, Callable, Awaitable

from aiomqtt import Client
from src.payload_handler import Method, PayloadHandler
from src.topic_manager import TopicManager

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
        self._request_handler: Optional[Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None
        self._connected = asyncio.Event()
        self._topic_manager = TopicManager()
        self.subscriptions = subscriptions or []

    def set_credentials(self, username: str, password: str):
        self._username = username
        self._password = password

    def set_request_handler(self, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        self._request_handler = handler

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
                header = payload.get("header", {})

                # Handle response
                if "request_id" in header and "response_code" in header:
                    request_id = header["request_id"]
                    async with self._lock:
                        future = self._pending_requests.pop(request_id, None)
                    if future and not future.done():
                        future.set_result(payload)

                # Handle incoming request
                elif "method" in header and self._request_handler:
                    request_id = header["request_id"]
                    response_topic = self._topic_manager.build_response_topic(
                        request_topic=message.topic.value
                    )
                    response = await self._request_handler(payload)
                    await self.publish(response_topic, response)

            except json.JSONDecodeError:
                print("Invalid JSON received")
            except Exception as e:
                print(f"Error in message loop: {e}")

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
