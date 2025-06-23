import asyncio
import json
import uuid
from typing import Optional, Dict, Any, Callable, Awaitable
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from src.payload_handler import Method, PayloadHandler


class MQTTClient:
    def __init__(self, broker: str, port: int = 1883, timeout: int = 5):
        self.broker = broker
        self.port = port
        self.client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.is_connected = False
        self.loop = asyncio.get_event_loop()
        self.timeout = timeout

        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

        # For responding to incoming requests
        self._request_handler: Optional[Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None

    def username_pw_set(self, username: str, password: str):
        """Sets the MQTT client login credentials."""
        self.client.username_pw_set(username, password)

    def set_request_handler(self, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        """Sets a coroutine handler to respond to requests."""
        self._request_handler = handler

    def on_connect(self, client, userdata, connect_flags, rc, properties):
        if rc == 0:
            self.is_connected = True
            print("Connected to MQTT broker")
        else:
            print(f"Connection failed with code {rc}")

    def on_message(self, client, userdata, msg):
        print(f"Received message on topic {msg.topic}: {msg.payload.decode()}")
        coro = self._handle_message(msg.topic, msg.payload.decode())
        asyncio.run_coroutine_threadsafe(coro, self.loop)

    async def _handle_message(self, topic: str, payload_str: str):
        try:
            payload = json.loads(payload_str)
            header = payload.get("header", {})

            # If this is a response to a request
            if "request_id" in header and "response_code" in header:
                request_id = header["request_id"]
                async with self._lock:
                    future = self._pending_requests.pop(request_id, None)
                if future and not future.done():
                    future.set_result(payload)

            # If this is an incoming request
            elif "method" in header and self._request_handler:
                request_id = header["request_id"]
                response_topic = f"{topic}/{request_id}"
                response = await self._request_handler(payload)
                await self.publish(response_topic, response)

        except json.JSONDecodeError:
            print("Invalid JSON payload received")
        except Exception as e:
            print(f"Error in _handle_message: {e}")

    async def connect(self):
        await self.loop.run_in_executor(None, self.client.connect, self.broker, self.port)
        self.client.loop_start()
        while not self.is_connected:
            await asyncio.sleep(0.1)

    async def disconnect(self):
        await self.loop.run_in_executor(None, self.client.disconnect)
        self.client.loop_stop()
        self.is_connected = False

    async def publish(self, topic: str, payload: Dict[str, Any], qos: int = 0):
        if not self.is_connected:
            raise ConnectionError("Not connected to MQTT broker")
        payload_str = json.dumps(payload)
        await self.loop.run_in_executor(None, self.client.publish, topic, payload_str, qos)

    async def subscribe(self, topic: str, qos: int = 0):
        if not self.is_connected:
            raise ConnectionError("Not connected to MQTT broker")
        await self.loop.run_in_executor(None, self.client.subscribe, topic, qos)

    async def unsubscribe(self, topic: str):
        if not self.is_connected:
            raise ConnectionError("Not connected to MQTT broker")
        await self.loop.run_in_executor(None, self.client.unsubscribe, topic)

    def generate_request_id(self) -> str:
        return str(uuid.uuid4())

    async def request(self, topic: str) -> Optional[Dict[str, Any]]:
        if not self.is_connected:
            raise ConnectionError("Not connected to MQTT broker")

        payload_handler = PayloadHandler()
        request_id = self.generate_request_id()
        request_payload = payload_handler.create_request_payload(
            method=Method.GET,
            path=topic.split("/")[-1],
            request_id=request_id
        )

        response_topic = f"{topic}/{request_id}"
        future = self.loop.create_future()

        async with self._lock:
            self._pending_requests[request_id] = future

        await self.subscribe(response_topic)
        await self.publish(topic, request_payload)

        try:
            response = await asyncio.wait_for(future, timeout=self.timeout)
            await self.unsubscribe(response_topic)
            return response
        except asyncio.TimeoutError:
            print(f"Request timed out after {self.timeout} seconds")
            async with self._lock:
                self._pending_requests.pop(request_id, None)
            await self.unsubscribe(response_topic)
            return None
